from typing import Dict, Union, List, Tuple
import aiohttp
import ssl
import asyncio
import time
from .dynamic_kwargs import response_data_processing
from .utils import facets_from_iid


## global variables
search_node_list = [
    "https://esgf-node.llnl.gov/esg-search/search",
    "https://esgf-data.dkrz.de/esg-search/search",
    "https://esgf-node.ipsl.upmc.fr/esg-search/search",
    "https://esgf-index1.ceda.ac.uk/esg-search/search",
]
# This is useless. If the nodes are up, they all return the same results since we are using distributed queries
# TODO: Rather check if any of these is down and determine our preferred one
# For now just use llnl
search_node = search_node_list[0]

## Data nodes in preferred order (from naomis code here: https://github.com/naomi-henderson/cmip6collect2/blob/main/myconfig.py)
data_nodes = [
    "esgf-data1.llnl.gov",
    "esgf-data2.llnl.gov",
    "aims3.llnl.gov",
    "esgdata.gfdl.noaa.gov",
    "esgf-data.ucar.edu",
    "dpesgf03.nccs.nasa.gov",
    "crd-esgf-drc.ec.gc.ca",
    "cmip.bcc.cma.cn",
    "cmip.dess.tsinghua.edu.cn",
    "cmip.fio.org.cn",
    "dist.nmlab.snu.ac.kr",
    "esg-cccr.tropmet.res.in",
    "esg-dn1.nsc.liu.se",
    "esg-dn2.nsc.liu.se",
    "esg.camscma.cn",
    "esg.lasg.ac.cn",
    "esg1.umr-cnrm.fr",
    "esgf-cnr.hpc.cineca.it",
    "esgf-data2.diasjp.net",
    "esgf-data3.ceda.ac.uk",
    "esgf-data3.diasjp.net",
    "esgf-nimscmip6.apcc21.org",
    "esgf-node2.cmcc.it",
    "esgf.bsc.es",
    "esgf.dwd.de",
    "esgf.ichec.ie",
    "esgf.nci.org.au",
    "esgf.rcec.sinica.edu.tw",
    "esgf1.dkrz.de",
    "esgf2.dkrz.de",
    "esgf3.dkrz.de",
    "noresg.nird.sigma2.no",
    "polaris.pknu.ac.kr",
    "vesg.ipsl.upmc.fr",
]


async def generate_recipe_inputs_from_iids(
    iid_list: List[str], ssl: ssl.SSLContext = None
) -> Dict[str, Union[List[str], Dict[str, str]]]:
    """_summary_

    Parameters
    ----------
    iid_list : _type_
        _description_

    Returns
    -------
    dict
        _description_
    """
    # Lets limit the amount of connections to avoid being flagged
    connector = aiohttp.TCPConnector(
        limit_per_host=10
    )  # Not sure we need a timeout now, but this might be useful in the future
    # combined with a retry.
    timeout = aiohttp.ClientTimeout(total=40)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        tasks = []
        for iid in iid_list:
            tasks.append(
                asyncio.ensure_future(iid_request(session, iid, search_node, ssl=ssl))
            )

        raw_input = await asyncio.gather(*tasks)
        recipe_inputs = {
            iid: {"urls": urls, **kwargs}
            for iid, (urls, kwargs) in zip(iid_list, raw_input)
            if urls is not None
        }

        print(
            "Failed to create recipe inputs for: \n"
            + "\n".join(sorted(list(set(iid_list) - set(recipe_inputs.keys()))))
        )
        return recipe_inputs


async def iid_request(
    session: aiohttp.ClientSession,
    iid: str,
    node: List[str],
    params: Dict = {},
    ssl: ssl.SSLContext = None,
):
    urls = None
    kwargs = None

    print(f"Requesting data for Node: {node} and {iid}...")
    response_data = await _esgf_api_request(session, node, iid, params)
    print(f"Filtering response data for {iid}...")
    filtered_response_data = await sort_and_filter_response(response_data, session)

    print(f"Determining dynamics kwargs for {iid}...")
    urls, kwargs = await response_data_processing(
        session, filtered_response_data, iid, ssl
    )

    return urls, kwargs


async def _esgf_api_request(
    session: aiohttp.ClientSession, node: str, iid: str, params: Dict[str, str]
) -> Dict[str, str]:

    # set default search parameters
    default_params = {
        "type": "File",
        "retracted": "false",
        "format": "application/solr+json",
        "fields": "url,size,table_id,title,instance_id,replica,data_node",
        "latest": "true",
        "distrib": "true",
        "limit": 500,  # This determines the number of urls/files that are returned. I dont expect this to be ever more than 500?
    }

    params = default_params | params
    facets = facets_from_iid(iid)
    # if we use latest in the params we cannot use version
    # TODO: We might want to be specific about the version here and use latest in the 'parsing' logic only. Needs discussion.
    if params["latest"] == "true":
        if "version" in facets:
            del facets["version"]
    if "CORDEX" in facets.values():
        # doesn't work otherwise
        del params["retracted"]

    # combine params and facets
    params = params | facets

    resp = await session.get(node, params=params)
    status_code = resp.status
    if not status_code == 200:
        raise RuntimeError(f"Request failed with {status_code} for {iid}")
    resp_data = await resp.json(
        content_type="text/json"
    )  # https://stackoverflow.com/questions/48840378/python-attempt-to-decode-json-with-unexpected-mimetype
    resp_data = resp_data["response"]["docs"]
    if len(resp_data) == 0:
        raise ValueError(f"No Files were found for {iid}")
    return resp_data


async def check_url(url, session):
    try:
        async with session.head(url, timeout=5) as resp:
            return resp.status
    except asyncio.exceptions.TimeoutError:
        return 503  # TODO: Is this best practice?


async def sort_and_filter_response(
    response: List[Dict[str, str]], session: aiohttp.ClientSession
) -> List[Dict[str, str]]:
    """This function takes the input of the ESGF API query with possible duplicates of filenames.
    It applies logic to choose between duplicate urls, and returns a list of dictionaries containing
    only the filtered urls and sorted by chronological order based on dates in the filenames.
    """
    # modify url to our preferred format (for now only http)
    response = [_pick_url_type(r) for r in response]

    # ok with this we get a bunch of duplicate urls.
    # What we want to do here is now:
    # - Group by filename
    # - for each group, check if non-replica is available, otherwise sort by url preference list

    # TODO: Is there a way to know if I got all the filenames that exist?
    filenames = list(set([r["title"] for r in response]))
    filename_groups = {fn: [] for fn in filenames}

    for r in response:
        filename_groups[r["title"]].append(r)

    # now filter the remaining
    filtered_filename_groups = await pick_data_node(filename_groups, session)

    # convert the keys to dates to get urls in chronological order (needed later for the recipe)
    filtered_filename_groups = {
        k.replace(".nc", "").split("_")[-1].split("-")[-1]: v
        for k, v in filtered_filename_groups.items()
    }
    return [
        v
        for _, v in sorted(
            zip(filtered_filename_groups.keys(), filtered_filename_groups.values())
        )
    ]


def _pick_url_type(response):
    # Chose preferred url format (for now only http).
    # Modifies the `url` field to contain only a string instead of a list
    modified_response = {k: v for k, v in response.items()}

    # ! we could support other protocols here, but for now this seems to work fine
    url = modified_response["url"]
    if any(
        ["HTTPServer" in u for u in url]
    ):  # seems redundant, should I try/except here instead?
        [http_url] = [u for u in url if "HTTPServer" in u]
        modified_response["url"] = http_url.split("|")[0]
        return modified_response
    else:
        raise ValueError("This recipe currently only supports HTTP links")


async def pick_data_node(
    response_groups: Dict[str, List[Dict[str, str]]], session: aiohttp.ClientSession
) -> Dict[str, Dict[str, str]]:
    """Filters out non-responsive data nodes, and then selects the preferred data node from available ones"""
    test_response_list = response_groups.get(list(response_groups.keys())[0])
    ## Determine preferred data node
    for data_node in data_nodes:
        print(f"DEBUG: Testing data node: {data_node}")
        matching_data_nodes = [
            r for r in test_response_list if r["data_node"] == data_node
        ]
        if len(matching_data_nodes) == 1:
            matching_data_node = matching_data_nodes[0]  # TODO: this is kinda clunky
            status = await check_url(matching_data_node["url"], session)
            if status in [200, 302, 308]:
                picked_data_node = data_node
                print(f"DEBUG: Picking preferred data_node: {picked_data_node}")
                break
            else:
                print(f"Got status {status} for {matching_data_node['instance_id']}")
        elif len(matching_data_nodes) == 0:
            print(f"DEBUG: Data node: {data_node} not available")
        else:
            raise  # this should never happen

    # loop through all groups and filter for the picked data node
    modified_response_groups = {}
    for k, response_list in response_groups.items():
        # This ensures that we only get one item
        [picked_data_node_response] = [
            r for r in response_list if r["data_node"] == picked_data_node
        ]
        modified_response_groups[k] = picked_data_node_response

    # Check that all keys (filenames) have a value
    assert set(response_groups.keys()) == set(modified_response_groups.keys())

    return modified_response_groups
