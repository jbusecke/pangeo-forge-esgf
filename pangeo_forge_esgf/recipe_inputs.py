import asyncio
from typing import Dict, List, Union, Optional

import aiohttp
import backoff

from .utils import facets_from_iid

# Data nodes in preferred order (from naomis code here: https://github.com/naomi-henderson/cmip6collect2/blob/main/myconfig.py)
# restrictign this to us nodes for performance reasons
preferred_data_nodes = [
    "esgf-data1.llnl.gov",
    "esgf-data2.llnl.gov",
    "aims3.llnl.gov",
    "esgdata.gfdl.noaa.gov",
]

async def generate_urls_from_iids_multi_nodes(
        iid_list: List[str],
        search_node_list: List[str]=None,
        connection_limit_per_host: Optional[int] = 10,
        coroutines_limit: Optional[int] = 50,
        ) -> Dict[str, Union[List[str], Dict[str, str]]]:
    """Execute search over multiple search nodes and return the first successful result for each iid"""
    if search_node_list is None:
        search_node_list = [
            "https://esgf-node.llnl.gov/esg-search/search",
            "https://esgf-data.dkrz.de/esg-search/search",
            "https://esgf-node.ipsl.upmc.fr/esg-search/search",
            "https://esgf-index1.ceda.ac.uk/esg-search/search",
        ]
    node_url_dicts = []
    remaining_iids = iid_list
    for node in search_node_list:
        print(f"Querying search node: {node}")
        node_url_dict = await generate_urls_from_iids(
            remaining_iids,
            search_node=node,
            connection_limit_per_host=connection_limit_per_host,
            coroutines_limit=coroutines_limit,
            )
        node_url_dicts.append(node_url_dict)
        remaining_iids = set(iid_list) - set(node_url_dict.keys())
        print(
            f"{len(remaining_iids)} iids are remaining after "
              f"queriying search node: {node}"
            )
        if len(remaining_iids) == 0:
            print("Found urls for all iids. Stopping search.")
            break
    
    # combine the results from all nodes
    combined_node_url_dict = {}
    for node_url_dict in node_url_dicts:
        combined_node_url_dict = combined_node_url_dict | node_url_dict
    
    print(
        "Failed to create recipe inputs for: \n"
        + "\n".join(sorted(remaining_iids))
    )
    print(f"Of {len(iid_list)} iids resolved urls for {len(combined_node_url_dict.keys())}")
    return combined_node_url_dict
        


async def generate_urls_from_iids(
    iid_list: List[str],
    search_node: Optional[str] = "https://esgf-node.llnl.gov/esg-search/search",
    connection_limit_per_host: Optional[int] = 10,
    coroutines_limit: Optional[int] = 50,
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
        limit_per_host=connection_limit_per_host
    )  # Not sure we need a timeout now, but this might be useful in the future
    # combined with a retry.
    # timeout = aiohttp.ClientTimeout(total=40)
    # timeout=timeout, 
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        semaphore = asyncio.BoundedSemaphore(coroutines_limit) #https://quentin.pradet.me/blog/how-do-you-limit-memory-usage-with-asyncio.html
        for iid in iid_list:
            tasks.append(asyncio.ensure_future(iid_request(session, iid, search_node, semaphore)))

        raw_urls = await asyncio.gather(*tasks)
        recipe_inputs = {
            iid: urls
            for iid, urls in zip(iid_list, raw_urls)
            if urls is not None
        }

        print(
            "Failed to create recipe inputs for: \n"
            + "\n".join(sorted(list(set(iid_list) - set(recipe_inputs.keys()))))
        )
        print(f"Of {len(iid_list)} iids was able to resolve urls for {len(recipe_inputs.keys())}")
        return recipe_inputs


async def iid_request(
    session: aiohttp.ClientSession,
    iid: str,
    node: List[str],
    semaphore: asyncio.BoundedSemaphore,
    params: Dict = {},
):
    urls = None

    async with semaphore:
        print(f"{iid=}: Requesting data from search node {node}")
        try: # TODO: Maybe retry and back of here too?
            response_data = await _esgf_api_request(session, node, iid, params)
        except Exception as e:
            print(f"{iid=}: Request failed with {e}")
            return None

        print(f"{iid=}: Filtering response data")
        filtered_response_data = await sort_and_filter_response(response_data, session, iid)

        if len(filtered_response_data) == 0:
            print(f"{iid=}: Not able to find responsive data node for all files. Maybe retry?")
        else: 
            print(f"{iid=}: Getting urls")
            urls = [r["url"] for r in filtered_response_data]
            print(f"{iid=}: DEBUG: WHAT ELSE is in a response? {filtered_response_data[0]}")
            print(f"{iid=}: Found {len(urls)} urls {urls=}")

        return urls


async def _esgf_api_request(
    session: aiohttp.ClientSession, node: str, iid: str, params: Dict[str, str]
) -> Dict[str, str]:
    # set default search parameters
    default_params = {
        "type": "File",
        "retracted": "false",
        "format": "application/solr+json",
        # "fields": "url,size,table_id,title,instance_id,replica,data_node",
        "fields": "url,table_id,title,instance_id,replica,data_node",
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

    # combine params and facets
    params = params | facets

    resp = await session.get(node, params=params)
    status_code = resp.status
    if not status_code == 200:
        raise RuntimeError(f"{iid=}: Request failed with {status_code}")
    resp_data = await resp.json(
        content_type="text/json"
    )  # https://stackoverflow.com/questions/48840378/python-attempt-to-decode-json-with-unexpected-mimetype
    resp_data = resp_data["response"]["docs"]
    if len(resp_data) == 0:
        raise ValueError(f"{iid=}: No Files were found")
    return resp_data

def backoff_hdlr(details):
    print ("iid='{args[0]}': Backing off {wait:0.1f} seconds after {tries} tries "
           "calling function {target} with args {args} and kwargs "
           "{kwargs}".format(**details))

@backoff.on_exception(
        backoff.expo,
        aiohttp.ClientError,
        max_tries=5,
        max_time=60*2, # in seconds
        on_backoff=backoff_hdlr,
        jitter=backoff.full_jitter,
        )
async def check_url(iid: str, url: str, session: aiohttp.ClientSession, timeout: int) -> int:
    async with session.head(url, timeout=timeout, raise_for_status=True) as resp:
        return resp.status

async def sort_and_filter_response(
    response: List[Dict[str, str]],
    session: aiohttp.ClientSession,
    iid: str
) -> List[Dict[str, str]]:
    """This function takes the input of the ESGF API query with possible duplicates of filenames.
    It applies logic to choose between duplicate urls, and returns a list of dictionaries containing
    only the filtered urls and sorted by chronological order based on dates in the filenames.
    """
    # modify url to our preferred format (for now only http)
    http_only_response = [_pick_url_type(r) for r in response]

    # ok with this we get a bunch of duplicate urls.
    # What we want to do here is now:
    # - Group by filename
    # - for each group, check if non-replica is available, otherwise sort by url preference list

    # TODO: Is there a way to know if I got all the filenames that exist?
    filenames = list(set([r["title"] for r in http_only_response]))
    filename_groups = {fn: [] for fn in filenames}

    for r in http_only_response:
        filename_groups[r["title"]].append(r)

    # now filter the remaining
    filtered_filename_groups = await pick_data_node(filename_groups, session, iid)

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

async def choose_data_nodes_for_file(
            iid: str,
            filename: str,
            response_list: List[Dict[str, str]],
            session: aiohttp.ClientSession,
    ) -> Dict[str, str]:
    """For a given filename and list of responses 
        (representing the same file on different data urls) and 
        return the first response with a responsive url"""
    print(f"{iid=} available data nodes for {filename}: {[r['data_node'] for r in response_list]}")
    valid_responses = []
    for r in response_list:
        data_node = r['data_node']
        # TODO: If this is successful I could implement a 'preferred data node' logic here as well. 
        # First check the urls matching the preferred data node, then check the rest.
        url = r['url']
        try:
            status = await check_url(iid, url, session, timeout=60) # maybe i coudl implement a backoff with increasing timeout here?
            # TODO: It would be neat if we can do this checking concurrently and cancel once we get a response.
            # This would maybe self-select the preferred data node in a way.
            if status in [200, 206]:
                valid_responses.append(r)
                break
        except Exception as e:
            print(f"{iid=}: in `choose_data_nodes_for_file`: {data_node=} {url=} failed with {e}")
    
    if len(valid_responses) > 1:
        raise ValueError(f"{iid=}: More than one valid response ({valid_responses}) found for {filename=}. This should not happen.")

    return filename, valid_responses

async def pick_data_node(
    response_groups: Dict[str, List[Dict[str, str]]],
    session: aiohttp.ClientSession,
    iid: str,
    allow_mixed_nodes: bool = True,
) -> Dict[str, Dict[str, str]]:
    """Filters out non-responsive data nodes, and then selects the preferred data node from available ones"""
    # # Response group example: 
    # keys correspond to filenames for a single iid
    # values are a list of dicts with attributes for each location of a specific file.
    # {'psl_day_EC-Earth3_ssp585_r136i1p1f1_gr_20500101-20501231.nc':
    # [{
    #     'data_node': 'esg-dn1.nsc.liu.se',
    #     'instance_id': 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r136i1p1f1.day.psl.gr.v20200412.psl_day_EC-Earth3_ssp585_r136i1p1f1_gr_20500101-20501231.nc',
    #     'replica': False,
    #     'table_id': ['day'],
    #     'title': 'psl_day_EC-Earth3_ssp585_r136i1p1f1_gr_20500101-20501231.nc',
    #     'url': 'http://esg-dn1.nsc.liu.se/thredds/fileServer/esg_dataroot9/cmip6data/CMIP6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r136i1p1f1/day/psl/gr/v20200412/psl_day_EC-Earth3_ssp585_r136i1p1f1_gr_20500101-20501231.nc',
    #     'score': 1.0}
    #     ],
    #     ...
    # }

    # TODO: We could make this logic easier by just taking the first available data node *per* file. 
    # This might also lead to unforseen issues, so lets for now say we want all files to be 
    # available on a single data node
         

    print(f"{iid=}: Allowing mixed data nodes")
    # Try the data nodes for each filename concurrently
    tasks = []
    for filename, response_list in response_groups.items(): #TODO: This could be done concurrently for each filename to save time
        print(f"{iid=} Start evaluating valid data_nodes for {filename}")
        tasks.append(
            asyncio.ensure_future(
            choose_data_nodes_for_file(
            iid,
            filename,
            response_list,
            session
            )))
    valid_responses = await asyncio.gather(*tasks)
    
    # parse gathered output into Dict[filename, single response]
    single_response_dict = {
        filename: responses[0] for filename, responses in valid_responses if len(responses)==1
        }
    
    # Check that all files are available on the particular data_node
    all_filenames = set(response_groups.keys())
    found_filenames = set(single_response_dict.keys())
    missing_filenames = list(
        all_filenames - found_filenames
        )
    if len(missing_filenames) > 0:
        print(f"{iid=}: Could only find {len(found_filenames)}/{len(all_filenames)} filenames. {missing_filenames=}")
        return {}
    else: 
        print(f"{iid=}: Found a valid url for all files")
        # diagnose how many datanodes we used
        data_nodes_used = list(set([r['data_node'] for r in single_response_dict.values()]))
        print(f"{iid=}: Data Nodes used: {data_nodes_used}")
        return single_response_dict
