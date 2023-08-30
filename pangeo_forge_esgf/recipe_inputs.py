import aiohttp
import asyncio

from .utils import facets_from_iid
from typing import Dict, List, Tuple, Any, Optional
# import backoff #might still be using the backoff stuff later
from tqdm.asyncio import tqdm

## async steps
# def backoff_hdlr(details):
#     print ("Backing off {wait:0.1f} seconds after {tries} tries "
#            "calling function {target} with args {args} and kwargs "
#            "{kwargs}".format(**details))

# @backoff.on_predicate(
#     backoff.constant,
#     lambda x: x is None,
#     on_backoff=backoff_hdlr,
#     interval = 2, # in seconds
#     max_tries = 2, 
# )
async def url_responsive(
        session: aiohttp.ClientSession,
        semaphore: asyncio.BoundedSemaphore,
        url: str,  
        timeout: int
        ) -> bool:
    async with semaphore:
        try:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status <= 300: # TODO: Is this a good way to check if the search node and data_url is responsive?
                    return url
        # except asyncio.TimeoutError:
        #     print(f"Timeout for {url}")
        #     return None # should trigger a backoff just like a failed request
        # I guess one should not do this but there are a lot of other errors that can happen here.
        except Exception as e:
            # print(f"Responsivness check for {url=} failed with: {e}")
            return None

# @backoff.on_exception(
#     backoff.expo,
#     (aiohttp.ClientError, asyncio.TimeoutError),
#     max_time = 60, # in seconds
#     base=5, # prevent the backoff from being too short in the first tries
#     on_backoff=backoff_hdlr,
#     jitter=backoff.full_jitter,
# )
# NOTE: I am going to use a predicate backoff as above here. This will more broadly back off on anything that returns a None here.
# Might consider wrapping the pure request in a backoff.on_exception separately. Worried that the Timeouts here would accumulate.
# but for now this should be fine.
# @backoff.on_predicate(
#     backoff.expo,
#     lambda x: x is None,
#     on_backoff=backoff_hdlr,
#     max_time = 20, # in seconds
#     base=4,
# )
# @backoff.on_predicate(
#     backoff.constant,
#     lambda x: x is None,
#     on_backoff=backoff_hdlr,
#     interval = 2, # in seconds
#     max_tries = 3, 
# )
async def get_response_data(
    session: aiohttp.ClientSession,
    semaphore: asyncio.BoundedSemaphore,
    url: str,
    params=Dict[str, str],
    timeout: int = 5
    ) -> str:
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=timeout, raise_for_status=True) as response:
                response_data = await response.json(
                    content_type="text/json"
                )  # https://stackoverflow.com/questions/48840378/python-attempt-to-decode-json-with-unexpected-mimetype
            return response_data
        # except asyncio.TimeoutError:
        #     print(f"Timeout for {url}")
        #     return None 
        # except aiohttp.ClientError:
        #     print(f"ClientError for {url}")
        #     return None
        except Exception as e:
            # print(f"Getting response data for {url=} failed with: {e}")
            return None # should trigger a backoff 

## mid-level steps (not directly making requests)
async def filter_responsive_urls(session: aiohttp.ClientSession, semaphore: asyncio.BoundedSemaphore, node_list: List[str]) -> List[str]:
    """Filters a list of search nodes for those that are responsive."""
    tasks = []
    for url in node_list:
        tasks.append(asyncio.ensure_future(url_responsive(session, semaphore, url, timeout=20)))

    unfiltered_urls = await asyncio.gather(*tasks)
    return [url for url in unfiltered_urls if url is not None]

async def get_first_responsive_url(
        session: aiohttp.ClientSession,
        semaphore: asyncio.BoundedSemaphore,
        iid_url_tuple: Tuple[str, List[str]]) -> Tuple[str, str]:
    """Filters a list of search nodes for those that are responsive."""
    label, url_list = iid_url_tuple
    try: 
        tasks = []
        for url in url_list:
            tasks.append(asyncio.ensure_future(url_responsive(session, semaphore, url, timeout=20)))

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()    
        return (label, done.pop().result())
    except Exception as e:
        # print(f"Error for {label=}: {e}")
        return (label, None)

async def filter_responsive_file_urls(
    session: aiohttp.ClientSession,
    semaphore: asyncio.BoundedSemaphore,
    iid_url_tuple_list: List[Tuple[str, List[str]]]
    ) -> List[Tuple[str, List[str]]]:
    tasks = []
    for iid_url_tuple in iid_url_tuple_list:
        tasks.append(asyncio.ensure_future(get_first_responsive_url(session, semaphore, iid_url_tuple)))
    # results = await asyncio.gather(*tasks)
    results = await tqdm.gather(*tasks)
    filtered_results = [r for r in results if r[1] is not None]
    return filtered_results

async def get_urls_for_iid(
        session: aiohttp.ClientSession,
        semaphore: asyncio.BoundedSemaphore,
        iid: str,
        node_url: str,
        timeout: int = 20
        ) -> str:
    params = esgf_params_from_iid({}, iid)
    # print(f"{iid=} Requesting from {node_url=}")
    iid_response = await get_response_data(session, semaphore, node_url, params=params, timeout=timeout)
    # check validity of response
    if iid_response is None:
        # print(f"{iid =}: Got no response  {node_url=}")
        return None
    elif iid_response['response']['numFound'] == 0:
        # print(f"{iid =}: No files found on {node_url=}")
        return None
    else:
        # return only the payload
        return {iid: iid_response['response']["docs"]}
    

## utility processing functions (working on response output)
def get_http(urls:list[str]) -> str:
    """Filter for http urls"""
    [url] = [url.split('|')[0] for url in urls if url.endswith('HTTPServer')]
    return url

def nest_dict_from_keyed_list(keyed_list:List[Tuple[str, Any]], sep:str = '|'):
    """
    Creates nested dict from a flat list of tuples (key, value) 
    where key is a string with a separator indicating the levels of the dict.
    This is not general(only works on two levels), but works for the specific case of the ESGF search API
    """
    new_dict = {}

    for label, url in keyed_list:
        # split label
        iid, filename = label.split('|')
        if iid not in new_dict.keys():
            new_dict[iid] = {}
        if filename not in new_dict[iid].keys():
            new_dict[iid][filename] = url
    return new_dict

def sort_urls_by_time(urls:List[str]) -> List[str]:
    """Sorts a list of urls by the time stamp in the filename."""
    sorted_urls = sorted(urls, key=lambda x: x.split('/')[-1])
    return sorted_urls

def url_result_processing(
    flat_urls_per_file:List[Tuple[str, str]],
    expected_files: Dict[str, List[str]]
    ):
    filtered_dict = nest_dict_from_keyed_list(flat_urls_per_file)

    # now check which files are missing per iid
    files_found_per_iid = {}
    for iid in expected_files.keys():
        required_files = len(expected_files[iid])
        if iid in filtered_dict.keys():
            found_files = len(filtered_dict[iid].keys())
        else:
            found_files = 0
        files_found_per_iid[iid] = (found_files, required_files)

    # iid_results_combined is the ground truth about which files should be present
    # create final dict with urls if all files are found
    url_dict = {}
    for iid, counts in files_found_per_iid.items():
        if counts[0] != counts[1]:
            print(f"Skipping {iid} because not all files were found. Found {counts[0]} out of {counts[1]}")
            print(f'Found files: {list(filtered_dict[iid].keys())}')
            print(f"Expected files: {list(expected_files[iid])}")
        else:
            # sort urls by filname only
            urls = [url for filename, url in filtered_dict[iid].items()]
            url_dict[iid] = sort_urls_by_time(urls)
    return url_dict

def flatten_iid_filename(iid, r):
    return f"{iid}|{r['id'].split('|')[0]}"

def get_unique_filenames(iid_results: List[Dict[str, List[Dict[str, str]]]]) -> Dict[str, List[str]]:
    """Extract unique filenames from results"""
    filename_dict = {}
    for result in iid_results:
        for iid, res in result.items():
            filenames = []
            for r in res:
                filename = r['id'].split("|")[0]
                filenames.append(filename)
            sorted_unique_filenames = sort_urls_by_time(list(set(filenames)))

            # check for duplicate timesteps
            unique_timesteps = set([f.split('_')[-1] for f in sorted_unique_filenames])
            if len(unique_timesteps) != len(sorted_unique_filenames):
                raise ValueError(f"Duplicate files found. This sometimes happens when the API returns multiple versions.")
            
            filename_dict[iid] = sorted_unique_filenames        
    return filename_dict

def esgf_params_from_iid(params: Dict[str, str], iid: str):
    """Generates parameters for a GET request to the ESGF API based on the instance id."""
    # set default search parameters
    default_params = {
        "type": "File",
        "retracted": "false",
        "format": "application/solr+json",
        # "fields": "url,size,table_id,title,instance_id,replica,data_node",
        "fields": "id, url,title, latest, version, instance_id, replica, data_node",
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
            # TODO: Add print/logging here to clarify that we are ignoring the version and getting the latest
            del facets["version"]

    # combine params and facets
    params = params | facets
    return params


async def get_urls_from_esgf(
        iids: List[str],
        limit_per_host: int = 10,
        max_concurrency: int = 50,
        max_concurrency_response: int = 500,
        search_nodes: Optional[List[str]] = None
):
    if search_nodes is None:
        search_nodes = [
            "http://esgf-node.llnl.gov/esg-search/search",
            "http://esgf-data.dkrz.de/esg-search/search",
            "http://esgf-node.ipsl.upmc.fr/esg-search/search",
            "http://esgf-index1.ceda.ac.uk/esg-search/search",
            "http://esg-dn1.nsc.liu.se/esg-search/search",
            "http://esgf.nci.org.au/esg-search/search",
        ]
    
    semaphore = asyncio.BoundedSemaphore(max_concurrency) #https://quentin.pradet.me/blog/how-do-you-limit-memory-usage-with-asyncio.html
    semaphore_responsive = asyncio.BoundedSemaphore(max_concurrency_response)
    connector = aiohttp.TCPConnector(
        limit_per_host=limit_per_host
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        print(f"Checking responsiveness of {search_nodes=}")
        responsive_search_nodes = await filter_responsive_urls(session, semaphore_responsive, search_nodes)
        if len(responsive_search_nodes) == 0:
            raise RuntimeError(f"None of the {search_nodes=} are responsive")
        print(f"{responsive_search_nodes=}")

        # We are now basically making requests to all search nodes fore each iid. This will return 
        # results on a *file* basis. While this is rather redundant, I have seen cases where there are
        # inconsistencies between search nodes, and I just want to make super sure that we get every single
        # file/url combo that might be available. To speed this up, just trim the list of search nodes!

        print(f"Requesting urls for {iids=}")
        tasks = []
        for iid in iids:
            for search_node in responsive_search_nodes:
                tasks.append(asyncio.ensure_future(get_urls_for_iid(session, semaphore, iid, search_node, timeout=10)))
        
        # trying with a progressbar
        iid_results = await tqdm.gather(*tasks)
        
        # iid_results = await asyncio.gather(*tasks)

        print("Processing responses")

        # filter out None values
        iid_results_filtered = [result for result in iid_results if result is not None]

        print("Processing responses: Expected files per iid")
        # split out the expected number of files per iid
        expected_files_per_iid = get_unique_filenames(iid_results_filtered)
        print(f"DEBUG: {expected_files_per_iid =}")

        print("Processing responses: Check for missing iids")
        # Status message about which iids were not even found on any of the search nodes.
        remaining_iids = [iid for iid in iids if iid in [list(r.keys())[0] for r in iid_results_filtered]]
        missing_iids = list(set(iids) - set(remaining_iids))
        if len(missing_iids) > 0:
            print(f"Not able to find results for the following {len(missing_iids)} iids: {missing_iids}")
        
        # convert flat list of results to dictionary(iid: dict(filename:[unique_urls]))
        print("Processing responses: Flatten results")
        keyed_results = [(flatten_iid_filename(iid, r), get_http(r['url'])) for r_dict in iid_results_filtered for iid,r_list in r_dict.items() for r in r_list]
        

        print("Processing responses: Group results")
        # aggregate urls of results per iid and filename
        group_dict = {}
        for r in keyed_results:
            if r[0] not in group_dict:
                group_dict[r[0]] = []
            group_dict[r[0]].append(r[1])
            
        iid_results_grouped = [(k,list(set(v))) for k,v in group_dict.items()]
        
        print("Find responsive urls")
        filtered_urls_per_file = await filter_responsive_file_urls(session, semaphore_responsive, iid_results_grouped)

    final_url_dict = url_result_processing(filtered_urls_per_file, expected_files_per_iid)

    missing_iids = set(iids) - set(final_url_dict.keys())
    print(f"Was not able to construct url list for the following ({len(missing_iids)}/{len(iids)}) iids:"+"\n"+"\n".join(missing_iids))
    return final_url_dict