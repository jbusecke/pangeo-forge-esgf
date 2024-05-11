import aiohttp
import asyncio
from dataclasses import dataclass
from pangeo_forge_esgf.utils import facets_from_iid, split_square_brackets
from typing import Union, Any
import warnings
from tqdm.asyncio import tqdm


@dataclass
class ESGFAsyncClient:
    urls: Union[list[str], Any] = None
    limit: int = 10
    latest: bool = True
    distributed: bool = False
    max_concurrency: int = 200
    connection_per_host: int = 10
    timeout: int = 30

    def __post_init__(self):
        if self.urls is None:
            self.urls = [
                "https://esgf-node.llnl.gov/esg-search/search",
                "https://esgf.ceda.ac.uk/esg-search/search",
                "https://esgf-node.ornl.gov/esg-search/search",
                "https://esgf-data.dkrz.de/esg-search/search",
                "https://esgf-node.ipsl.upmc.fr/esg-search/search",
                "https://esg-dn1.nsc.liu.se/esg-search/search",
                "https://esgf.nci.org.au/esg-search/search",
            ]
        self.core_params = {
            "limit": self.limit,
            "latest": str(self.latest).lower(),
            "distrib": str(self.distributed).lower(),
            "format": "application/solr+json",
        }
        self.semaphore = asyncio.BoundedSemaphore(
            self.max_concurrency
        )  # https://quentin.pradet.me/blog/how-do-you-limit-memory-usage-with-asyncio.html
        self.connector = aiohttp.TCPConnector(limit_per_host=self.connection_per_host)
        self.timeout = aiohttp.ClientTimeout(total=self.timeout)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=self.connector, raise_for_status=True
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def fetch(self, url, params, results=None):
        if results is None:
            results = []
        paginated_params = params.copy()  # Create a copy to modify per request
        offset = paginated_params.get("offset", 0)
        limit = paginated_params.get("limit", self.limit)
        total = offset + limit + 1
        async with self.semaphore:  # maybe we dont need this for now.
            try:
                async with self.session.get(
                    url, params=paginated_params, timeout=self.timeout
                ) as res:
                    if res.status == 200:
                        response = await res.json(
                            content_type="text/json"
                        )  # https://stackoverflow.com/questions/48840378/python-attempt-to-decode-json-with-unexpected-mimetype
                        limit = len(response["response"]["docs"])
                        total = response["response"]["numFound"]
                        offset = response["response"]["start"]
                        params["offset"] = offset + limit
                        results.extend(
                            response["response"]["docs"]
                        )  # Assuming the data is in 'results' key
                        if (offset + limit) < total:
                            paginated_params["offset"] = offset + limit
                            await self.fetch(url, paginated_params, results)
            except (aiohttp.ClientTimeout, asyncio.TimeoutError) as e:
                print(f"Request to {url} timed out: {e}")
        return results

    async def fetch_all(self, request_type: str, facets_list: list[dict]):
        # async with aiohttp.ClientSession(connector=self.connector) as session:
        tasks = []
        for facets in facets_list:
            for url in self.urls:
                params = self.core_params.copy() | {"type": request_type} | facets
                tasks.append(self.fetch(url, params))
        results = await tqdm.gather(*tasks)
        combined_results = [item for sublist in results for item in sublist]
        return combined_results

    async def search_datasets(self, iids: list[str]):
        facets_list = [facets_from_iid(iid) for iid in iids]
        dataset_response = await self.fetch_all("Dataset", facets_list=facets_list)
        dataset_response_merged = combine_responses(dataset_response)
        return dataset_response_merged

    async def search_files(self, dataset_ids: list[str]):
        batchsize = 10
        dataset_ids_batches = [
            dataset_ids[i : i + batchsize]
            for i in range(0, len(dataset_ids), batchsize)
        ]
        file_response = await self.fetch_all(
            "File",
            facets_list=[
                {"dataset_id": dataset_ids_batch}
                for dataset_ids_batch in dataset_ids_batches
            ],
        )
        file_response_merged = combine_responses(file_response)
        return file_response_merged

    # TODO: Put this in the README

    async def expand_iids(self, iids: list[str]):
        """Convience wrapper to make it easy to search iids for requests.
        Also splits square brackets!

        Parameters
        ----------
        iids : list[str]
            _description_

        Returns
        -------
        _type_
            _description_
        ```
        from pangeo_forge_esgf.async_client import ESGFAsyncClient
        async def main():
            async with ESGFAsyncClient() as client:
                iids = [
                "CMIP6.ScenarioMIP.*.*.ssp245.*.SImon.sifb.gn.v20190815",
                "CMIP6.CMIP.*.*.historical.*.Omon.zmeso.gn.v20180803",
                ]
                expanded_iids = await client.expand_iids(iids)
                return expanded_iids
        expanded_iids = asyncio.run(main())
        ```
        """
        iids_split = []
        for iid in iids:
            if "[" in iid:
                iids_split.extend(split_square_brackets(iid))
            else:
                iids_split.append(iid)

        dataset_response = await self.search_datasets(iids_split)
        # clean up iids
        clean_iids: list[str] = [sanitize_id(r) for r in dataset_response]
        unique_iids = list(set([iid for iid in clean_iids]))
        return unique_iids

    async def recipe_data(self, iids: list[str]):
        """Get the recipe data for a list of iids

        Parameters
        ----------
        iids : list[str]
            _description_

        Returns
        -------
        _type_
            _description_
        """
        dataset_response = await self.search_datasets(iids)
        dataset_ids = [r["id"] for r in dataset_response]
        file_response = await self.search_files(dataset_ids)
        return combine_to_iid_dict(dataset_response, file_response)


def combine_responses(responses: list[dict]):
    """Combining response dict from multiple requests into a single list of dicts.
    Identify duplicates on the key `id`, check that all other key values are the
    same and then drop duplicates"""
    # identify duplicate dict by key `id`
    unique_ids = list(set([r["id"] for r in responses]))
    unique_responses = []
    for id in unique_ids:
        # get all responses with the same id
        id_responses = [r for r in responses if r["id"] == id]
        if len(id_responses) == 1:
            unique_responses.append(id_responses[0])
        else:
            # check that all other key values are the same
            if all([r == id_responses[0] for r in id_responses]):
                unique_responses.append(id_responses[0])
            else:
                raise ValueError(
                    f"Responses with id {id} are not the same \n {responses}"
                )
    return unique_responses


def sanitize_id(r: dict) -> str:
    """Sanitize the id field from the response to extract the instance_id, filename and data_node
    # goddamn it, the 'id' field does not adhere to the schema for some datasets, so ill treat the values as the truth?
    # So in some cases the id field includes the data_node in other not...great
    # In some cases the instance_id includes the filename and in others not...this is infuriating!
    # Ok ill assume that i can trust the data_node and 'title' output and will have to do some massaging to get the dataset
    # instance_id
    # Different cases I have experienced so far for the 'id' schema
    # - <dataset_instance_id>.<filename>|<data_node>
    # - <dataset_instance_id>.<filename>
    # I wonder if that is specified somewhere
    # Maybe also a reason to use the official esgf python client?
    #

    """
    # Ughhh what is the actual unique identifier?????
    # For type "Dataset" we can maybe take the 'instance_id' or use the 'id'
    # For type "File" I just found instances where there is a weird trailing '_0' in the
    # 'instance_id', 'master_id', 'id', but not in the 'dataset_id' this is infuriating...

    # I think I need to treat each case differently....wild.
    # the dataset_id (which is the instance_id on a dataset) is the reliable link here?

    if r["type"] == "Dataset":
        identifier = r["id"]
    elif r["type"] == "File":
        identifier = r["dataset_id"]

    data_node: str = r["data_node"]

    # split the 'id' field into instance_id and data_node and filename
    def maybe_split_id(id: str) -> str:
        if "|" in id:
            diid, dn = id.split("|")
            assert data_node == dn
            return diid
        else:
            return id

    final_instance_id = maybe_split_id(identifier)
    return final_instance_id


def combine_to_iid_dict(
    ds_responses: list[dict],
    file_responses: list[dict],
):
    iid_dict: dict[str, dict] = {}
    # process the dasaset response
    dataset_dict: dict[str, dict[str, dict]] = {}
    for ds in ds_responses:
        iid = sanitize_id(ds)
        if iid not in dataset_dict:
            dataset_dict[iid] = {}
        dataset_dict[iid][ds["id"]] = ds

    file_dict: dict[str, dict[str, dict]] = {}
    for file in file_responses:
        iid = sanitize_id(file)
        if iid not in file_dict:
            file_dict[iid] = {}
        file_dict[iid][file["id"]] = file

    # compare the iids in both datasets and files
    no_file_match = set(dataset_dict.keys()) - set(file_dict.keys())
    if len(no_file_match) > 0:
        print(f"No files found for the following iids: {list(no_file_match)}")
    matched_iids = set(dataset_dict.keys()) & set(file_dict.keys())
    # This should not happen. It means we failed to have unique iids
    # linkig datasets and files
    reverse_match = set(file_dict.keys()) - set(dataset_dict.keys())
    if len(reverse_match) > 0:
        raise ValueError(f"iid mismatch found for: {list(reverse_match)}")
    # for each iid check which nodes have the max number of files
    for iid in matched_iids:
        max_num_files_dataset = max(
            [i["number_of_files"] for i in dataset_dict[iid].values()]
        )
        data_nodes_from_files = list(
            set([i["data_node"] for i in file_dict[iid].values()])
        )
        complete_data_nodes = []
        for node in data_nodes_from_files:
            files_matching = [
                i
                for i in file_dict[iid].values()
                if i["data_node"] == node and get_http_url(i) is not None
            ]
            if len(files_matching) == max_num_files_dataset:
                complete_data_nodes.append(node)
        if len(complete_data_nodes) == 0:
            warnings.warn(f"No complete data nodes found for {iid}")
        else:
            # now pick any of the complete nodes to pick for download
            # TODO: I could implement a preference list here, but for
            # now lets pick the first one
            node_pick = complete_data_nodes[0]
        iid_dict[iid] = {}
        iid_dict[iid]["dataset"] = dataset_dict[iid][f"{iid}|{node_pick}"]
        iid_dict[iid]["files"] = [
            i for i in file_dict[iid].values() if i["data_node"] == node_pick
        ]

    return iid_dict


def get_http_url(file_response: dict) -> Union[bool, None]:
    url = [u for u in file_response["url"] if "HTTPServer" in u]
    if len(url) > 0:
        return url[0].split("|")[0]
    else:
        return None


def get_sorted_http_urls_from_iid_dict(iid_dict: dict):
    urls_and_filenames = [(get_http_url(i), i["title"]) for i in iid_dict["files"]]
    sorted_urls = sorted(urls_and_filenames, key=lambda x: x[1])
    return [u[0] for u in sorted_urls]


# # process the responses
# - ds_response -> dict[iid_derived, ds_data]
# - file_response -> dict[iid_derived, file_data]
# - Cross Check filenumber
#     - (ds_data['"number_of_files": 1'], against file_data)
#     - Pick a data node
# - combine as dict[iid, ds_data + ds_data['file_response'] = file_data]
# - inject to store


#         # list of preferred data nodes
#         preferred_data_nodes = [
#             "aims3.llnl.gov",
#             "esgf-data1.llnl.gov",
#             "esgf-data.ucar.edu",
#             "vesg.ipsl.upmc.fr",
#             "esgf.ceda.ac.uk",
#             "esgf3.dkrz.de",
#         ]
