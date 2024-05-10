import aiohttp
import asyncio
from dataclasses import dataclass
from pangeo_forge_esgf.utils import facets_from_iid
from typing import Union, Any


@dataclass
class ESGFAsyncClient:
    urls: Union[list[str], Any] = None
    limit: int = 10
    latest: bool = True
    distributed: bool = False
    max_concurrency: int = 100
    connection_per_host: int = 50
    timeout: int = 10

    def __post_init__(self):
        if self.urls is None:
            self.urls = [
                "https://esgf.ceda.ac.uk/esg-search/search",
                "https://esgf-data.dkrz.de/esg-search/search",
                "https://esgf-node.ipsl.upmc.fr/esg-search/search",
                "https://esg-dn1.nsc.liu.se/esg-search/search",
                "https://esgf-node.llnl.gov/esg-search/search",
                "https://esgf.nci.org.au/esg-search/search",
                "https://esgf-node.ornl.gov/esg-search/search",
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

    async def fetch(self, session, url, params, results=None):
        if results is None:
            results = []
        paginated_params = params.copy()  # Create a copy to modify per request
        offset = paginated_params.get("offset", 0)
        limit = paginated_params.get("limit", self.limit)
        total = offset + limit + 1
        async with self.semaphore:
            async with session.get(
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
                        await self.fetch(session, url, paginated_params, results)
        return results

    async def gather_and_combine(self, tasks):
        results = await asyncio.gather(*tasks)
        combined_results = [item for sublist in results for item in sublist]
        return combined_results

    async def fetch_all(self, request_type: str, facets_list: list[dict]):
        async with aiohttp.ClientSession(connector=self.connector) as session:
            tasks = []
            for facets in facets_list:
                for url in self.urls:
                    params = self.core_params.copy() | {"type": request_type} | facets
                    tasks.append(self.fetch(session, url, params))
            return await self.gather_and_combine(tasks)
            # results = await asyncio.gather(*tasks)
            # combined_results = [item for sublist in results for item in sublist]
            # return combined_results

    async def search_iids(self, iids: list[str]):
        facets_list = [facets_from_iid(iid) for iid in iids]
        dataset_response = await self.fetch_all("Dataset", facets_list=facets_list)
        dataset_response_merged = combine_responses(dataset_response)
        return dataset_response_merged

    async def search_files(self, dataset_ids: list[str]):
        file_results = await self.fetch_all(
            "File", facets_list=[{"dataset_id": dataset_ids}]
        )
        # split dataset_ids into batches and run a single request for each batch
        # batchsize = 100
        # file_results = []
        # dataset_ids_batches = [dataset_ids[i:i + batchsize] for i in range(0, len(dataset_ids), batchsize)]
        # for dataset_ids_batch in dataset_ids_batches:
        #     batch_file_results = await self.fetch_all("File", facets_list=[{'dataset_id':id} for id in dataset_ids_batch])
        #     file_results.append(batch_file_results)
        return file_results


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
                raise ValueError(f"Responses with id {id} are not the same")
    return unique_responses
