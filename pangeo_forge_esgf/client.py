import requests
import os
from typing import Any, Union
from dataclasses import dataclass
from pangeo_forge_esgf.utils import facets_from_iid, split_square_brackets
from collections import defaultdict

import logging

logger = logging.getLogger(__name__)


# Python client for a SOLR ESGF search API
@dataclass
class ESGFClient:
    base_url: Union[str, None] = None
    distributed: bool = True
    retracted: bool = False
    format: str = "application/solr+json"
    latest: bool = True
    dataset_output_fields: Union[list[str], None] = None
    file_output_fields: Union[list[str], None] = None

    def __post_init__(self):
        if self.base_url is None:
            self.base_url = "https://esgf-node.llnl.gov"
        if "search" in self.base_url:
            raise ValueError(
                "Please provide the base URL of the ESGF search API, without `.../esg-search/search`."
            )
        self.url = os.path.join(self.base_url, "esg-search/search")
        # pad the user input with required fields
        if self.dataset_output_fields is not None:
            dataset_required_fields = ["id", "instance_id"]
            self.dataset_fields = set(
                self.dataset_output_fields + dataset_required_fields
            )
        else:
            self.dataset_fields = None  # get all fields available
        if self.file_output_fields is not None:
            file_required_fields = [
                "id",
                "instance_id",
                "data_node",
                "title",
                "url",
                "checksum",
                "checksum_type",
                "tracking_id",
            ]
            self.file_fields = set(self.file_output_fields + file_required_fields)
        else:
            self.file_fields = None  # get all fields available

    def _paginated_request(self, **params):
        """Yields paginated responses using the ESGF REST API."""
        offset = params.get("offset", 0)
        limit = params.get("limit", 25)
        total = offset + limit + 1
        while (offset + limit) < total:
            response = requests.get(self.url, params=params)
            response.raise_for_status()
            response = response.json()
            yield response
            limit = len(response["response"]["docs"])
            total = response["response"]["numFound"]
            offset = response["response"]["start"]
            params["offset"] = offset + limit

    def _search_datasets(self, **facets):
        """Dataset level search"""
        params = {
            "type": "Dataset",
            "retracted": str(self.retracted).lower(),
            "format": self.format,
            "latest": str(self.latest).lower(),
            "distrib": str(self.distributed).lower(),
        }
        if self.dataset_fields is not None:
            params["fields"] = ",".join(self.dataset_fields)
        params.update(facets)
        response_generator = self._paginated_request(**params)
        self._dataset_results = self._get_response_fields(
            response_generator, fields=self.dataset_fields
        )

    def _search_files_from_dataset_ids(self, dataset_ids: list[str]):
        """Search files from dataset ids. (dataset_id = instance_id|data_node)"""
        # raise error if datasets_id is empty
        if len(dataset_ids) == 0:
            raise ValueError("No dataset_ids provided.")

        # batch the dataset_ids with a batch_length and combine at the end
        batch_length = 200
        dataset_ids_batches: list[list[str]] = [
            dataset_ids[i : i + batch_length]
            for i in range(0, len(dataset_ids), batch_length)
        ]

        params = {
            "type": "File",
            "retracted": str(self.retracted).lower(),
            "format": self.format,
            "latest": str(self.latest).lower(),
            "distrib": str(self.distributed).lower(),
        }
        if self.file_fields is not None:
            params["fields"] = ",".join(self.file_fields)

        # get the results for each batch
        self._file_results = []
        for dataset_ids_batch in dataset_ids_batches:
            batch_params: dict[str, Union[str, list]] = {
                k: v for k, v in params.items()
            }
            batch_params["dataset_id"] = dataset_ids_batch
            response_generator = self._paginated_request(**batch_params)
            processed_response: list[dict] = self._get_response_fields(
                response_generator, fields=self.file_fields
            )
            self._file_results.extend(processed_response)

    def _get_response_fields(self, response_generator, fields=None) -> list[dict]:
        response_list = []
        for response in response_generator:
            for r in response["response"]["docs"]:
                if fields is None:
                    # extract everything
                    response_list.append(r)
                else:
                    response_list.append({f: r[f] for f in fields})
        return response_list

    def _get_unique_field_list(
        self, results: list[dict[str, Any]], field: str
    ) -> list[str]:
        """return list of unique values for a field in the search results."""
        return list(set([f[field] for f in results]))

    def expand_instance_id_list(self, instance_id_list: list[str]) -> list[str]:
        """Given a list of instance ids with wildcards and square brackets, return all instance ids that the ESGF API can resolve."""
        search_iids = []
        for iid in instance_id_list:
            if "[" in iid:
                search_iids.extend(split_square_brackets(iid))
            else:
                search_iids.append(iid)

        # now make a request for each of these instance ids
        valid_iid_list = []
        for iid in search_iids:
            facets = facets_from_iid(iid)
            self._search_datasets(**facets)
            valid_iid_list.extend(
                self._get_unique_field_list(self._dataset_results, "instance_id")
            )
        return list(set(valid_iid_list))

    def get_recipe_inputs_from_iid_list(
        self, instance_id_list: list[str]
    ) -> dict[str, dict[str, dict[str, Any]]]:
        dataset_ids = []
        # TODO refactor to take iid list as top level api
        for iid in instance_id_list:
            facets = facets_from_iid(iid)
            self._search_datasets(**facets)
            dataset_ids_single = self._get_unique_field_list(
                self._dataset_results, "id"
            )
            dataset_ids.extend(dataset_ids_single)
        logger.debug(f"Searching files for {dataset_ids=}")
        self._search_files_from_dataset_ids(dataset_ids)
        logger.debug(f"Extracting fields {self.file_fields=}")
        formatted_response = self._format_file_response_for_recipe(self._file_results)
        logger.debug(f"{formatted_response=}")
        return self._prune_formatted_response(formatted_response)

    def _format_file_response_for_recipe(
        self, response: list[dict]
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Extract file information from search results.
        Returns results as a nested dictionary
        Dataset instance_id -> data_node -> filename -> file info

        NOTE: We internally use the 'dataset instance_id' as the top level key, but there is a
        file level instance_id here too. We split that up internally to ensure consistency.
        """
        formatted_response: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        for r in response:
            instance_id, filename, data_node = sanitize_id(r)
            formatted_response[instance_id][data_node][filename] = r
        return formatted_response

    def _prune_formatted_response(
        self,
        formatted_response: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        """Prune the formatted response:
        - Find all filenames
        - Remove data nodes that do not contain all filenames
        - Pick data nodes based on preferred list

        """
        filenames: dict[str, list[str]] = {i: [] for i in formatted_response.keys()}
        for instance_id, data_node_dict in formatted_response.items():
            for data_node, file_dict in data_node_dict.items():
                filenames[instance_id].extend(file_dict.keys())
        filenames = {i: list(set(v)) for i, v in filenames.items()}

        # find all data_node names
        data_nodes: list[str] = []
        for instance_id, data_node_dict in formatted_response.items():
            data_nodes.extend(data_node_dict.keys())
        data_nodes = list(set(data_nodes))
        logger.debug(f"JUST FOR DEVELOPMENT: {data_nodes=}")

        def get_http_url(urls: list[str]) -> Union[str, None]:
            for url in urls:
                url, app, protocol = url.split("|")
                if protocol == "HTTPServer":
                    return url
            return None

        def log_missing_iids(before: dict[str, Any], after: dict[str, Any], message):
            iids_lost = set(before.keys() - set(after.keys()))
            logger.info(f"{message} {len(iids_lost)} datasets: {iids_lost}")

        # create a new nested dict that only contains a single http url
        single_http_url_response: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        for instance_id, data_node_dict in formatted_response.items():
            for data_node, file_dict in data_node_dict.items():
                for filename, fields in file_dict.items():
                    http_url = get_http_url(fields["url"])
                    if http_url is not None:
                        insert_dict = {"url": http_url}
                        if self.file_fields is not None:
                            other_fields = self.file_fields
                        else:
                            other_fields = list(fields.keys())
                        for f in other_fields:
                            if f != "url":
                                insert_dict[f] = fields[f]
                        single_http_url_response[instance_id][data_node][filename] = (
                            insert_dict
                        )
        log_missing_iids(
            formatted_response,
            single_http_url_response,
            "Could not find any http url for",
        )

        pruned_data_nodes_response: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        # remove data nodes that do not contain all filenames
        for instance_id, data_node_dict in single_http_url_response.items():
            for data_node, file_dict in data_node_dict.items():
                if all([f in file_dict.keys() for f in filenames[instance_id]]):
                    pruned_data_nodes_response[instance_id][data_node] = file_dict

        log_missing_iids(
            single_http_url_response,
            pruned_data_nodes_response,
            "Could not find a single data node with all files for",
        )

        # list of preferred data nodes
        preferred_data_nodes = [
            "aims3.llnl.gov",
            "esgf-data1.llnl.gov",
            "esgf-data.ucar.edu",
            "vesg.ipsl.upmc.fr",
            "esgf.ceda.ac.uk",
            "esgf3.dkrz.de",
        ]

        single_data_node_response: dict[str, dict[str, Any]] = defaultdict(dict)
        for instance_id, data_node_dict in pruned_data_nodes_response.items():
            for data_node in preferred_data_nodes:
                if data_node in data_node_dict.keys():
                    break
            else:
                # otherwise pick the first one
                if len(data_node_dict) > 0:
                    data_node = list(data_node_dict.keys())[0]
            single_data_node_response[instance_id] = data_node_dict[data_node]

        log_missing_iids(
            pruned_data_nodes_response,
            single_data_node_response,
            "Lost input during data node selection for",
        )

        return single_data_node_response


def sanitize_id(r: dict) -> tuple[str, str, str]:
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
    """
    filename: str = r["title"]
    data_node: str = r["data_node"]

    # split the 'id' field into instance_id and data_node and filename
    def maybe_split_id(id: str) -> str:
        if "|" in r["id"]:
            diid, dn = r["id"].split("|")
            assert data_node == dn
            return diid
        else:
            return r["id"]

    def get_final_instance_id(dataset_instance_id: str, filename: str) -> str:
        # make sure that the filename is not included
        if filename in dataset_instance_id:
            return dataset_instance_id.replace("." + filename, "")
        else:
            return dataset_instance_id

    dataset_instance_id = maybe_split_id(r["id"])
    final_instance_id = get_final_instance_id(dataset_instance_id, filename)
    return final_instance_id, filename, data_node
