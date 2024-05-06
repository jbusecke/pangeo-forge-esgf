import requests
import os
from typing import Any, Union
from dataclasses import dataclass
from pangeo_forge_esgf.utils import facets_from_iid, split_square_brackets
from collections import defaultdict


# Python client for a SOLR ESGF search API
@dataclass
class ESGFClient:
    base_url: str = "https://esgf-node.llnl.gov/"
    distributed: bool = True
    retracted: bool = False
    format: str = "application/solr+json"
    latest: bool = True

    def __post_init__(self):
        if "search" in self.base_url:
            raise ValueError(
                "Please provide the base URL of the ESGF search API, without `.../esg-search/search`."
            )
        self.url = os.path.join(self.base_url, "esg-search/search")

    def _paginated_request(self, **params):
        """Yields paginated responses using the ESGF REST API."""
        offset = params.get("offset", 0)
        limit = params.get("limit", 50)
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
        params.update(facets)
        self._dataset_results = self._paginated_request(**params)

    def _search_files_from_dataset_ids(self, dataset_ids: list[str]):
        """Search files from dataset ids. (dataset_id = instance_id|data_node)"""
        # raise error if datasets_id is empty
        if len(dataset_ids) == 0:
            raise ValueError("No dataset_ids provided.")
        params = {
            "type": "File",
            "retracted": str(self.retracted).lower(),
            "format": self.format,
            "latest": str(self.latest).lower(),
            "distrib": str(self.distributed).lower(),
            "dataset_id": dataset_ids,
        }
        self._file_results = self._paginated_request(**params)

    def _get_response_fields(self, fields: list[str], type: str) -> list[dict]:
        """Extract fields from the search results. This also materializes the generator."""
        response_list = []
        if type == "dataset":
            response_data = self._dataset_results
        elif type == "file":
            response_data = self._file_results
        else:
            raise ValueError("Invalid type. Must be 'dataset' or 'file'.")

        # Note that it seems to be necessary to include the instance_id field in the search results to get any response data
        if "instance_id" not in fields:
            fields.append("instance_id")
        for response in response_data:
            for r in response["response"]["docs"]:
                response_list.append({field: r[field] for field in fields})
        return response_list

    def _get_unique_field_list(self, field: str, type: str) -> list[str]:
        """return list of unique values for a field in the search results."""
        return list(
            set([f[field] for f in self._get_response_fields([field], type=type)])
        )

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
                self._get_unique_field_list("instance_id", type="dataset")
            )
        return valid_iid_list

    def get_recipe_inputs_from_iid_list(
        self, instance_id_list: list[str]
    ) -> dict[str, dict[str, dict[str, Any]]]:
        fields = [
            "id",
            "instance_id",
            "data_node",
            "title",
            "checksum",
            "checksum_type",
            "url",
        ]
        dataset_ids = []
        # TODO refactor to take iid list as top level api
        for iid in instance_id_list:
            facets = facets_from_iid(iid)
            self._search_datasets(**facets)
            dataset_ids_single = self._get_unique_field_list("id", type="dataset")
            dataset_ids.extend(dataset_ids_single)
        print(f"Searching files for {dataset_ids=}")
        self._search_files_from_dataset_ids(dataset_ids)
        print(f"Extracting fields {fields=}")
        response = self._get_response_fields(fields, type="file")
        formatted_response = self._format_file_response_for_recipe(response)
        print(f"{formatted_response=}")
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
            formatted_response[instance_id][data_node][filename] = {
                k: v
                for k, v in r.items()
                if k not in ["id", "instance_id", "data_node", "title"]
            }
        return formatted_response

    def _prune_formatted_response(
        self, formatted_response: dict[str, dict[str, dict[str, Any]]]
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

        # find all data_node names and print
        data_nodes: list[str] = []
        for instance_id, data_node_dict in formatted_response.items():
            data_nodes.extend(data_node_dict.keys())
        data_nodes = list(set(data_nodes))

        def get_http_url(urls: list[str]) -> Union[str, None]:
            for url in urls:
                url, app, protocol = url.split("|")
                if protocol == "HTTPServer":
                    return url
            return None

        # create a new nested dict that only contains a single http url
        single_http_url_response: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        for instance_id, data_node_dict in formatted_response.items():
            for data_node, file_dict in data_node_dict.items():
                for f, fields in file_dict.items():
                    http_url = get_http_url(fields["url"])
                    if http_url is not None:
                        insert_dict = {"url": http_url}
                        # TODO: We might want to retain other fields here in addition to the checksum?
                        for field in ["checksum", "checksum_type"]:
                            insert_dict[field] = fields[field]
                        single_http_url_response[instance_id][data_node][f] = (
                            insert_dict
                        )

        pruned_data_nodes_response: dict[str, dict[str, dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        # remove data nodes that do not contain all filenames
        for instance_id, data_node_dict in single_http_url_response.items():
            for data_node, file_dict in data_node_dict.items():
                if all([f in file_dict.keys() for f in filenames[instance_id]]):
                    pruned_data_nodes_response[instance_id][data_node] = file_dict

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
