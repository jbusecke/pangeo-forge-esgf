import requests

from .utils import facets_from_iid


def request_from_facets(url, **facets):
    params = {
        "type": "Dataset",
        "retracted": "false",
        "format": "application/solr+json",
        "fields": "instance_id",
        "latest": "true",
        "distrib": "true",
        "limit": 500,
    }
    params.update(facets)
    return requests.get(url=url, params=params)


def instance_ids_from_request(json_dict):
    iids = [item["instance_id"] for item in json_dict["response"]["docs"]]
    uniqe_iids = list(set(iids))
    return uniqe_iids


def parse_instance_ids(iid: str) -> list[str]:
    """Parse an instance id with wildcards"""
    facets = facets_from_iid(iid)
    # convert string to list if square brackets are found
    for k, v in facets.items():
        if "[" in v:
            v = (
                v.replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(" ", "")
                .split(",")
            )
        facets[k] = v
    facets_filtered = {k: v for k, v in facets.items() if v != "*"}

    # TODO: I should make the node url a keyword argument.
    # For now this works well enough
    url = "https://esgf-node.llnl.gov/esg-search/search"
    # url = "https://esgf-data.dkrz.de/esg-search/search"
    # TODO: how do I iterate over this more efficiently?
    # Maybe we do not want to allow more than x files parsed?
    resp = request_from_facets(url, **facets_filtered)
    if resp.status_code != 200:
        print(f"Request [{resp.url}] failed with {resp.status_code}")
        return resp
    else:
        json_dict = resp.json()
        return instance_ids_from_request(json_dict)
