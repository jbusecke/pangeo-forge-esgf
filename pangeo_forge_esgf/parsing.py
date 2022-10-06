import requests

from .params import request_params
from .utils import facets_from_iid


def request_from_facets(url, mip, **facets):
    params = request_params[mip].copy()
    params.update(facets)
    return requests.get(url=url, params=params)


def instance_ids_from_request(json_dict):
    iids = [item["instance_id"] for item in json_dict["response"]["docs"]]
    uniqe_iids = list(set(iids))
    return uniqe_iids


def parse_instance_ids(iid: str, url: str = None, mip: str = None) -> list[str]:
    """Parse an instance id with wildcards"""
    # TODO: I should make the node url a keyword argument. For now this works well enough
    if url is None:
        url = "https://esgf-node.llnl.gov/esg-search/search"
        # url = "https://esgf-data.dkrz.de/esg-search/search"
    if mip is None:
        # take project id from first iid entry by default
        mip = iid.split(".")[0]
    facets = facets_from_iid(iid, mip)
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

    # TODO: how do I iterate over this more efficiently? Maybe we do not want to allow more than x files parsed?
    resp = request_from_facets(url, mip, **facets_filtered)
    if resp.status_code != 200:
        print(f"Request [{resp.url}] failed with {resp.status_code}")
        return resp
    else:
        json_dict = resp.json()
        return instance_ids_from_request(json_dict)
