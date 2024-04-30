import requests
from typing import Optional, List

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


def split_square_brackets(facet_string: str) -> List[str]:
    ## split a string like this `a.[b1, b2].c.[d1, d2]` into a list like this: ['a.b1.c.d1', 'a.b1.c.d2', 'a.b2.c.d1', 'a.b2.c.d2']
    if "[" not in facet_string:
        return [facet_string]

    start_index = facet_string.find("[")
    end_index = facet_string.find("]")
    prefix = facet_string[:start_index]
    suffix = facet_string[end_index + 1 :]

    inner_parts = [
        part.strip() for part in facet_string[start_index + 1 : end_index].split(",")
    ]

    split_iid_combinations = []
    for part in inner_parts:
        inner_combinations = split_square_brackets(part + suffix)
        for inner_combination in inner_combinations:
            split_iid_combinations.append(prefix + inner_combination)

    return split_iid_combinations


def parse_instance_ids(
    iid_string: str, search_nodes: Optional[list[str]] = None
) -> list[str]:
    """Parse an instance id with wildcards"""
    # I am never sure where to get the full list of SOLR indicies, took this from intake-esgf: https://intake-esgf.readthedocs.io/en/latest/configure.html
    if search_nodes is None:
        search_nodes = [
            "https://esgf-node.llnl.gov/esg-search/search",
            "https://esgf-data.dkrz.de/esg-search/search",
            "https://esgf.nci.org.au/esg-search/search",
            "https://esgf-node.ornl.gov/esg-search/search",
            "https://esgf-node.ipsl.upmc.fr/esg-search/search",
            "https://esg-dn1.nsc.liu.se/esg-search/search",
            "https://esgf.ceda.ac.uk/esg-search/search",
        ]

    # first resolve the square brackets
    split_iids: List[str] = split_square_brackets(iid_string)

    parsed_iids: List[str] = []
    for iid in split_iids:
        for node in search_nodes:
            facets = facets_from_iid(iid)
            facets_filtered = {
                k: v for k, v in facets.items() if v != "*"
            }  # leaving out the wildcards here will just request everything for that facet

            resp = request_from_facets(search_node, **facets_filtered)
            if resp.status_code != 200:
                print(f"Request [{resp.url}] failed with {resp.status_code}")
            else:
                json_dict = resp.json()
                parsed_iids.extend(instance_ids_from_request(json_dict))
    return list(set(parsed_iids))
