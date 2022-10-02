from typing import Dict
from .params import id_templates
import requests
import re


def facets_from_iid(iid: str, mip: str = None) -> Dict[str, str]:
    """Translates iid string to facet dict according to CMIP6 naming scheme"""
    if mip is None:
        # take project id from first iid entry by default
        mip = iid.split(".")[0]
    iid_name_template = id_templates[mip]
    facets = {}
    for name, value in zip(iid_name_template.split("."), iid.split(".")):
        facets[name] = value
    return facets


def get_dataset_id_template(project: str, url: str = None):
    """Requests the dataset_id string template for an ESGF project"""
    if url is None:
        url = "https://esgf-node.llnl.gov/esg-search/search"
    params = {
        "project": project,
        "fields": "*",
        "limit": 1,
        "format": "application/solr+json"
    }
    r = requests.get(url, params)
    #print(r.status_code)
    return r.json()["response"]["docs"][0]["dataset_id_template_"][0]


def facets_from_template(template: str):
    """Parse the (dataset_id) string template into a list of (facet) keys"""
    regex = r"\((.*?)\)"
    return re.findall(regex, template)
