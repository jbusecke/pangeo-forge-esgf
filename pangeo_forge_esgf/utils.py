import re
from typing import Dict

import requests

from .params import id_templates, known_projects


def ensure_project_str(project: str) -> str:
    """Ensure that the project string has right format

    This is mainly neccessary for CORDEX projects because the
    project facet in the dataset_id is lowercase while in the API
    search we have to use uppercase or a mixture of upper and lowercase.

    """
    for p in known_projects:
        if project.upper() == p.upper():
            return p
    return project


def facets_from_iid(iid: str, project: str = None) -> Dict[str, str]:
    """Translates iid string to facet dict according to CMIP6 naming scheme"""
    if project is None:
        # take project id from first iid entry by default
        project = ensure_project_str(iid.split(".")[0])
    iid = f"{project}." + ".".join(iid.split(".")[1:])
    iid_name_template = id_templates[project]
    # this does not work yet with CORDEX project
    # template = get_dataset_id_template(project)
    # facet_names = facets_from_template(template)
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
        "fields": "project,dataset_id_template_",
        "limit": 1,
        "format": "application/solr+json",
    }
    r = requests.get(url, params)
    return r.json()["response"]["docs"][0]["dataset_id_template_"][0]


def facets_from_template(template: str):
    """Parse the (dataset_id) string template into a list of (facet) keys"""
    regex = r"\((.*?)\)"
    return re.findall(regex, template)
