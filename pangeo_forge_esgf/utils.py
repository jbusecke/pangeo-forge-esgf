from typing import Dict
from .params import id_templates


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
