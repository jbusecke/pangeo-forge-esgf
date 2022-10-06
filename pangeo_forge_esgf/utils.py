from typing import Dict


def facets_from_iid(iid: str) -> Dict[str, str]:
    """Translates iid string to facet dict according to CMIP6 naming scheme"""
    iid_name_template = "mip_era.activity_id.institution_id.source_id.experiment_id.variant_label.table_id.variable_id.grid_label.version"
    facets = {}
    for name, value in zip(iid_name_template.split("."), iid.split(".")):
        facets[name] = value
    return facets
