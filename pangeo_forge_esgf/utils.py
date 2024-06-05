from typing import Dict

CMIP6_naming_schema = "mip_era.activity_id.institution_id.source_id.experiment_id.member_id.table_id.variable_id.grid_label.version"


def facets_from_iid(iid: str, fix_version: bool = True) -> Dict[str, str]:
    """Translates iid string to facet dict according to CMIP6 naming scheme.
    By default removes `v` from version
    """
    iid_name_template = CMIP6_naming_schema
    template_split = iid_name_template.split(".")
    iid_split = iid.split(".")
    if len(template_split) != len(iid_split):
        raise ValueError(f"Found {len(iid_split)} facets in `iid`, but expected {len(template_split)}. Got {iid_split=}")
    facets = {}
    for name, value in zip(template_split, iids_split):
        facets[name] = value
    if fix_version:
        facets["version"] = facets["version"].replace("v", "")
    return facets
