from typing import Dict, List

CMIP6_naming_schema = "mip_era.activity_id.institution_id.source_id.experiment_id.member_id.table_id.variable_id.grid_label.version"


def facets_from_iid(iid: str, fix_version: bool = True) -> Dict[str, str]:
    """Translates iid string to facet dict according to CMIP6 naming scheme.
    By default removes `v` from version
    """
    iid_name_template = CMIP6_naming_schema
    facets = {}
    for name, value in zip(iid_name_template.split("."), iid.split(".")):
        facets[name] = value
    if fix_version:
        facets["version"] = facets["version"].replace("v", "")
    facets = {
        k: v for k, v in facets.items() if v != "*"
    }  # leaving out the wildcards here will just request everything for that facet
    return facets


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
