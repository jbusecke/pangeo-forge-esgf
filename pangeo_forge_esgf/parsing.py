import warnings
from typing import Optional
from pangeo_forge_esgf.client import ESGFClient


def parse_instance_ids(
    iid_string: str,
    search_nodes: Optional[list[str]] = None,
    search_node: Optional[str] = None,
) -> list[str]:
    """Parse an instance id with wildcards"""
    warnings.warn(
        "The parsing module will be deprecated soon. Please use the ESGFClient method `expand_instance_id_list` instead.",
        DeprecationWarning,
    )

    if search_node is not None:
        warnings.warn(
            "`search_node` is being deprecated. Please provide a list of urls to `search_nodes` instead",
            DeprecationWarning,
        )
        # make this backwards compatible
        if search_nodes is None:
            search_nodes = [search_node]

    # I am never sure where to get the full list of SOLR indicies, took this from intake-esgf: https://intake-esgf.readthedocs.io/en/latest/configure.html
    parsed_iids = []
    if search_nodes is None:
        search_nodes = [
            "https://esgf-node.llnl.gov",
            "https://esgf-data.dkrz.de",
            "https://esgf.nci.org.au",
            "https://esgf-node.ornl.gov",
            "https://esgf-node.ipsl.upmc.fr",
            "https://esg-dn1.nsc.liu.se",
            "https://esgf.ceda.ac.uk",
        ]

    for node in search_nodes:
        client = ESGFClient(base_url=node)
        parsed_iids.extend(client.expand_instance_id_list([iid_string]))

    return parsed_iids
