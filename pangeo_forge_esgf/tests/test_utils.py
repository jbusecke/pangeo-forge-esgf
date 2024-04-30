import pytest
from pangeo_forge_esgf.utils import facets_from_iid, CMIP6_naming_schema


@pytest.mark.parametrize("fix_version", [True, False])
def test_facets_from_iid(fix_version):
    iid = CMIP6_naming_schema
    facets = facets_from_iid(iid, fix_version=fix_version)

    for k, v in facets.items():
        if k == "version":
            if fix_version:
                assert k == "ersion"
            else:
                assert k == v
        else:
            assert k == v
