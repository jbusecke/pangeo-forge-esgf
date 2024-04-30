import pytest
from from pangeo_forge_esgf.utils import facets_from_iid, CMIP6_naming_schema

@pytest.mark.parametrize('version_fix',[True, False])
def test_facets_from_iid(version_fix):
  iid = CMIP6_naming_schema
  facets = facets_from_iid(iid, version_fix=version_fix)

  for k,v in facets.items():
    if k == 'version':
      if version_fix:
        assert k == 'ersion'
      else:
        assert k == v
    else:
      assert k == v
