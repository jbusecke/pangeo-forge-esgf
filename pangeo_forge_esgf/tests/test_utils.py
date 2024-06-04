import pytest
import requests
from pangeo_forge_esgf.utils import facets_from_iid, CMIP6_naming_schema


def get_official_drs_naming_scheme():
    # URL of the raw file on GitHub
    url = "https://raw.githubusercontent.com/WCRP-CMIP/CMIP6_CVs/2c9cf667546f31a495cb2e3b8d9d5892bc7abaa2/CMIP6_DRS.json"

    # Send a GET request to the URL
    response = requests.get(url)
    if response.status_code == 200:
        # Split the response text into lines
        lines = response.text.splitlines()
        # Access the specific line, line numbers start from 0 so line 5 is index 4
        line_of_interest = lines[4]
        line_of_interest = (
            line_of_interest.split(":")[-1]
            .strip()
            .replace('"', "")
            .replace(",", "")
            .replace("<", "")
            .replace(">", "")
            .replace("/", ".")
        )
        return line_of_interest
    else:
        raise ("Failed to fetch file")


def test_naming_scheme_against_upstream():
    upstream_naming_scheme = get_official_drs_naming_scheme()
    assert CMIP6_naming_schema == upstream_naming_scheme


@pytest.mark.parametrize("fix_version", [True, False])
def test_facets_from_iid(fix_version):
    iid = CMIP6_naming_schema
    facets = facets_from_iid(iid, fix_version=fix_version)

    for k, v in facets.items():
        if k == "version":
            if fix_version:
                assert v == "ersion"
            else:
                assert k == "version"
        else:
            assert k == v

def test_facets_from_iid_wrong_length():
    iid = 'Just.three.facets'
    with pytest.raises(ValueError):
        facets_from_iid(iid)
