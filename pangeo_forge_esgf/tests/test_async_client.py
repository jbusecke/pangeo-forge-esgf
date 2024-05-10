from pangeo_forge_esgf.async_client import ESGFAsyncClient, combine_responses
import pytest
import asyncio
import requests


class TestESGFAsyncClient:
    # check that a normal request response is the same as async request
    @pytest.mark.parametrize(
        "url",
        [
            "https://esgf.ceda.ac.uk/esg-search/search",
            "https://esgf-data.dkrz.de/esg-search/search",
            "https://esgf-node.ipsl.upmc.fr/esg-search/search",
            "https://esg-dn1.nsc.liu.se/esg-search/search",
            "https://esgf-node.llnl.gov/esg-search/search",
            "https://esgf.nci.org.au/esg-search/search",
            "https://esgf-node.ornl.gov/esg-search/search",
        ],
    )
    def test_single_request_against_sync(self, url):
        limit = 10
        request_type = "Dataset"
        facets = {
            "mip_era": "CMIP6",
            "activity_id": "ScenarioMIP",
            "institution_id": "NCAR",
            "source_id": "CESM2-WACCM",
            "experiment_id": "ssp245",
            "member_id": "r1i1p1f1",
            "table_id": "SImon",
            "variable_id": "sifb",
            "grid_label": "gn",
            "version": "20190815",
        }
        params = {
            "limit": limit,
            "distrib": "false",
            "format": "application/solr+json",
            "type": request_type,
        }

        async def main():
            client = ESGFAsyncClient([url], limit=limit)
            return await client.fetch_all("Dataset", [facets])

        data_async = asyncio.run(main())
        data_sync = requests.get(url, params=params | facets).json()["response"]["docs"]
        assert data_async == data_sync


def test_combine_responses():
    responses = [
        {"id": 1, "data": 1},
        {"id": 1, "data": 1},
        {"id": 2, "data": 2},
        {"id": 2, "data": 2},
        {"id": 3, "data": 3},
        {"id": 3, "data": 3},
    ]
    combined = combine_responses(responses)
    assert len(combined) == 3
    assert combined[0] == {"id": 1, "data": 1}
    assert combined[1] == {"id": 2, "data": 2}
    assert combined[2] == {"id": 3, "data": 3}


def test_combine_response_different():
    responses = [
        {"id": 1, "data": 1},
        {"id": 1, "data": 2},
        {"id": 2, "data": 2},
        {"id": 2, "data": 2},
        {"id": 3, "data": 3},
        {"id": 3, "data": 3},
    ]
    with pytest.raises(ValueError):
        combine_responses(responses)
