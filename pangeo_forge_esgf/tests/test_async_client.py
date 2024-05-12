from pangeo_forge_esgf.async_client import (
    ESGFAsyncClient,
    combine_responses,
    sanitize_id,
    get_sorted_http_urls_from_iid_dict,
)
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
    # TODO: allow_failures is not working (I have no idea how to do this,
    # and tried to copy stuff from CHAT GPT but it didn't work)
    # @pytest.mark.allow_failures(3) # allow 3 search nodes to fail
    def test_fetch_against_sync(self, url):
        # small request that wont require pagination
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
            "latest": "true",
            "limit": limit,
            "distrib": "false",
            "format": "application/solr+json",
            "type": request_type,
        }
        combined_params = params | facets

        async def main():
            async with ESGFAsyncClient([url], limit=limit) as client:
                return await client.fetch_all("Dataset", [facets])

        data_async = asyncio.run(main())
        res = requests.get(url, params=combined_params)
        res.raise_for_status()
        data_sync = res.json()["response"]["docs"]
        assert data_async == data_sync

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
    def test_paginated_fetch_against_sync(self, url):
        # Larger request with small limit, that will require pagination
        limit = 1
        request_type = "Dataset"
        facets = {
            "mip_era": "CMIP6",
            "activity_id": "ScenarioMIP",
            "table_id": "SImon",
            "variable_id": "sifb",
            "grid_label": "gn",
            "experiment_id": "ssp245",
            "member_id": "r1i1p1f1",
        }
        params = {
            "latest": "true",
            "limit": limit,
            "distrib": "false",
            "format": "application/solr+json",
            "type": request_type,
        }
        combined_params = params | facets

        def paginated_sync_request(url, **params):
            """Yields paginated responses using the ESGF REST API."""
            offset = params.get("offset", 0)
            limit = params["limit"]
            total = offset + limit + 1
            while (offset + limit) < total:
                response = requests.get(url, params=params)
                response.raise_for_status()
                response = response.json()
                yield response["response"]["docs"]
                limit = len(response["response"]["docs"])
                total = response["response"]["numFound"]
                offset = response["response"]["start"]
                params["offset"] = offset + limit

        async def main():
            async with ESGFAsyncClient([url], limit=limit) as client:
                return await client.fetch_all("Dataset", [facets])

        data_async = asyncio.run(main())
        raw_data_sync = list(paginated_sync_request(url, **combined_params))
        data_sync = []
        for data in raw_data_sync:
            data_sync.extend(data)
        assert data_async == data_sync

    def test_search_datasets(self):
        iid_list = [
            "CMIP6.ScenarioMIP.*.*.ssp245.*.SImon.sifb.gn.v20190815",
            "CMIP6.CMIP.*.*.historical.*.Omon.zmeso.gn.v20180803",
        ]

        async def main():
            async with ESGFAsyncClient() as client:
                return await client.search_datasets(iid_list)

        data = asyncio.run(main())
        assert len(data) > 0

    def test_search_files(self):
        did_list = [
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r21i1p1f1.Omon.zmeso.gn.v20180803|vesg.ipsl.upmc.fr",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r14i1p1f1.Omon.zmeso.gn.v20180803|vesg.ipsl.upmc.fr",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r3i1p1f1.Omon.zmeso.gn.v20180803|vesg.ipsl.upmc.fr",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r5i1p1f1.Omon.zmeso.gn.v20180803|aims3.llnl.gov",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r21i1p1f1.Omon.zmeso.gn.v20180803|vesg.ipsl.upmc.fr",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Omon.zmeso.gn.v20180803|esgf-node.ornl.gov",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r22i1p1f1.Omon.zmeso.gn.v20180803|vesg.ipsl.upmc.fr",
            "CMIP6.ScenarioMIP.NCAR.CESM2-WACCM.ssp245.r1i1p1f1.SImon.sifb.gn.v20190815|esgf-node.ornl.gov",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r7i1p1f1.Omon.zmeso.gn.v20180803|esgf-node.ornl.gov",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r24i1p1f1.Omon.zmeso.gn.v20180803|vesg.ipsl.upmc.fr",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r12i1p1f1.Omon.zmeso.gn.v20180803|aims3.llnl.gov",
        ]

        async def main():
            async with ESGFAsyncClient() as client:
                return await client.search_files(did_list)

        data = asyncio.run(main())
        assert len(data) > 0

    def test_expand_iids(self):
        "Check that for a list of valid iids we get the same back"
        iids = [
            "CMIP6.ScenarioMIP.NCAR.CESM2-WACCM.ssp245.r1i1p1f1.SImon.sifb.gn.v20190815",
            "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r21i1p1f1.Omon.zmeso.gn.v20180803",
        ]

        async def main():
            async with ESGFAsyncClient() as client:
                return await client.expand_iids(iids)

        data = asyncio.run(main())
        for iid in iids:
            assert iid in data


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


@pytest.mark.parametrize(
    "input",
    [
        {
            "type": "Dataset",
            "id": "some.facets.as.usual",
            "title": "and_a_filename_with_underscores.nc",
            "data_node": "just_some_node",
        },
        {
            "type": "File",
            "dataset_id": "some.facets.as.usual|just_some_node",
            "title": "and_a_filename_with_underscores.nc",
            "data_node": "just_some_node",
        },
    ],
)
def test_sanitize_id(input):
    iid = sanitize_id(input)
    assert iid == "some.facets.as.usual"


def test_combine_to_iid_dict():
    pass
    # TODO


def test_get_sorted_http_urls_from_iid_dict():
    iid_dict = {
        "dataset": {"some": "stuff"},
        "files": [
            {
                "url": ["http://some.url-this is last|something|HTTPServer"],
                "title": "b",
            },
            {"url": ["http://some.other.url|something|HTTPServer"], "title": "a"},
        ],
    }
    expected = ["http://some.other.url", "http://some.url-this is last"]
    urls = get_sorted_http_urls_from_iid_dict(iid_dict)
    assert urls == expected
