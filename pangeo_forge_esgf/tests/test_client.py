from pangeo_forge_esgf.client import ESGFClient
import pytest


class TestESGFClient:
    def test_init(self):
        client = ESGFClient()
        assert client.base_url == "https://esgf-node.llnl.gov/"
        assert client.url == "https://esgf-node.llnl.gov/esg-search/search"
        assert client.distributed is True
        assert client.retracted is False
        assert client.format == "application/solr+json"
        assert client.latest is True

    def test_init_wrong_url(self):
        with pytest.raises(ValueError):
            ESGFClient(base_url="https://esgf-node.llnl.gov/esg-search/search")

    def test_init_different_url(self):
        client = ESGFClient(base_url="https://some-thing.gov")
        assert client.url == "https://some-thing.gov/esg-search/search"

    def test_url(self):
        client = ESGFClient()
        assert client.url == "https://esgf-node.llnl.gov/esg-search/search"


def test_end_to_end():
    client = ESGFClient()
    raw_iids = [
        "CMIP6.ScenarioMIP.NCAR.CESM2-WACCM.ssp245.r1i1p1f1.SImon.sifb.gn.v20190815",
        "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r8i1p1f1.Omon.zmeso.gn.v20180803",
        "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r24i1p1f1.SImon.sifb.gn.v20180803",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp585.r47i1p1f1.SImon.sifb.gn.v20190815",
        "CMIP6.CMIP.MPI-M.MPI-ESM1-2-HR.historical.r1i1p1f1.3hr.pr.gn.v20190710",
        "CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp245.r3i1p1f1.Omon.zmeso.gn.v20191121",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp245.r43i1p1f1.SImon.sifb.gn.v20190815",
        "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r29i1p1f1.SImon.siitdthick.gn.v20180803",
        # these did pass
        "CMIP6.CMIP.THU.CIESM.historical.r3i1p1f1.Omon.tos.gn.v20200220",
        "CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp245.r15i1p1f2.day.pr.gr.v20201015",
        "CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp245.r111i1p1f1.day.psl.gr.v20210401",
        "CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r31i1p1f1.day.pr.gn.v20200623",
        "CMIP6.CMIP.MIROC.MIROC6.historical.r37i1p1f1.day.pr.gn.v20200519",
    ]
    parsed_iids = client.expand_instance_id_list(raw_iids)
    output = client.get_recipe_inputs_from_iid_list(parsed_iids)

    # this might fail in the future (due to flakyness but its a nice test of https://github.com/jbusecke/pangeo-forge-esgf/issues/42)
    assert set(output.keys()) == set(raw_iids)


def test_readme_example():
    # This is possibly flaky (due to the dependence on the ESGF API)
    parse_iids = [
        "CMIP6.PMIP.*.*.lgm.*.*.[uo,vo].*.*",
    ]
    client = ESGFClient("https://esgf-node.llnl.gov/")
    iids = client.expand_instance_id_list(parse_iids)

    # I expect at least these iids to be parsed
    # (there might be new ones that are added at a later point)
    expected_iids = [
        "CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gn.v20191002",
        "CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.uo.gn.v20200212",
        "CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200212",
        "CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gr1.v20200911",
        "CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200909",
        "CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.vo.gn.v20200212",
        "CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gn.v20191002",
        "CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.vo.gn.v20200212",
        "CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gr1.v20200911",
        "CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.vo.gn.v20190710",
    ]

    for iid in expected_iids:
        assert iid in iids
