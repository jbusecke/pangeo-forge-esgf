from pangeo_forge_esgf.parsing import parse_instance_ids


def test_readme_example():
    # This is possibly flaky (due to the dependence on the ESGF API)
    parse_iids = [
        "CMIP6.PMIP.*.*.lgm.*.*.uo.*.*",
        "CMIP6.PMIP.*.*.lgm.*.*.vo.*.*",
    ]
    iids = []
    for piid in parse_iids:
        iids.extend(parse_instance_ids(piid))
    iids

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
