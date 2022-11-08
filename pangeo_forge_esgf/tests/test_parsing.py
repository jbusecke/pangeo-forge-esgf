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


def test_cordex_projects():
    parse_iids = [
        "cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.*.r1i1p1.REMO2009.v1.mon.tas",
        "cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.*.r1i1p1.REMO2009.v1.mon.tas",
        "cordex-reklies.*.EUR-11.GERICS.*.historical.r1i1p1.REMO2015.v1.mon.tas",
        "cordex-adjust.*.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.*.r1i1p1.REMO2009.*.mon.tasAdjust",
        "cordex-esd.*.EUR-11.*.MPI-M-MPI-ESM-LR.*.r1i1p1.*.*.mon.tas",
    ]
    iids = []
    for piid in parse_iids:
        iids.extend(parse_instance_ids(piid))

    expected_iids = [
        "cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp26.r1i1p1.REMO2009.v1.mon.tas.v20160525",
        "cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.historical.r1i1p1.REMO2009.v1.mon.tas.v20160419",
        "cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.REMO2009.v1.mon.tas.v20160525",
        "cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp85.r1i1p1.REMO2009.v1.mon.tas.v20160525",
        "cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.rcp26.r1i1p1.REMO2009.v1.mon.tas.v20150609",
        "cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.REMO2009.v1.mon.tas.v20150609",
        "cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.rcp85.r1i1p1.REMO2009.v1.mon.tas.v20150609",
        "cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.historical.r1i1p1.REMO2009.v1.mon.tas.v20150609",
        "cordex-reklies.output.EUR-11.GERICS.MOHC-HadGEM2-ES.historical.r1i1p1.REMO2015.v1.mon.tas.v20170412",
        "cordex-reklies.output.EUR-11.GERICS.MIROC-MIROC5.historical.r1i1p1.REMO2015.v1.mon.tas.v20170329",
        "cordex-reklies.output.EUR-11.GERICS.CCCma-CanESM2.historical.r1i1p1.REMO2015.v1.mon.tas.v20170329",
        "cordex-reklies.output.EUR-11.GERICS.CNRM-CERFACS-CNRM-CM5.historical.r1i1p1.REMO2015.v1.mon.tas.v20170208",
        "cordex-adjust.bias-adjusted-output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.REMO2009.v1-SMHI-DBS45-MESAN-1989-2010.mon.tasAdjust.v20160919",
        "cordex-adjust.bias-adjusted-output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp26.r1i1p1.REMO2009.v1-SMHI-DBS45-MESAN-1989-2010.mon.tasAdjust.v20160919",
        "cordex-adjust.bias-adjusted-output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp85.r1i1p1.REMO2009.v1-SMHI-DBS45-MESAN-1989-2010.mon.tasAdjust.v20160919",
        "cordex-esd.output.EUR-11.DWD.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.EPISODES2018.v1-r1.mon.tas.v20180409",
        "cordex-esd.output.EUR-11.DWD.MPI-M-MPI-ESM-LR.historical.r1i1p1.EPISODES2018.v1-r1.mon.tas.v20180409",
        "cordex-esd.output.EUR-11.DWD.MPI-M-MPI-ESM-LR.rcp85.r1i1p1.EPISODES2018.v1-r1.mon.tas.v20180409",
        "cordex-esd.output.EUR-11.DWD.MPI-M-MPI-ESM-LR.rcp26.r1i1p1.EPISODES2018.v1-r1.mon.tas.v20180409",
    ]

    for iid in expected_iids:
        assert iid in iids
