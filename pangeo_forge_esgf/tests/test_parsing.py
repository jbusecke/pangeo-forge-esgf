from pangeo_forge_esgf.parsing import parse_instance_ids
import pytest


def test_readme_example():
    # This is possibly flaky (due to the dependence on the ESGF API)
    parse_iids = [
        "CMIP6.PMIP.*.*.lgm.*.*.[uo,vo].*.*",
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


@pytest.mark.parametrize(
    "facet_iid, expected",
    [
        ("a.b.c.d", ["a.b.c.d"]),
        ("a.[b1, b2].c.[d1, d2]", ["a.b1.c.d1", "a.b1.c.d2", "a.b2.c.d1", "a.b2.c.d2"]),
        ("a.[b1, b2].c.d", ["a.b1.c.d", "a.b2.c.d"]),
        ("a.b.c.[d1, d2]", ["a.b.c.d1", "a.b.c.d2"]),
    ],
)
def test_split_square_brackets(facet_iid, expected):
    from pangeo_forge_esgf.parsing import split_square_brackets

    assert split_square_brackets(facet_iid) == expected
