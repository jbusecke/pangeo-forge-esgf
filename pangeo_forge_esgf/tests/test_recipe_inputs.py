import pytest
from pangeo_forge_esgf.recipe_inputs import sort_urls_by_time


@pytest.mark.parametrize(
    "urls_raw, expected_urls",
    [
        (
            [
                "https://a_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20750101-20941231.nc",
                "https://b_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20950101-21001231.nc",
                "https://c_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20150101-20341231.nc",
                "https://d_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20550101-20741231.nc",
                "https://e_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20350101-20541231.nc",
            ],
            [
                "https://c_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20150101-20341231.nc",
                "https://e_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20350101-20541231.nc",
                "https://d_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20550101-20741231.nc",
                "https://a_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20750101-20941231.nc",
                "https://b_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20950101-21001231.nc",
            ],
        ),
    ],
)
def test_sort_urls_by_time(urls_raw, expected_urls):
    urls_sorted = sort_urls_by_time(urls_raw)
    for i in range(len(expected_urls)):
        assert expected_urls[i] == urls_sorted[i]
