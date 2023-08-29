from pangeo_forge_esgf.recipe_inputs import url_result_processing

def test_url_result_processing_sorted_output():
    iid = 'some.iid'
    urls_raw = [
        'https://a_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20750101-20941231.nc',
        'https://b_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20950101-21001231.nc',
        'https://c_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20150101-20341231.nc',
        'https://d_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20550101-20741231.nc',
        'https://e_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20350101-20541231.nc'
    ]
    flat_urls_per_file = [(f"{iid}|{url.split('/')[-1]}", url) for url in urls_raw]
    ref_dict = {iid: {url.split('/')[-1]:url for url in urls_raw}}
    url_dict = url_result_processing(flat_urls_per_file, ref_dict)
    print(url_dict)

    expected_urls = [
        'https://c_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20150101-20341231.nc',
        'https://e_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20350101-20541231.nc',
        'https://d_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20550101-20741231.nc',
        'https://a_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20750101-20941231.nc',
        'https://b_some_url/pr_day_MPI-ESM1-2-LR_ssp245_r17i1p1f1_gn_20950101-21001231.nc',
    ]
    for i in range(len(expected_urls)):
        assert expected_urls[i] == url_dict[iid][i]
