import pytest
from pangeo_forge_esgf.recipe_inputs import sort_urls_by_time, get_unique_filenames, filter_urls_first, filter_urls_preferred_node


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

def test_get_unique_filenames():
    filenames_w_data_node = [
     'iid.stuff_20700101-20791231.nc|data.node.a',
     'iid.stuff_20800101-20891231.nc|data.node.a',
     'iid.stuff_20800101-20891231.nc|data.node.b',
    ]
    expected_filenames = [
        'iid.stuff_20700101-20791231.nc',
        'iid.stuff_20800101-20891231.nc',
    ]
    iid = 'some.iid'
    iid_results = [{iid:[{'id':f"{filename}|data.node.stuff"} for filename in filenames_w_data_node]}]
    filename_dict = get_unique_filenames(iid_results)
    filenames = filename_dict[iid]
    for i in range(len(expected_filenames)):
        assert filenames[i] == expected_filenames[i]
        
def test_get_unique_filenames_raise_on_duplicates():
    filenames_w_data_node = [
     'iid.stuff_20700101-20791231.nc|data.node.a',
     'iid.stuff_20800101-20891231.nc|data.node.a',
     'iid.staff_20800101-20891231.nc|data.node.a',
    ]
    iid = 'some.iid'
    iid_results = [{iid:[{'id':f"{filename}|data.node.stuff"} for filename in filenames_w_data_node]}]
    with pytest.raises(ValueError):
        get_unique_filenames(iid_results)

def test_filter_first_file_urls():
    unfiltered = [
        ('some.iid.you.like|some.filename.pattern', ['url1', 'url2']),
        ('some.iid.you.like|some.other_filename.pattern', ['urla', 'urlb']),
        ('some.other_iid.you.like|some.filename.pattern', ['urlc']),
        ('some.other_iid.you.like|some.other_filename.pattern', ['urlb', 'urla']),
        ]
    expected = [
        ('some.iid.you.like|some.filename.pattern', 'url1'),
        ('some.iid.you.like|some.other_filename.pattern', 'urla'),
        ('some.other_iid.you.like|some.filename.pattern', 'urlc'),
         ('some.other_iid.you.like|some.other_filename.pattern', 'urlb'),
        ]
    filtered = filter_urls_first(unfiltered)
    for i in range(len(expected)):
        for ii in range(2):
            assert filtered[i][ii] == expected[i][ii]

# def test_filter_urls_preferred_node():
#     unfiltered = [
#         ('some.iid.you.like|some.filename.pattern', ['urlb', 'url2']),
#         ('some.iid.you.like|some.other_filename.pattern', ['url2', 'urla', 'urlb']),
#         ('some.other_iid.you.like|some.filename.pattern', ['urlc']),
#         ('some.other_iid.you.like|some.other_filename.pattern', ['urlb', 'urla']),
#         ]
#     expected = [
#         ('some.iid.you.like|some.filename.pattern', 'url2'),
#         ('some.iid.you.like|some.other_filename.pattern', 'urla'),
#         ('some.other_iid.you.like|some.filename.pattern', 'urlc'),
#          ('some.other_iid.you.like|some.other_filename.pattern', 'urla'),
#         ]
#     filtered = filter_urls_preferred_node(unfiltered, preferred_file=['urla', 'url2'])
#     pass

