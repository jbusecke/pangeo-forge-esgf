# pangeo-forge-esgf

Using queries to the ESGF API to generate urls and keyword arguments for receipe generation in pangeo-forge

## Parsing a list of instance ids using wildcards

Pangeo forge recipes require the user to provide exact instance_id's for the datasets they want to be processed. Discovering these with the [web search](https://esgf-node.llnl.gov/search/cmip6/) can become cumbersome, especially when dealing with a large number of members/models etc.

`pangeo-forge-esgf` provides some functions to query the ESGF API based on instance_id values with wildcards.

For example if you want to find all the zonal (`uo`) and meridonal (`vo`) velocities available for the `lgm` experiment of PMIP, you can do:

```python
from pangeo_forge_esgf.parsing import parse_instance_ids
parse_iids = [
    "CMIP6.PMIP.*.*.lgm.*.*.uo.*.*",
    "CMIP6.PMIP.*.*.lgm.*.*.vo.*.*",
]
iids = []
for piid in parse_iids:
    iids.extend(parse_instance_ids(piid))
iids
```

and you will get:

```
['CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gn.v20191002',
 'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.uo.gn.v20200212',
 'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200212',
 'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gr1.v20200911',
 'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200909',
 'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.vo.gn.v20200212',
 'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gn.v20191002',
 'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.vo.gn.v20200212',
 'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gr1.v20200911',
 'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.vo.gn.v20190710']
```

Eventually I hope I can leverage this functionality to handle user requests in PRs that add wildcard instance_ids, but for now this might be helpful to manually construct lists of instance_ids to submit to a pangeo-forge feedstock.

## Generating PGF recipe input (urls) from instance_ids

```python
from pangeo_forge_esgf import get_urls_from_esgf
iids = ['CMIP6.CMIP.CSIRO-ARCCSS.ACCESS-CM2.historical.r1i1p1f1.SImon.sifb.gn.v20200817']
url_dict = await get_urls_from_esgf(iids)
url_dict['CMIP6.CMIP.CSIRO-ARCCSS.ACCESS-CM2.historical.r1i1p1f1.SImon.sifb.gn.v20200817']
```

gives

```
100%|██████████| 5/5 [00:01<00:00,  4.98it/s]
Processing responses
Processing responses: Expected files per iid
Processing responses: Check for missing iids
Processing responses: Flatten results
Processing responses: Group results
Find responsive urls
100%|██████████| 1/1 [00:00<00:00,  3.25it/s]
['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CSIRO-ARCCSS/ACCESS-CM2/historical/r1i1p1f1/SImon/sifb/gn/v20200817/sifb_SImon_ACCESS-CM2_historical_r1i1p1f1_gn_185001-201412.nc']
```

or if you want to see detaile debugging statements

```python
from pangeo_forge_esgf import get_urls_from_esgf, setup_logging
setup_logging('DEBUG')
iids = ['CMIP6.CMIP.CSIRO-ARCCSS.ACCESS-CM2.historical.r1i1p1f1.SImon.sifb.gn.v20200817']
url_dict = await get_urls_from_esgf(iids)
url_dict['CMIP6.CMIP.CSIRO-ARCCSS.ACCESS-CM2.historical.r1i1p1f1.SImon.sifb.gn.v20200817']
```

