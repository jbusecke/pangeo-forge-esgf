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


## Use different ESGF project

You can also use different ESGF projects and facets to search, e.g., for a search in the ESGF CORDEX datasets, use:
```python
parse_iids = [
    "CORDEX.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.*.r1i1p1.REMO2009.v1.mon.tas",
    "CORDEX.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.*.r1i1p1.REMO2009.v1.mon.tas"
]
iids = []
```

using a different url:
```python
url = "https://esgf-data.dkrz.de/esg-search/search"

for piid in parse_iids:
    iids.extend(parse_instance_ids(piid, url=url))
iids
```

results in
```
['cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.historical.r1i1p1.REMO2009.v1.mon.tas.v20160419',
 'cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp26.r1i1p1.REMO2009.v1.mon.tas.v20160525',
 'cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp85.r1i1p1.REMO2009.v1.mon.tas.v20160525',
 'cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.REMO2009.v1.mon.tas.v20160525',
 'cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.rcp26.r1i1p1.REMO2009.v1.mon.tas.v20150609',
 'cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.rcp85.r1i1p1.REMO2009.v1.mon.tas.v20150609',
 'cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.REMO2009.v1.mon.tas.v20150609',
 'cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.historical.r1i1p1.REMO2009.v1.mon.tas.v20150609']
```
