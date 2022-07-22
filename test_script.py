import asyncio
from pangeo_forge_esgf import generate_recipe_inputs_from_iids

iids = [
    #PMIP runs requested by @CommonClimate
    'CMIP6.PMIP.MIROC.MIROC-ES2L.past1000.r1i1p1f2.Amon.tas.gn.v20200318',
    'CMIP6.PMIP.MRI.MRI-ESM2-0.past1000.r1i1p1f1.Amon.tas.gn.v20200120',
    'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.past2k.r1i1p1f1.Amon.tas.gn.v20210714',
]

recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids))
print('DONE')
print(recipe_inputs)