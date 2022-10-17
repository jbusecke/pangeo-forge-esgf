# %%
import asyncio

from pangeo_forge_esgf import generate_recipe_inputs_from_iids

iids = [
    # PMIP runs requested by @CommonClimate
    "CMIP6.PMIP.MIROC.MIROC-ES2L.past1000.r1i1p1f2.Amon.tas.gn.v20200318",
    "CMIP6.PMIP.MRI.MRI-ESM2-0.past1000.r1i1p1f1.Amon.tas.gn.v20200120",
    "CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.past2k.r1i1p1f1.Amon.tas.gn.v20210714",
    "CMIP6.CMIP.NCC.NorESM2-LM.historical.r1i1p1f1.Omon.vmo.gr.v20190815",
    "CMIP6.PMIP.MIROC.MIROC-ES2L.past1000.r1i1p1f2.Amon.tas.gn.v20200318",
    "CMIP6.PMIP.MRI.MRI-ESM2-0.past1000.r1i1p1f1.Amon.tas.gn.v20200120",
    "CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.past2k.r1i1p1f1.Amon.tas.gn.v20210714",
    "CMIP6.CMIP.FIO-QLNM.FIO-ESM-2-0.piControl.r1i1p1f1.Omon.vsf.gn",  # this one should not be available. This changes daily. Check the data nodes which are down to find examples.
]

recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids))
print("DONE")
print(recipe_inputs)

# %%
# How can I check if a file is available?
# url = 'http://esg1.umr-cnrm.fr/thredds/fileServer/CMIP6_CNRM/PMIP/CNRM-CERFACS/CNRM-CM6-1/lig127k/r1i1p1f2/Amon/tauv/gr/v20200212/tauv_Amon_CNRM-CM6-1_lig127k_r1i1p1f2_gr_185001-209912.nc'
# url = 'http://esgf-nimscmip6.apcc21.org/thredds/fileServer/my_cmip6_dataroot/Historical/R2/aa008p-SImon/CMIP6/CMIP/NIMS-KMA/KACE-1-0-G/historical/r2i1p1f1/SImon/siflsensupbot/gr/v20200130/siflsensupbot_SImon_KACE-1-0-G_historical_r2i1p1f1_gr_185001-201412.nc'
# url = 'http://esgf.ichec.ie'
url = "http://cmip.dess.tsinghua.edu.cn"
# import requests
# from requests import ReadTimeout
# try:
#     test = requests.head(url, timeout=5)
#     print(test.status_code)
# except ReadTimeout:
#     print('Caught timeout. Assuming this file is not available right now')

# async version
# import asyncio
# import aiohttp
# # async def main():
# #     async with aiohttp.ClientSession() as session:
# #         async with session.head(url, timeout=2) as resp:
# #             print(resp.status)


# async def _check_url(url, session):
#     async with session.head(url) as resp:
#         return resp.status

# session = aiohttp.ClientSession()
# status2 = await _check_url(url, session)
# print(status2)

# 302 is ok?
# %%
