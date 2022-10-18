import asyncio
import ssl

from pyesgf.logon import LogonManager

from pangeo_forge_esgf import generate_recipe_inputs_from_iids

# logon
manager = LogonManager()
if not manager.is_logged_on():
    myproxy_host = "esgf-data.dkrz.de"
    manager.logon(hostname=myproxy_host, interactive=True, bootstrap=True)

# create SSL context
sslcontext = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
sslcontext.load_verify_locations(capath=manager.esgf_certs_dir)
sslcontext.load_cert_chain(manager.esgf_credentials)

iids = [
    "cordex.output.EUR-11.GERICS.ECMWF-ERAINT.evaluation.r1i1p1.REMO2015.v1.mon.tas.v20180813",
    "cordex.output.EUR-44.MPI-CSC.MPI-M-MPI-ESM-LR.historical.r1i1p1.REMO2009.v1.mon.tas.v20150609",
    "cordex-reklies.output.EUR-11.GERICS.MIROC-MIROC5.historical.r1i1p1.REMO2015.v1.mon.tas",
]

recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids, ssl=sslcontext))
print("DONE")
print(recipe_inputs)
