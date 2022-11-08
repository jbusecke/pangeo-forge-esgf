# Test recipe creation and recipe run locally for CORDEX datasets
#
# This only works with pange_forge_recipes <= 0.9.1 due to
# https://github.com/pangeo-forge/pangeo-forge-recipes/issues/418
#

import asyncio
import ssl

import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe, setup_logging
from pyesgf.logon import LogonManager

from pangeo_forge_esgf import generate_recipe_inputs_from_iids


def create_recipes(iids, ssl=None):

    recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids, ssl=ssl))

    recipes = {}

    for iid, recipe_input in recipe_inputs.items():
        urls = recipe_input.get("urls", None)
        pattern_kwargs = recipe_input.get("pattern_kwargs", {})
        # add ssl keyword to fsspec
        pattern_kwargs["fsspec_open_kwargs"] = {"ssl": sslcontext}
        recipe_kwargs = recipe_input.get("recipe_kwargs", {})
        pattern = pattern_from_file_sequence(urls, "time", **pattern_kwargs)
        if urls is not None:
            recipes[iid] = XarrayZarrRecipe(
                pattern, xarray_concat_kwargs={"join": "exact"}, **recipe_kwargs
            )
    print("+++Failed iids+++")
    print(list(set(iids) - set(recipes.keys())))
    print("+++Successful iids+++")
    print(list(recipes.keys()))

    return recipes


def logon():
    manager = LogonManager()
    if not manager.is_logged_on():
        myproxy_host = "esgf-data.dkrz.de"
        manager.logon(hostname=myproxy_host, interactive=True, bootstrap=True)

    # create SSL context
    sslcontext = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    sslcontext.load_verify_locations(capath=manager.esgf_certs_dir)
    sslcontext.load_cert_chain(manager.esgf_credentials)
    return sslcontext


iids = [
    "cordex-reklies.output.EUR-11.GERICS.MIROC-MIROC5.historical.r1i1p1.REMO2015.v1.mon.tas",
    "cordex.output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.historical.r1i1p1.REMO2009.v1.mon.tas",
    "cordex-adjust.bias-adjusted-output.EUR-11.MPI-CSC.MPI-M-MPI-ESM-LR.rcp45.r1i1p1.REMO2009.v1-SMHI-DBS45-MESAN-1989-2010.mon.tasAdjust.v20160919",
]

sslcontext = logon()

recipes = create_recipes(iids, sslcontext)

# now do the tutorial
setup_logging()

# Prune the recipe
recipe_pruned = recipes[iids[2]].copy_pruned()

# Run the recipe
run_function = recipe_pruned.to_function()
run_function()

ds = xr.open_zarr(recipe_pruned.target_mapper, consolidated=True)
print(ds)
