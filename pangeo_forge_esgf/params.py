# example input4MIPs: input4MIPs.CMIP6.AerChemMIP.UoM.UoM-AIM-ssp370-lowNTCF-1-2-1.atmos.mon.mole_fraction_of_carbon_dioxide_in_air.gr1-GMNHSH.v20181127
known_projects = [
    "CMIP6",
    "CMIP5",
    "obs4MIPs",
    "input4MIPs",
    "CORDEX",
    "CORDEX-Reklies",
    "CORDEX-Adjust",
    "CORDEX-ESD",
]


# dataset id templates
cmip6_template = "mip_era.activity_id.institution_id.source_id.experiment_id.variant_label.table_id.variable_id.grid_label.version"
cordex_template = "project.product.domain.institute.driving_model.experiment.ensemble.rcm_name.rcm_version.time_frequency.variable.version"

# request params
base_params = {
    # "type": "File",
    "format": "application/solr+json",
    # "fields": "instance_id",
    "fields": "url,size,table_id,title,instance_id,replica,data_node,frequency,time_frequency",
    "latest": "true",
    "distrib": "true",
    "limit": 500,
}

cmip6_params = base_params | {"retracted": "false"}
cordex_params = base_params | {}

# this needs refactoring when you request the dataset_id template from ESGF servers
id_templates = {
    "CMIP6": cmip6_template,
    "CORDEX": cordex_template,
    "CORDEX-Reklies": cordex_template,
}

request_params = {
    "CMIP6": cmip6_params,
    "CORDEX": cordex_params,
    "CORDEX-Reklies": cordex_params,
}


# There is another problem with CORDEX-Reklies, e.g.
# "cordex-reklies.output.EUR-11.GERICS.MIROC-MIROC5.historical.r1i1p1.REMO2015.v1.mon.tas"
# The product="output" facet will give no result although the dataset_id clearly says it's "output".
# However the API result is empty list, so the output facet has to be removed when CORDEX-Reklies is searched, hmmm...
