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
cordex_template = "project.product.domain.institute.driving_model.experiment.ensemble.rcm_name.rcm_version.time_frequency.variable"

# request params
base_params = {
    "type": "Dataset",
    "format": "application/solr+json",
    "fields": "instance_id",
    "latest": "true",
    "distrib": "true",
    "limit": 500,
}

cmip6_params = base_params | {"retracted": "false"}
cordex_params = base_params.copy()

# this needs refactoring when you request the dataset_id template from ESGF servers
id_templates = {"CMIP6": cmip6_template, "CORDEX": cordex_template}

request_params = {"CMIP6": cmip6_params, "CORDEX": cordex_params}
