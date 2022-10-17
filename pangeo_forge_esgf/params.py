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
