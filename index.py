import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()

# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = "dp6-estudos"
bigquery_dataset = "estudos_munhoz"

CONNECTION_STRING = f"bigquery://{gcp_project}/{bigquery_dataset}"

context = ge.get_context()

# <snippet>
datasource_config = {
    "name": "my_bigquery_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "bigquery://dp6-estudos/estudos_munhoz",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "ConfiguredAssetSqlDataConnector",
            "assets": {
                "teste_merge_raw": {
                    "schema_name": "estudos_munhoz"
                }
            },
        }
    }
}
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING

# <snippet>
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>
print('a')

# <snippet>
context.add_datasource(**datasource_config)
# </snippet>
print('b')

# Test for BatchRequest naming a table.
# <snippet>
batch_request = BatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="estudos_munhoz.teste_merge_raw",  # this is the name of the table you want to retrieve
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_bigquery_datasource"]
assert "estudos_munhoz.teste_merge_raw" in set(
    context.get_available_data_asset_names()["my_bigquery_datasource"][
        "default_inferred_data_connector_name"
    ]
)