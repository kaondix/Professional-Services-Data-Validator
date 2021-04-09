# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pandas
import pytest
import random
from datetime import datetime, timedelta

from data_validation import consts

SOURCE_TABLE_FILE_PATH = "source_table_data.json"
TARGET_TABLE_FILE_PATH = "target_table_data.json"

SOURCE_CONN_CONFIG = {
    "source_type": "Pandas",
    "table_name": "my_table",
    "file_path": SOURCE_TABLE_FILE_PATH,
    "file_type": "json",
}

TARGET_CONN_CONFIG = {
    "source_type": "Pandas",
    "table_name": "my_table",
    "file_path": TARGET_TABLE_FILE_PATH,
    "file_type": "json",
}

SAMPLE_SCHEMA_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Schema",
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_AGGREGATES: [],
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_RESULT_HANDLER: None,
}

STRING_CONSTANT = "constant"

SOURCE_QUERY_DATA = [
    {
        "date": "2020-01-01",
        "int_val": 1,
        "double_val": 2.3,
        "text_constant": STRING_CONSTANT,
        "text_val": "hello",
        "text_val_two": "goodbye",
    }
]

RANDOM_STRINGS = ["a", "b", "c", "d"]


@pytest.fixture
def module_under_test():
    import data_validation.schema_validation

    return data_validation.schema_validation


def _create_table_file(table_path, data):
    """ Create JSON File """
    with open(table_path, "w") as f:
        f.write(data)


def _generate_fake_data(
        rows=10, initial_id=0, second_range=60 * 60 * 24, int_range=100, random_strings=None
):
    """Return a list of dicts with given number of rows.

    Data Keys:
        id: a unique int per row
        timestamp_value: a random timestamp in the past {second_range} back
        date_value: a random date in the past {second_range} back
        int_value: a random int value inside 0 to {int_range}
        text_value: a random string from supplied list
    """
    data = []
    random_strings = random_strings or RANDOM_STRINGS
    for i in range(initial_id, initial_id + rows):
        rand_seconds = random.randint(0, second_range)
        rand_timestamp = datetime.now() - timedelta(seconds=rand_seconds)
        rand_date = rand_timestamp.date()

        row = {
            "id": i,
            "date_value": rand_date,
            "timestamp_value": rand_timestamp,
            "int_value": random.randint(0, int_range),
            "text_constant": STRING_CONSTANT,
            "text_value": random.choice(random_strings),
            "text_value_two": random.choice(random_strings),
        }
        data.append(row)

    return data


def _get_fake_json_data(data):
    for row in data:
        row["date_value"] = str(row["date_value"])
        row["timestamp_value"] = str(row["timestamp_value"])
        row["text_constant"] = row["text_constant"]
        row["text_value"] = row["text_value"]
        row["text_value_two"] = row["text_value_two"]

    return json.dumps(data)


def test_import(module_under_test):
    assert True


def test_schema_validation_matching(module_under_test):
    source_fields = {"field1": "string", "field2": "datetime", "field3": "string"}
    target_fields = {"field1": "string", "field2": "timestamp", "field_3": "string"}

    expected_results = [['field1', 'field1', '1', '1', 'Pass', 'Source_type:string Target_type:string'],
                        ['field2', 'field2', '1', '1', 'Fail', 'Data type mismatch between source and target. '
                                                               'Source_type:datetime Target_type:timestamp'],
                        ['field3', 'N/A', '1', '0', 'Fail', "Target doesn't have a matching field name"],
                        ['N/A', 'field_3', '0', '1', 'Fail', "Source doesn't have a matching field name"]
                        ]
    assert expected_results == module_under_test.schema_validation_matching(source_fields, target_fields)


def test_data_validation_schema_validation(module_under_test):
    num_rows = 1
    data = _generate_fake_data(rows=num_rows, second_range=0)
    json_data = _get_fake_json_data(data)
    _create_table_file(SOURCE_TABLE_FILE_PATH, json_data)
    json_data_target = json.loads(json_data)
    # removing id field
    del json_data_target[0]["id"]
    # adding new field that source doesn't have
    json_data_target[0]["id_new"] = "1"

    json_data_target = json.dumps(json_data_target)
    _create_table_file(TARGET_TABLE_FILE_PATH, json_data_target)

    client = module_under_test.SchemaValidation(SAMPLE_SCHEMA_CONFIG, verbose=True)
    result_df = client.execute_schema_validation()
    # remove fields that are non-deterministic
    del result_df["run_id"]
    del result_df["start_time"]
    del result_df["end_time"]

    expected_list = [
        ["Schema", "Schema", "my_table", "my_table", "id", "id", "Schema", "1", "0", "Fail"],
        ["Schema", "Schema", "my_table", "my_table", "date_value", "date_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "timestamp_value", "timestamp_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "int_value", "int_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "text_constant", "text_constant", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "text_value", "text_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "text_value_two", "text_value_two", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "", "id_new", "Schema", "0", "1", "Fail"]
    ]

    df_expected = pandas.DataFrame(expected_list, columns=['validation_name', 'validation_type', 'source_table_name',
                                                           'target_table_name', 'source_column_name',
                                                           'target_column_name', 'aggregation_type', 'source_agg_value',
                                                           'target_agg_value', 'status'])

    assert len(result_df) == 8

    failures = result_df[result_df["status"].str.contains("Fail")]
    success = result_df[result_df["status"].str.contains("Pass")]
    assert result_df["source_agg_value"].astype(float).sum() == df_expected["source_agg_value"].astype(float).sum()
    assert result_df["target_agg_value"].astype(float).sum() == df_expected["target_agg_value"].astype(float).sum()
    assert failures.shape[0] == 2
    assert success.shape[0] == 6
