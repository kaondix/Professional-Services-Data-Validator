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
import numpy
import pandas
import pytest
import random
from datetime import datetime, timedelta

from ibis.backends.pandas.client import PandasClient

from data_validation import consts, exceptions

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

ORACLE_CONN_CONFIG = {
    "source_type": "Oracle",
    "host": "127.0.0.1",
    "port": 1521,
}

SAMPLE_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "col_a",
            "target_column": "col_a",
            "field_alias": "count_col_a",
            "type": "count",
        },
        {
            "source_column": "col_b",
            "target_column": "col_b",
            "field_alias": "count_col_b",
            "type": "count",
        },
    ],
    consts.CONFIG_THRESHOLD: 0.0,
    consts.CONFIG_RESULT_HANDLER: None,
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

SAMPLE_THRESHOLD_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "col_a",
            "target_column": "col_a",
            "field_alias": "count_col_a",
            "type": "count",
        },
        {
            "source_column": "col_b",
            "target_column": "col_b",
            "field_alias": "count_col_b",
            "type": "count",
        },
    ],
    consts.CONFIG_THRESHOLD: 150.0,
    consts.CONFIG_RESULT_HANDLER: None,
}

SAMPLE_ROW_CONFIG = {
    # BigQuery Specific Connection Config
    "source_conn": SOURCE_CONN_CONFIG,
    "target_conn": TARGET_CONN_CONFIG,
    # Validation Type
    consts.CONFIG_TYPE: "Row",
    consts.CONFIG_MAX_RECURSIVE_QUERY_SIZE: 50,
    # Configuration Required Depending on Validator Type
    "schema_name": None,
    "table_name": "my_table",
    "target_schema_name": None,
    "target_table_name": "my_table",
    consts.CONFIG_GROUPED_COLUMNS: [
        {
            consts.CONFIG_FIELD_ALIAS: "date_value",
            consts.CONFIG_SOURCE_COLUMN: "date_value",
            consts.CONFIG_TARGET_COLUMN: "date_value",
            consts.CONFIG_CAST: "date",
        },
    ],
    consts.CONFIG_PRIMARY_KEYS: [
        {
            consts.CONFIG_FIELD_ALIAS: "id",
            consts.CONFIG_SOURCE_COLUMN: "id",
            consts.CONFIG_TARGET_COLUMN: "id",
            consts.CONFIG_CAST: None,
        },
    ],
    consts.CONFIG_CALCULATED_FIELDS: [
        {
            "source_calculated_columns": ["text_constant"],
            "target_calculated_columns": ["text_constant"],
            "field_alias": "length_text_constant",
            "type": "length",
            "depth": 0,
        },
    ],
    consts.CONFIG_AGGREGATES: [
        {
            "source_column": "text_value",
            "target_column": "text_value",
            "field_alias": "count_text_value",
            "type": "count",
        },
        {
            "source_column": "length_text_constant",
            "target_column": "length_text_constant",
            "field_alias": "sum_length",
            "type": "sum",
        },
    ],
    consts.CONFIG_RESULT_HANDLER: None,
}

JSON_DATA = """[{"col_a":0,"col_b":"a"},{"col_a":1,"col_b":"b"}]"""
JSON_COLA_ZERO_DATA = """[{"col_a":null,"col_b":"a"}]"""
JSON_BAD_DATA = """[{"col_a":0,"col_b":"a"},{"col_a":1,"col_b":"b"},{"col_a":2,"col_b":"c"},{"col_a":3,"col_b":"d"},{"col_a":4,"col_b":"e"}]"""

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
SOURCE_DF = pandas.DataFrame(SOURCE_QUERY_DATA)
JOIN_ON_FIELDS = ["date"]
NON_OBJECT_FIELDS = pandas.Index(["int_val", "double_val"])

RANDOM_STRINGS = ["a", "b", "c", "d", None]
RANDOM_STRINGS = ["a", "b", "c", "d"]


@pytest.fixture
def module_under_test():
    import data_validation.data_validation

    return data_validation.data_validation


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


def test_data_validation_client(module_under_test, fs):
    """ Test getting a Data Validation Client """
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()
    assert int(result_df.source_agg_value[0]) == 2


def test_get_pandas_schema(module_under_test):
    """ Test extracting pandas schema from dataframes for Ibis Pandas."""
    pandas_schema = module_under_test.DataValidation._get_pandas_schema(
        SOURCE_DF, SOURCE_DF, JOIN_ON_FIELDS
    )

    assert (pandas_schema.index == NON_OBJECT_FIELDS).all()


def test_zero_source_value(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]

    assert col_a_pct_diff == 100


def test_zero_target_value(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]

    assert col_a_pct_diff == -100


def test_zero_both_values(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]

    assert numpy.isnan(col_a_pct_diff)


def test_status_success_validation(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_threshold = col_a_result_df.pct_threshold.values[0]
    col_a_status = col_a_result_df.status.values[0]

    assert col_a_pct_threshold == 0.0
    assert col_a_status == "success"


def test_status_fail_validation(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_COLA_ZERO_DATA)

    client = module_under_test.DataValidation(SAMPLE_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_threshold = col_a_result_df.pct_threshold.values[0]
    col_a_status = col_a_result_df.status.values[0]

    assert col_a_pct_threshold == 0.0
    assert col_a_status == "fail"


def test_threshold_equals_diff(module_under_test, fs):
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    _create_table_file(TARGET_TABLE_FILE_PATH, JSON_BAD_DATA)

    client = module_under_test.DataValidation(SAMPLE_THRESHOLD_CONFIG)
    result_df = client.execute()

    col_a_result_df = result_df[result_df.validation_name == "count_col_a"]
    col_a_pct_diff = col_a_result_df.pct_difference.values[0]
    col_a_pct_threshold = col_a_result_df.pct_threshold.values[0]
    col_a_status = col_a_result_df.status.values[0]

    assert col_a_pct_diff == 150.0
    assert col_a_pct_threshold == 150.0
    assert col_a_status == "success"


def test_get_pandas_data_client(module_under_test, fs):
    conn_config = SAMPLE_CONFIG["source_conn"]
    _create_table_file(SOURCE_TABLE_FILE_PATH, JSON_DATA)
    ibis_client = module_under_test.DataValidation.get_data_client(conn_config)

    assert isinstance(ibis_client, PandasClient)


def test_get_oracle_data_client(module_under_test):
    with pytest.raises(
            exceptions.DataClientConnectionFailure, match=r".*pip install cx_Oracle"
    ):
        module_under_test.DataValidation.get_data_client(ORACLE_CONN_CONFIG)


def test_row_level_validation_perfect_match(module_under_test, fs):
    data = _generate_fake_data(second_range=0)
    json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG)
    result_df = client.execute()

    expected_date_result = '{"date_value": "%s"}' % str(datetime.now().date())
    assert expected_date_result == result_df["group_by_columns"].max()

    assert result_df["difference"].sum() == 0


def test_calc_field_validation_string_len_match(module_under_test, fs):
    num_rows = 100
    data = _generate_fake_data(rows=num_rows, second_range=0)
    json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG)
    result_df = client.execute()
    calc_val_df = result_df[result_df["validation_name"] == "sum_length"]

    assert calc_val_df["source_agg_value"].sum() == str(num_rows * len(STRING_CONSTANT))


def test_row_level_validation_non_matching(module_under_test, fs):
    data = _generate_fake_data(rows=10, second_range=0)
    trg_data = _generate_fake_data(initial_id=11, rows=1, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data + trg_data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG, verbose=True)
    result_df = client.execute()
    validation_df = result_df[result_df["validation_name"] == "count_text_value"]

    # TODO: this value is 0 because a COUNT() on now rows returns Null.
    # When calc fields is released, we could COALESCE(COUNT(), 0) to avoid this
    assert validation_df["difference"].sum() == 0

    expected_date_result = '{"date_value": "%s", "id": "11"}' % str(
        datetime.now().date()
    )
    grouped_column = validation_df[validation_df["difference"].isnull()][
        "group_by_columns"
    ].max()
    assert expected_date_result == grouped_column


def test_row_level_validation_smart_count(module_under_test, fs):
    data = _generate_fake_data(rows=100, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data + data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG)
    result_df = client.execute()
    validation_df = result_df[result_df["validation_name"] == "count_text_value"]

    expected_date_result = '{"date_value": "%s"}' % str(datetime.now().date())
    assert expected_date_result == validation_df["group_by_columns"].max()
    assert validation_df["difference"].sum() == 100


def test_row_level_validation_multiple_aggregations(module_under_test, fs):
    data = _generate_fake_data(rows=10, second_range=0)
    trg_data = _generate_fake_data(initial_id=11, rows=1, second_range=0)

    source_json_data = _get_fake_json_data(data)
    target_json_data = _get_fake_json_data(data + trg_data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, source_json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, target_json_data)

    client = module_under_test.DataValidation(SAMPLE_ROW_CONFIG, verbose=True)
    result_df = client.execute()
    validation_df = result_df[result_df["validation_name"] == "count_text_value"]

    # Expect 11 rows, one for each PK value
    assert len(validation_df) == 11
    assert validation_df["source_agg_value"].astype(float).sum() == 10
    assert validation_df["target_agg_value"].astype(float).sum() == 11


def test_compare_schema(module_under_test):
    source_fields = {"field1": "string", "field2": "datetime", "field3": "string"}
    target_fields = {"field1": "string", "field2": "timestamp", "field_3": "string"}

    expected_results = [['field1', 'field1', '1', '1', 'Pass', 'Source_type:string Target_type:string'],
                        ['field2', 'field2', '1', '1', 'Fail', 'Data type mismatch between source and target. '
                                                     'Source_type:datetime Target_type:timestamp'],
                        ['field3', 'N/A', '1', '0', 'Fail', "Target doesn't have a matching field name"],
                        ['N/A', 'field_3', '0', '1', 'Fail', "Source doesn't have a matching field name"]
                        ]
    assert expected_results == module_under_test.DataValidation.compare_schema(source_fields, target_fields)


def test_data_validation_schema_validation(module_under_test):
    num_rows = 1
    data = _generate_fake_data(rows=num_rows, second_range=0)
    json_data = _get_fake_json_data(data)

    _create_table_file(SOURCE_TABLE_FILE_PATH, json_data)
    _create_table_file(TARGET_TABLE_FILE_PATH, json_data)
    client = module_under_test.DataValidation(SAMPLE_SCHEMA_CONFIG, verbose=True)
    result_df = client.execute()
    del result_df["run_id"]
    del result_df["start_time"]
    del result_df["end_time"]

    expected_list = [
        ["Schema", "Schema", "my_table", "my_table", "id", "id", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "date_value", "date_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "timestamp_value", "timestamp_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "int_value", "int_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "text_constant", "text_constant", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "text_value", "text_value", "Schema", "1", "1", "Pass"],
        ["Schema", "Schema", "my_table", "my_table", "text_value_two", "text_value_two", "Schema", "1", "1", "Pass"]
    ]

    df_expected = pandas.DataFrame(expected_list)
    df_expected.columns = ['validation_name', 'validation_type', 'source_table_name', 'target_table_name',
                           'source_column_name', 'target_column_name', 'aggregation_type', 'source_agg_value',
                           'target_agg_value', 'status']

    assert len(result_df) == 7
    assert result_df["source_agg_value"].astype(float).sum() == df_expected["source_agg_value"].astype(float).sum()
    assert result_df["target_agg_value"].astype(float).sum() == df_expected["target_agg_value"].astype(float).sum()
