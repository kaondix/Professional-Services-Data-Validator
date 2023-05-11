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

import os
from unittest import mock

import pytest

from data_validation import __main__ as main
from data_validation import cli_tools, data_validation, consts
from tests.system.data_sources.deploy_cloudsql.cloudsql_resource_manager import (
    CloudSQLResourceManager,
)
from tests.system.data_sources.test_bigquery import BQ_CONN


# Local testing requires the Cloud SQL Proxy.
# https://cloud.google.com/sql/docs/postgres/connect-admin-proxy

# Cloud SQL proxy listens to localhost
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "guestbook")
PROJECT_ID = os.getenv("PROJECT_ID")

CONN = {
    "source_type": "Postgres",
    "host": POSTGRES_HOST,
    "user": "postgres",
    "password": POSTGRES_PASSWORD,
    "port": 5432,
    "database": POSTGRES_DATABASE,
}


@pytest.fixture
def cloud_sql(request):
    if not request.config.getoption("--no-cloud-sql"):
        postgres_instance = CloudSQLResourceManager(
            PROJECT_ID,
            "POSTGRES_12",
            "data-validator-postgres12",
            POSTGRES_PASSWORD,
            database_id="guestbook",
            assign_public_ip=True,
            authorized_networks=None,
            cpu=1,
            memory="4GB",
            enable_bin_logs=False,
            already_exists=True,
        )

        # If instance already exists, returns host IP and does not add new data
        postgres_instance.setup()
        postgres_instance.add_data(
            "gs://pso-kokoro-resources/test/postgres/system_integration/postgres_data.sql"
        )
        postgres_instance.add_data(
            "gs://pso-kokoro-resources/test/postgres/system_integration/postgres_data_row.sql"
        )


def test_postgres_count(cloud_sql):
    """Test count validation on Postgres instance"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Column",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_AGGREGATES: [
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: None,
                consts.CONFIG_TARGET_COLUMN: None,
                consts.CONFIG_FIELD_ALIAS: "count",
            },
            {
                consts.CONFIG_TYPE: "count",
                consts.CONFIG_SOURCE_COLUMN: "guestname",
                consts.CONFIG_TARGET_COLUMN: "guestname",
                consts.CONFIG_FIELD_ALIAS: "count_guestname",
            },
            {
                consts.CONFIG_TYPE: "sum",
                consts.CONFIG_SOURCE_COLUMN: "entryid",
                consts.CONFIG_TARGET_COLUMN: "entryid",
                consts.CONFIG_FIELD_ALIAS: "sum_entryid",
            },
        ],
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"].equals(df["target_agg_value"])
    assert sorted(list(df["source_agg_value"])) == ["28", "7", "7"]


def test_postgres_row(cloud_sql):
    """Test row validaiton on Postgres"""
    config_row_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        # Validation Type
        consts.CONFIG_TYPE: "Row",
        # Configuration Required Depending on Validator Type
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "test_data_types_postgres_row",
        consts.CONFIG_COMPARISON_FIELDS: [
            {
                "source_column": "hash__all",
                "target_column": "hash__all",
                "field_alias": "hash__all",
                "cast": None,
            }
        ],
        consts.CONFIG_CALCULATED_FIELDS: [
            {
                "source_calculated_columns": ["serial_col"],
                "target_calculated_columns": ["serial_col"],
                "field_alias": "cast__serial_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["int_col"],
                "target_calculated_columns": ["int_col"],
                "field_alias": "cast__int_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["text_col"],
                "target_calculated_columns": ["text_col"],
                "field_alias": "cast__text_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["char_col"],
                "target_calculated_columns": ["char_col"],
                "field_alias": "cast__char_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["varchar_col"],
                "target_calculated_columns": ["varchar_col"],
                "field_alias": "cast__varchar_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["float_col"],
                "target_calculated_columns": ["float_col"],
                "field_alias": "cast__float_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["numeric_col"],
                "target_calculated_columns": ["numeric_col"],
                "field_alias": "cast__numeric_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["timestamp_col"],
                "target_calculated_columns": ["timestamp_col"],
                "field_alias": "cast__timestamp_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["date_col"],
                "target_calculated_columns": ["date_col"],
                "field_alias": "cast__date_col",
                "type": "cast",
                "depth": 0,
            },
            {
                "source_calculated_columns": ["cast__serial_col"],
                "target_calculated_columns": ["cast__serial_col"],
                "field_alias": "ifnull__cast__serial_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__int_col"],
                "target_calculated_columns": ["cast__int_col"],
                "field_alias": "ifnull__cast__int_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__text_col"],
                "target_calculated_columns": ["cast__text_col"],
                "field_alias": "ifnull__cast__text_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__char_col"],
                "target_calculated_columns": ["cast__char_col"],
                "field_alias": "ifnull__cast__char_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__varchar_col"],
                "target_calculated_columns": ["cast__varchar_col"],
                "field_alias": "ifnull__cast__varchar_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__float_col"],
                "target_calculated_columns": ["cast__float_col"],
                "field_alias": "ifnull__cast__float_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__numeric_col"],
                "target_calculated_columns": ["cast__numeric_col"],
                "field_alias": "ifnull__cast__numeric_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__timestamp_col"],
                "target_calculated_columns": ["cast__timestamp_col"],
                "field_alias": "ifnull__cast__timestamp_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["cast__date_col"],
                "target_calculated_columns": ["cast__date_col"],
                "field_alias": "ifnull__cast__date_col",
                "type": "ifnull",
                "depth": 1,
            },
            {
                "source_calculated_columns": ["ifnull__cast__serial_col"],
                "target_calculated_columns": ["ifnull__cast__serial_col"],
                "field_alias": "rstrip__ifnull__cast__serial_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__int_col"],
                "target_calculated_columns": ["ifnull__cast__int_col"],
                "field_alias": "rstrip__ifnull__cast__int_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__text_col"],
                "target_calculated_columns": ["ifnull__cast__text_col"],
                "field_alias": "rstrip__ifnull__cast__text_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__char_col"],
                "target_calculated_columns": ["ifnull__cast__char_col"],
                "field_alias": "rstrip__ifnull__cast__char_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__varchar_col"],
                "target_calculated_columns": ["ifnull__cast__varchar_col"],
                "field_alias": "rstrip__ifnull__cast__varchar_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__float_col"],
                "target_calculated_columns": ["ifnull__cast__float_col"],
                "field_alias": "rstrip__ifnull__cast__float_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__numeric_col"],
                "target_calculated_columns": ["ifnull__cast__numeric_col"],
                "field_alias": "rstrip__ifnull__cast__numeric_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__timestamp_col"],
                "target_calculated_columns": ["ifnull__cast__timestamp_col"],
                "field_alias": "rstrip__ifnull__cast__timestamp_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["ifnull__cast__date_col"],
                "target_calculated_columns": ["ifnull__cast__date_col"],
                "field_alias": "rstrip__ifnull__cast__date_col",
                "type": "rstrip",
                "depth": 2,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__serial_col"],
                "field_alias": "upper__rstrip__ifnull__cast__serial_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__int_col"],
                "field_alias": "upper__rstrip__ifnull__cast__int_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__text_col"],
                "field_alias": "upper__rstrip__ifnull__cast__text_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__char_col"],
                "field_alias": "upper__rstrip__ifnull__cast__char_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__varchar_col"],
                "field_alias": "upper__rstrip__ifnull__cast__varchar_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__float_col"],
                "field_alias": "upper__rstrip__ifnull__cast__float_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__numeric_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__numeric_col"],
                "field_alias": "upper__rstrip__ifnull__cast__numeric_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__timestamp_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__timestamp_col"],
                "field_alias": "upper__rstrip__ifnull__cast__timestamp_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                "target_calculated_columns": ["rstrip__ifnull__cast__date_col"],
                "field_alias": "upper__rstrip__ifnull__cast__date_col",
                "type": "upper",
                "depth": 3,
            },
            {
                "source_calculated_columns": [
                    "upper__rstrip__ifnull__cast__serial_col",
                    "upper__rstrip__ifnull__cast__int_col",
                    "upper__rstrip__ifnull__cast__text_col",
                    "upper__rstrip__ifnull__cast__char_col",
                    "upper__rstrip__ifnull__cast__varchar_col",
                    "upper__rstrip__ifnull__cast__float_col",
                    "upper__rstrip__ifnull__cast__numeric_col",
                    "upper__rstrip__ifnull__cast__timestamp_col",
                    "upper__rstrip__ifnull__cast__date_col",
                ],
                "target_calculated_columns": [
                    "upper__rstrip__ifnull__cast__serial_col",
                    "upper__rstrip__ifnull__cast__int_col",
                    "upper__rstrip__ifnull__cast__text_col",
                    "upper__rstrip__ifnull__cast__char_col",
                    "upper__rstrip__ifnull__cast__varchar_col",
                    "upper__rstrip__ifnull__cast__float_col",
                    "upper__rstrip__ifnull__cast__numeric_col",
                    "upper__rstrip__ifnull__cast__timestamp_col",
                    "upper__rstrip__ifnull__cast__date_col",
                ],
                "field_alias": "concat__all",
                "type": "concat",
                "depth": 4,
            },
            {
                "source_calculated_columns": ["concat__all"],
                "target_calculated_columns": ["concat__all"],
                "field_alias": "hash__all",
                "type": "hash",
                "depth": 5,
            },
        ],
        consts.CONFIG_PRIMARY_KEYS: [
            {
                "source_column": "serial_col",
                "target_column": "serial_col",
                "field_alias": "serial_col",
                "cast": None,
            }
        ],
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
        consts.CONFIG_RANDOM_ROW_BATCH_SIZE: "5",
        consts.CONFIG_USE_RANDOM_ROWS: True,
    }

    data_validator = data_validation.DataValidation(
        config_row_valid,
        verbose=False,
    )
    df = data_validator.execute()

    assert df["source_agg_value"][0] == df["target_agg_value"][0]


def test_schema_validation(cloud_sql):
    """Test schema validation on Postgres instance"""
    config_count_valid = {
        consts.CONFIG_SOURCE_CONN: CONN,
        consts.CONFIG_TARGET_CONN: CONN,
        consts.CONFIG_TYPE: "Schema",
        consts.CONFIG_SCHEMA_NAME: "public",
        consts.CONFIG_TABLE_NAME: "entries",
        consts.CONFIG_FORMAT: "table",
        consts.CONFIG_FILTER_STATUS: None,
    }

    data_validator = data_validation.DataValidation(
        config_count_valid,
        verbose=False,
    )
    df = data_validator.execute()

    for validation in df.to_dict(orient="records"):
        assert validation["validation_status"] == consts.VALIDATION_STATUS_SUCCESS


def mock_get_connection_config(*args):
    if args[1] in ("pg-conn", "mock-conn"):
        return CONN
    elif args[1] == "bq-conn":
        return BQ_CONN


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_schema_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "schema",
            "-sc=pg-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            (
                # PostgreSQL integrals go to BigQuery INT64.
                "--allow-list=int16:int64,int32:int64,int32[non-nullable]:int64,"
                # Oracle NUMBERS that map to BigQuery NUMERIC.
                "decimal(20,0):decimal(38,9),decimal(10,2):decimal(38,9),"
                # Oracle NUMBERS that map to BigQuery BIGNUMERIC.
                # When issue-839 is resolved we need to edit the line below as appropriate.
                "decimal(38,0):decimal(38,9),"
                # BigQuery does not have a float32 type.
                "float32:float64,"
                # TODO When issue-706 is complete remove the timestamp line below
                "timestamp('UTC'):timestamp"
            ),
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=*",
            "--min=*",
            "--max=*",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_column_validation_core_types_to_bigquery():
    parser = cli_tools.configure_arg_parser()
    # TODO Change --min/max strings below to include col_tstz when issue-706 is complete.
    # We've excluded col_float32 because BigQuery does not have an exact same type and float32/64 are lossy and cannot be compared.
    # TODO Change --sum and --max options to include col_char_2 when issue-842 is complete.
    args = parser.parse_args(
        [
            "validate",
            "column",
            "-sc=pg-conn",
            "-tc=bq-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--filter-status=fail",
            "--sum=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime",
            "--min=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime",
            "--max=col_int8,col_int16,col_int32,col_int64,col_dec_20,col_dec_38,col_dec_10_2,col_float64,col_varchar_30,col_string,col_date,col_datetime",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0


@mock.patch(
    "data_validation.state_manager.StateManager.get_connection_config",
    new=mock_get_connection_config,
)
def test_row_validation_core_types():
    parser = cli_tools.configure_arg_parser()
    args = parser.parse_args(
        [
            "validate",
            "row",
            "-sc=mock-conn",
            "-tc=mock-conn",
            "-tbls=pso_data_validator.dvt_core_types",
            "--primary-keys=id",
            "--filter-status=fail",
            "--hash=*",
        ]
    )
    config_managers = main.build_config_managers_from_args(args)
    assert len(config_managers) == 1
    config_manager = config_managers[0]
    validator = data_validation.DataValidation(config_manager.config, verbose=False)
    df = validator.execute()
    # With filter on failures the data frame should be empty
    assert len(df) == 0
