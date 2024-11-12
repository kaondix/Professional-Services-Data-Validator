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

import argparse
import logging
import os
from unittest import mock
import pytest

from data_validation import cli_tools, exceptions, config_manager, consts
from data_validation import __main__ as main


TEST_CONN = '{"source_type":"Example"}'
CLI_ARGS = {
    "command": "validate",
    "validate_cmd": "column",
    "source_conn": TEST_CONN,
    "target_conn": TEST_CONN,
    "tables_list": "my_schema.my_table",
    "sum": "col_a,col_b",
    "count": "col_a,col_b",
    "config_file": "example_test.yaml",
    "verbose": True,
}

BASE_RUNNER_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "dry_run": False,
    "command": "configs",
    "validation_config_cmd": "run",
    "kube_completions": True,
}
CONFIG_RUNNER_ARGS_1 = BASE_RUNNER_ARGS | {
    "config_file": "gs://pso-kokoro-resources/resources/test/unit/test__main/3validations/first.yaml",
    "config_dir": None,
}
CONFIG_RUNNER_ARGS_2 = BASE_RUNNER_ARGS | {
    "config_dir": "gs://pso-kokoro-resources/resources/test/unit/test__main/3validations",
}
CONFIG_RUNNER_ARGS_3 = BASE_RUNNER_ARGS | {
    "config_dir": "gs://pso-kokoro-resources/resources/test/unit/test__main/4partitions",
}
CONFIG_RUNNER_ARGS_4 = CONFIG_RUNNER_ARGS_3 | {
    "kube_completions": False,
}
CONFIG_RUNNER_EXCEPTION_TEXT = (
    "Error '{}' occurred while running config file {}. Skipping it for now."
)
VALIDATE_COLUMN_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "validate",
    "validate_cmd": "column",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
    consts.CONFIG_SOURCE_CONN: TEST_CONN,
    consts.CONFIG_TARGET_CONN: TEST_CONN,
    consts.CONFIG_FILE: None,
    consts.CONFIG_FILE_JSON: None,
}
BROKEN_VALIDATE_COLUMN_CONFIG_MISSING_COMMAND = {
    k: v for k, v in VALIDATE_COLUMN_CONFIG.items() if k != "command"
}  # as above without the command item
BROKEN_VALIDATE_COLUMN_CONFIG_INCORRECT_COMMAND = VALIDATE_COLUMN_CONFIG | {
    "command": "incorrectcommand"
}  # as above with command item replaced
VALIDATE_ROW_CONFIG = VALIDATE_COLUMN_CONFIG | {
    "validate_cmd": "row",
    consts.CONFIG_TYPE: consts.ROW_VALIDATION,
}  # as above with 2 items replaced
VALIDATE_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "configs",
    "validation_config_cmd": "run",
    "dry_run": False,
    consts.CONFIG_TYPE: consts.ROW_VALIDATION,
    consts.CONFIG_FILE: "test.yaml",
    "config_dir": None,
    "kube_completions": None,
}
CONNECTION_LIST_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "list",
}
CONNECTION_ADD_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "connections",
    "connect_cmd": "add",
    "connect_type": "BigQuery",
    consts.SECRET_MANAGER_TYPE: "gcp",
    consts.SECRET_MANAGER_PROJECT_ID: "dummy-gcp-project",
    consts.PROJECT_ID: "dummy-gcp-project",
    consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH: None,
    "connection_name": "dummy-bq-connection",
}
BROKEN_CONNECTION_CONFIG_INCORRECT_COMMAND = CONNECTION_ADD_ARGS | {
    "command": "incorrectcommand"
}  # as above with command item replaced
FIND_TABLES_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "find-tables",
}
DEPLOY_ARGS = {
    "verbose": False,
    "log_level": "INFO",
    "command": "deploy",
}
GENERATE_PARTITIONS_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "generate-table-partitions",
    "partition_num": 9,
    "parts_per_file": 5,
    "tables_list": "my_schema.my_table",
    consts.CONFIG_TYPE: consts.COLUMN_VALIDATION,
}
QUERY_CONFIG = {
    "verbose": False,
    "log_level": "INFO",
    "command": "query",
    "conn": "dummy-bq-connection",
    "query": "SELECT 1 AS TEST",
}


class MockIbisClient(object):
    _source_type = "BigQuery"
    name = "bigquery"


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CLI_ARGS),
)
def test_configure_arg_parser(mock_args):
    """Test arg parser values."""
    args = cli_tools.get_parsed_args()
    file_path = main._get_arg_config_file(args)

    assert file_path == "example_test.yaml"


@mock.patch("data_validation.__main__.run_validations")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_1),
)
def test_config_runner_1(mock_args, mock_build, mock_run, caplog):
    """config_runner, runs the validations, so we have to mock run_validations and examine the arguments
    passed to it. Build Config Managers reads the yaml files and builds the validation configs,
    which also includes creating a connection to the database. That is beyond a unit test, so mock
    build_config_managers_from_yaml.
    First test - run validation on a single file - and provide the -kc argument
    Expected result
    1. One config manager created
    2. Warning about inappropriate use of -kc
    Other test cases can be developed.
    """
    caplog.set_level(logging.WARNING)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    main.config_runner(args)
    # assert warning is seen
    assert caplog.messages == [
        "--kube-completions or -kc specified, which requires a config directory, however a specific config file is provided."
    ]
    # assert that only one config manager object is present
    assert len(mock_run.call_args.args[1]) == 1


@mock.patch("data_validation.__main__.run_validations")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_2),
)
def test_config_runner_2(mock_args, mock_build, mock_run, caplog):
    """Second test - run validation on a directory - and provide the -kc argument,
    but not running in a Kubernetes Completion Configuration. Expected result
    1. Multiple (3) config manager created for validation
    2. Warning about inappropriate use of -kc"""
    caplog.set_level(logging.WARNING)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    main.config_runner(args)
    # assert warning is seen
    assert caplog.messages == [
        "--kube-completions or -kc specified, however not running in Kubernetes Job completion, check your command line."
    ]
    # assert that validation is called thrice, once for each file
    assert mock_run.call_count == 3


@mock.patch("data_validation.__main__.run_validations")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_3(mock_args, mock_build, mock_run, caplog):
    """Second test - run validation on a directory - and provide the -kc argument,
    have system believe it is running in a Kubernetes Completion Environment. Expected result
    1. No warnings
    2. run validation called as though config file is provided (config_dir is None)
    3. run validation config file name corresponds to value of JOB_COMPLETION_INDEX
    4. One config manager created for validation
    """
    caplog.set_level(logging.WARNING)
    os.environ["JOB_COMPLETION_INDEX"] = "2"
    args = cli_tools.get_parsed_args()
    caplog.clear()
    main.config_runner(args)
    # assert no warnings
    assert caplog.messages == []
    # assert that only one config manager and one validation corresponding to JOB_COMPLETION_INDEX is set.
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "0002.yaml"
    assert len(mock_run.call_args.args[1]) == 1


@mock.patch("data_validation.__main__.run_validations")
@mock.patch(
    "data_validation.__main__.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_4),
)
def test_config_runner_4(mock_args, mock_build, mock_run, caplog):
    """Third test - run validation on a directory with failures in one validation,
        Running in a non Kube completions environment. Expected Result:
    1. All 4 files are validated, even though one of them raises an exception.
    2. Exception from one validation is trapped, file skipped and raised at the end.
    """
    mock_run.side_effect = [10, ValueError("Boom!"), 12, 10]
    caplog.set_level(logging.WARNING)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    with pytest.raises(exceptions.ValidationException) as e_info:
        main.config_runner(args)
    # assert that exception message was output for the failed validation
    # validation is called four times, once for each file
    # After all four files were validated, an exception was raised back to main to return status
    assert caplog.messages[0] == CONFIG_RUNNER_EXCEPTION_TEXT.format(
        "Boom!", "0001.yaml"
    )
    assert mock_run.call_count == 4
    assert e_info.value.args[0] == "Some of the validations raised an exception"
=======
def run_main_and_check_exit_code(expected_code):
    exit_code = main.main()
    assert exit_code == expected_code


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_COLUMN_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_COLUMN_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation")
def test_exit_code_0_for_successful_column_validation(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_COLUMN_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_COLUMN_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_column_validation(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**BROKEN_VALIDATE_COLUMN_CONFIG_MISSING_COMMAND),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            BROKEN_VALIDATE_COLUMN_CONFIG_MISSING_COMMAND,
            MockIbisClient(),
            MockIbisClient(),
            verbose=False,
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation")
def test_exit_code_1_for_malformed_input_config_missing_command(
    mock_args, mock_build, mock_run
):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**BROKEN_VALIDATE_COLUMN_CONFIG_INCORRECT_COMMAND),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            BROKEN_VALIDATE_COLUMN_CONFIG_INCORRECT_COMMAND,
            MockIbisClient(),
            MockIbisClient(),
            verbose=False,
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation")
def test_exit_code_1_for_malformed_input_config_incorrect_command(
    mock_args, mock_build, mock_run
):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_ROW_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_ROW_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation")
def test_exit_code_0_for_successful_row_validation(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_ROW_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_ROW_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_row_validation(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_yaml",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation")
def test_exit_code_0_for_successful_validation_config(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**VALIDATE_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_yaml",
    return_value=[
        config_manager.ConfigManager(
            VALIDATE_CONFIG, MockIbisClient(), MockIbisClient(), verbose=False
        )
    ],
)
@mock.patch("data_validation.__main__.run_validation", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_validation_config(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_LIST_ARGS),
)
@mock.patch("data_validation.cli_tools.list_connections")
def test_exit_code_0_for_successful_connection_list(mock_args, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_LIST_ARGS),
)
@mock.patch(
    "data_validation.cli_tools.list_connections", side_effect=Exception("Boom!")
)
def test_exit_code_1_for_failed_connection_list(mock_args, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_ADD_ARGS),
)
@mock.patch("data_validation.clients.get_data_client")
def test_exit_code_0_for_successful_connection_add(mock_args, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONNECTION_ADD_ARGS),
)
@mock.patch("data_validation.clients.get_data_client", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_connection_add(mock_args, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**BROKEN_CONNECTION_CONFIG_INCORRECT_COMMAND),
)
@mock.patch("data_validation.clients.get_data_client")
def test_exit_code_1_for_malformed_input_connection_config_incorrect_command(
    mock_args, mock_run
):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**FIND_TABLES_ARGS),
)
@mock.patch("data_validation.__main__.find_tables_using_string_matching")
def test_exit_code_0_for_successful_find_tables(mock_args, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**FIND_TABLES_ARGS),
)
@mock.patch(
    "data_validation.__main__.find_tables_using_string_matching",
    side_effect=Exception("Boom!"),
)
def test_exit_code_1_for_failed_find_tables(mock_args, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**DEPLOY_ARGS),
)
@mock.patch("data_validation.app.app.run")
def test_exit_code_0_for_successful_deploy(mock_args, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**DEPLOY_ARGS),
)
@mock.patch("data_validation.app.app.run", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_deploy(mock_args, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**GENERATE_PARTITIONS_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            GENERATE_PARTITIONS_CONFIG,
            MockIbisClient(),
            MockIbisClient(),
            verbose=False,
        )
    ],
)
@mock.patch("data_validation.__main__.PartitionBuilder")
def test_exit_code_0_for_successful_generate_partitions(
    mock_args, mock_build, mock_run
):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**GENERATE_PARTITIONS_CONFIG),
)
@mock.patch(
    "data_validation.__main__.build_config_managers_from_args",
    return_value=[
        config_manager.ConfigManager(
            GENERATE_PARTITIONS_CONFIG,
            MockIbisClient(),
            MockIbisClient(),
            verbose=False,
        )
    ],
)
@mock.patch("data_validation.__main__.PartitionBuilder", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_generate_partitions(mock_args, mock_build, mock_run):
    run_main_and_check_exit_code(1)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**QUERY_CONFIG),
)
@mock.patch("data_validation.clients.get_data_client")
def test_exit_code_0_for_successful_query(mock_args, mock_run):
    run_main_and_check_exit_code(0)


@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**QUERY_CONFIG),
)
@mock.patch("data_validation.clients.get_data_client", side_effect=Exception("Boom!"))
def test_exit_code_1_for_failed_query(mock_args, mock_run):
    run_main_and_check_exit_code(1)
