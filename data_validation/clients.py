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


import copy
import warnings
import logging
import google.oauth2.service_account
import ibis
import pandas
import third_party.ibis.ibis_postgres.client
# import third_party.ibis.ibis_addon.base_sqlalchemy.alchemy
from google.cloud import bigquery
# from ibis.backends.pandas.client import PandasClient
# from ibis.backends.postgres.client import PostgreSQLClient
# from third_party.ibis.ibis_cloud_spanner.api import connect as spanner_connect
from third_party.ibis.ibis_impala.api import impala_connect
from data_validation import client_info, consts, exceptions
from data_validation.secret_manager import SecretManagerBuilder

ibis.options.sql.default_limit = None

# TODO(googleapis/google-auth-library-python#520): Remove after issue is resolved
warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)
warnings.filterwarnings(
    "ignore",
    "Cannot create BigQuery Storage client, the dependency google-cloud-bigquery-storage is not installed",
)
warnings.filterwarnings(
    "ignore", "The GenericFunction 'regex_extract' is already registered"
)


def _raise_missing_client_error(msg):
    def get_client_call(*args, **kwargs):
        raise Exception(msg)

    return get_client_call


# If you have a Teradata License there is an optional teradatasql import
try:
    from third_party.ibis.ibis_teradata.client import TeradataClient
except Exception:
    msg = "pip install teradatasql (requires Teradata licensing)"
    TeradataClient = _raise_missing_client_error(msg)

# If you have an cx_Oracle driver installed
try:
    from third_party.ibis.ibis_oracle.client import OracleClient
except Exception:
    OracleClient = _raise_missing_client_error("pip install cx_Oracle")

try:
    from third_party.ibis.ibis_snowflake.client import (
        SnowflakeClient as snowflake_connect,
    )
except Exception:
    snowflake_connect = _raise_missing_client_error(
        "pip install snowflake-connector-python"
    )

# If you have Db2 client installed
try:
    from third_party.ibis.ibis_DB2.client import DB2Client
except Exception:
    DB2Client = _raise_missing_client_error("pip install ibm_db_sa")


def get_bigquery_client(project_id, dataset_id="", credentials=None):
    info = client_info.get_http_client_info()
    google_client = bigquery.Client(
        project=project_id, client_info=info, credentials=credentials
    )

    ibis_client = ibis.bigquery.connect(
        project_id=project_id, dataset_id=dataset_id, credentials=credentials
    )

    # # Override the BigQuery client object to ensure the correct user agent is
    # included.
    ibis_client.client = google_client
    return ibis_client


def get_pandas_client(table_name, file_path, file_type):
    """Return pandas client and env with file loaded into DataFrame

    table_name (str): Table name to use as reference for file data
    file_path (str): The local, s3, or GCS file path to the data
    file_type (str): The file type of the file (csv or json)
    """
    if file_type == "csv":
        df = pandas.read_csv(file_path)
    elif file_type == "json":
        df = pandas.read_json(file_path)
    else:
        raise ValueError(f"Unknown Pandas File Type: {file_type}")

    pandas_client = ibis.pandas.connect({table_name: df})

    return pandas_client


def is_oracle_client(client):
    try:
        return isinstance(client, OracleClient)
    except TypeError:
        # When no Oracle client has been installed OracleClient is not a class
        return False


def get_ibis_table(client, schema_name, table_name, database_name=None):
    """Return Ibis Table for Supplied Client.

    client (IbisClient): Client to use for table
    schema_name (str): Schema name of table object
    table_name (str): Table name of table object
    database_name (str): Database name (generally default is used)
    """
    if type(client) in [
        # OracleClient,
        ibis.backends.postgres.Backend,
        # DB2Client,
        ibis.backends.mssql.Backend,
    ]:
        return client.table(table_name, database=database_name, schema=schema_name)
    elif type(client) in [ibis.backends.pandas.Backend]:
        return client.table(table_name, schema=schema_name)
    else:
        return client.table(table_name, database=schema_name)


def get_ibis_query(client, query):
    """Return Ibis Table from query expression for Supplied Client."""
    return client.sql(query)


def get_ibis_table_schema(client, schema_name, table_name):
    """Return Ibis Table Schema for Supplied Client.

    client (IbisClient): Client to use for table
    schema_name (str): Schema name of table object, may not need this since Backend uses database
    table_name (str): Table name of table object
    database_name (str): Database name (generally default is used)
    """
    if type(client) in [
        ibis.backends.mysql.Backend, 
        ibis.backends.postgres.Backend,
        ibis.backends.mssql.Backend,
    ]:
        return client.table(table_name).schema()
    else:
        return client.get_schema(table_name, schema_name)


def list_schemas(client):
    """Return a list of schemas in the DB."""
    if hasattr(client, "list_databases"):
        return client.list_databases()
    else:
        return [None]

def list_tables(client, schema_name):
    """Return a list of tables in the DB schema."""
    if type(client) in [
        # OracleClient,
        ibis.backends.postgres.Backend,
        # DB2Client,
        ibis.backends.mssql.Backend,
    ]:
        return client.list_tables()
    return client.list_tables(database=schema_name)

def get_all_tables(client, allowed_schemas=None):
    """Return a list of tuples with database and table names.

    client (IbisClient): Client to use for tables
    allowed_schemas (List[str]): List of schemas to pull.
    """
    table_objs = []
    schemas = list_schemas(client)
    for schema_name in schemas:
        if allowed_schemas and schema_name not in allowed_schemas:
            continue
        try:
            tables = list_tables(client, schema_name)
        except Exception as e:
            logging.warning(f"List Tables Error: {schema_name} -> {e}")
            continue

        for table_name in tables:
            table_objs.append((schema_name, table_name))

    return table_objs


def get_data_client(connection_config):
    """Return DataClient client from given configuration"""
    connection_config = copy.deepcopy(connection_config)
    source_type = connection_config.pop(consts.SOURCE_TYPE)
    secret_manager_type = connection_config.pop(consts.SECRET_MANAGER_TYPE, None)
    secret_manager_project_id = connection_config.pop(
        consts.SECRET_MANAGER_PROJECT_ID, None
    )

    decrypted_connection_config = {}
    if secret_manager_type is not None:
        sm = SecretManagerBuilder().build(secret_manager_type.lower())
        for config_item in connection_config:
            decrypted_connection_config[config_item] = sm.maybe_secret(
                secret_manager_project_id, connection_config[config_item]
            )
    else:
        decrypted_connection_config = connection_config

    # The ibis_bigquery.connect expects a credentials object, not a string.
    if consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH in decrypted_connection_config:
        key_path = decrypted_connection_config.pop(
            consts.GOOGLE_SERVICE_ACCOUNT_KEY_PATH
        )
        if key_path:
            decrypted_connection_config[
                "credentials"
            ] = google.oauth2.service_account.Credentials.from_service_account_file(
                key_path
            )

    if source_type not in CLIENT_LOOKUP:
        msg = 'ConfigurationError: Source type "{source_type}" is not supported'.format(
            source_type=source_type
        )
        raise Exception(msg)

    try:
        data_client = CLIENT_LOOKUP[source_type](**decrypted_connection_config)
        data_client._source_type = source_type
    except Exception as e:
        msg = 'Connection Type "{source_type}" could not connect: {error}'.format(
            source_type=source_type, error=str(e)
        )
        raise exceptions.DataClientConnectionFailure(msg)

    return data_client


def get_max_column_length(client):
    """Return the max column length supported by client.

    client (IbisClient): Client to use for tables
    """
    if is_oracle_client(client):
        # We can't reliably know which Version class client.version is stored in
        # because it is out of our control. Therefore using string identification
        # of Oracle <= 12.1 to avoid exceptions of this nature:
        #  TypeError: '<' not supported between instances of 'Version' and 'Version'
        if str(client.version)[:2] in ["10", "11"] or str(client.version)[:4] == "12.1":
            return 30
    return 128


CLIENT_LOOKUP = {
    "BigQuery": get_bigquery_client,
    "Impala": impala_connect,
    "MySQL": ibis.mysql.connect,
    "Oracle": OracleClient,
    "FileSystem": get_pandas_client,
    "Postgres": ibis.postgres.connect,
    "Redshift": ibis.postgres.connect,
    "Teradata": TeradataClient,
    "MSSQL": ibis.mssql.connect,
    "Snowflake": snowflake_connect,
    # "Spanner": spanner_connect,
    "DB2": DB2Client,
}
