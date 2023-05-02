# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas
import warnings

import teradatasql
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from typing import Mapping, Any
import ibis.expr.schema as sch
from ibis.backends.base.sql import BaseSQLBackend
from third_party.ibis.ibis_teradata.compiler import TeradataCompiler
from third_party.ibis.ibis_teradata.datatypes import _get_type, _teradatasql_to_ibis_type


class Backend(BaseSQLBackend):
    name = "teradata"
    compiler = TeradataCompiler
    NO_LOCK_SQL = "LOCKING ROW FOR ACCESS "

    def do_connect(
        self,
        host: str = "localhost",
        user_name: str | None = None,
        password: str | None = None,
        port: int = 1025,
        logmech: str | None = 'TD2',
        use_no_lock_tables: bool = False,
    ) -> None:
        self.teradata_config = {
            "host": host,
            "user": user_name,
            "password": password,
            "dbs_port": port,
            "logmech": logmech,
        }

        self.client = teradatasql.connect(**self.teradata_config)
        self.con = self.client.cursor()
        self.use_no_lock_tables = use_no_lock_tables

    def close(self):
        """Close the connection."""
        self.con.close()

    @property
    def version(self):
        return teradatasql.vernumber.sVersionNumber
    
    LIST_DATABASE_SQL = """
    SELECT * FROM DBC.Databases
    WHERE DatabaseName LIKE '%{database_like}%'
    """  

    def list_databases(self, like=None):
        database_like = like or ""

        list_database_sql = self.LIST_DATABASE_SQL.format(database_like=database_like)
        databases_df = self._execute(list_database_sql, results=True)

        return list(databases_df.DatabaseName.str.rstrip())

    LIST_TABLE_SQL = """
        SELECT * FROM DBC.Tables
        WHERE DatabaseName LIKE '%{database_like}%'
        AND TableName LIKE '%{table_like}%'
    """  
    def list_tables(self, like=None, database=None):
        database = database or ""
        table = like or ""

        list_table_sql = self.LIST_TABLE_SQL.format(
            database_like=database, table_like=table
        )
        tables_df = self._execute(list_table_sql, results=True)
        return list(tables_df.TableName.str.rstrip())
    
    def _fully_qualified_name(self, name, database):
        if database:
            return "{}.{}".format(database, name)
        else:
            return name
     
    def _breakdown_qualified_name(self, qualified_name):
        database, table = qualified_name.split(".")
        return database, table

    def _get_table_schema(self, qualified_name): 
        dataset, table = self._breakdown_qualified_name(qualified_name)
        return self.get_schema(table, database=dataset)
    
    TABLE_SCHEMA_SQL = """
    HELP COLUMN {qualified_name}.*;
    """ 

    def _get_teradata_schema(self, qualified_name):
        query = self.TABLE_SCHEMA_SQL.format(qualified_name=qualified_name)
        pairs = [row[:2] for row in self.raw_sql(query)]
        names, types = zip(*pairs)

        clean_names = [name.rstrip() for name in names]
        ibis_types = [_get_type(type.rstrip()) for type in types]
        ibis_fields = dict(zip(clean_names, ibis_types))

        return ibis_fields


    def get_schema(self, table_name: str, database: str = None) -> sch.Schema:
        """Return a Schema object for the indicated table and database.
        Parameters
        ----------
        table_name
            Table name
        database
            Database name
        Returns
        -------
        Schema
            Ibis schema
        """
        qualified_name = self._fully_qualified_name(table_name, database)
        schema = self._get_teradata_schema(qualified_name)
        return sch.Schema(schema)


    def _get_schema_using_query(self, query):
        cur = self.raw_sql(query)
        # resets the state of the cursor and closes operation
        cur.fetchall()
        ibis_fields = self._adapt_types(cur.description)
        return sch.Schema(ibis_fields)
    
    def _adapt_types(self, descr):
        names = []
        adapted_types = []
        for col in descr:
            names.append(col[0])
            teradata_typename = col[1]
            typename = _teradatasql_to_ibis_type[teradata_typename]

            if typename == 'decimal':
                precision, scale = col[4:6]
                adapted_types.append(dt.Decimal(precision, scale))
            else:
                adapted_types.append(typename)
        return dict(zip(names, adapted_types))

    def _execute(self, sql, results=False):
        if self.use_no_lock_tables and sql.strip().startswith("SELECT"):
            sql = self.NO_LOCK_SQL + sql

        with warnings.catch_warnings():
            # Suppress pandas warning of SQLAlchemy connectable DB support
            warnings.simplefilter("ignore")
            df = pandas.read_sql(sql, self.client)

        if results:
            return df

        return None

    def execute(
        self,
        expr: ir.Expr,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: str = 'default',
        **kwargs: Any,
    ):
        """Compile and execute an Ibis expression.
        Compile and execute Ibis expression using this backend client
        interface, returning results in-memory in the appropriate object type
        Parameters
        ----------
        expr
            Ibis expression
        limit
            For expressions yielding result sets; retrieve at most this number
            of values/rows. Overrides any limit already set on the expression.
        params
            Named unbound parameters
        kwargs
            Backend specific arguments. For example, the clickhouse backend
            uses this to receive `external_tables` as a dictionary of pandas
            DataFrames.
        Returns
        -------
        DataFrame | Series | Scalar
            * `Table`: pandas.DataFrame
            * `Column`: pandas.Series
            * `Scalar`: Python scalar value
        """
        # Overwrite the execute() function to use read_sql method 
        kwargs.pop('timecontext', None)
        query_ast = self.compiler.to_ast_ensure_limit(expr, limit, params=params)
        sql = query_ast.compile()
        self._log(sql)

        schema = self.ast_schema(query_ast, **kwargs)

        self._register_in_memory_tables(expr)

        if self.use_no_lock_tables and sql.strip().startswith("SELECT"):
            sql = self.NO_LOCK_SQL + self.compiled_sql

        with warnings.catch_warnings():
            # Suppress pandas warning of SQLAlchemy connectable DB support
            warnings.simplefilter("ignore")
            df = pandas.read_sql(sql, self.client)
        return df

        
    # Methods we need to implement for BaseSQLBackend
    def create_table(self):
        pass
    
    def create_view(self):
        pass
    
    def current_database(self):
        pass
    
    def drop_table(self):
        pass
    
    def drop_view(self):
        pass

    def fetch_from_cursor(self, cursor, schema):
        pass


