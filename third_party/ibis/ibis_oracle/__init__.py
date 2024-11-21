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
import sqlalchemy as sa
from sqlalchemy.dialects.oracle.base import OracleIdentifierPreparer
from sqlalchemy.dialects.oracle.cx_oracle import OracleDialect_cx_oracle
import re

import ibis.expr.datatypes as dt
from typing import Iterable, Literal, Tuple
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from third_party.ibis.ibis_oracle.compiler import OracleCompiler
from third_party.ibis.ibis_oracle.datatypes import _get_type


def _ora_denormalize_name(self, name):
    """Oracle specific version of sqlalchemy/engine/default.py.denormalize_name()

    The original function upper cases most identifiers unless they require quoting
    before use in SQL. This includes when non standard characters are in play.

    This prevents dictionary queries from succeeding. Really, the presence of special
    characters should be irrelevant to upper/lower case decisions.

    This method uppercases identifiers that include special characters, otherwise it
    follows the original code path.
    """
    if name is None:
        return None

    if self.identifier_preparer._requires_quotes_illegal_chars(name):
        name = name.upper()
    return super(OracleDialect_cx_oracle, self).denormalize_name(name)


OracleDialect_cx_oracle.denormalize_name = _ora_denormalize_name


class DVTOracleIdentifierPreparer(OracleIdentifierPreparer):
    def quote_identifier(self, value):
        """Quote an identifier.

        This method adds extra path of upper casing the identifier if it is being quoted
        due to special characters. Otherwise we follow the original path.

        This is because all names are normalised to lower case in DVT which is fine because
        unquoted table name are upper cased automatically by Oracle. If we add quotes due to
        special characters then we lose the auto uppercase operation. This method forces it.
        """
        if self._requires_quotes_illegal_chars(value):
            return super().quote_identifier(value.upper())
        else:
            return super().quote_identifier(value)


OracleDialect_cx_oracle.preparer = DVTOracleIdentifierPreparer


class Backend(BaseAlchemyBackend):
    name = "oracle"
    compiler = OracleCompiler

    def __init__(self, arraysize: int = 500):
        super().__init__()
        self.arraysize = arraysize

    def do_connect(
        self,
        host: str = "localhost",
        user: str = None,
        password: str = None,
        port: int = 1521,
        database: str = None,
        protocol: str = "TCP",
        url: str = None,
        driver: Literal["cx_Oracle"] = "cx_Oracle",
    ) -> None:
        if url is None:
            if driver != "cx_Oracle":
                raise NotImplementedError(
                    "cx_Oracle is currently the only supported driver"
                )
            dsn = """(description=(address=(protocol={})(host={})(port={}))(connect_data=(service_name={})))""".format(
                protocol, host, port, database
            )
            sa_url = sa.engine.url.URL.create(
                "oracle+cx_oracle",
                user,
                password,
                dsn,
            )
        else:
            sa_url = sa.engine.url.make_url(url)

        self.database_name = sa_url.database
        engine = sa.create_engine(
            sa_url,
            poolclass=sa.pool.StaticPool,
            arraysize=self.arraysize,
            # The hardcoding of 128 below is not great but is the simplest way of dealing with:
            #   https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1250
            # There is a quirk of SQLAlchemy behaviour that is discussed in comments of issue 1250 that
            # we cannot easily deal with, even if we "fixed" it it could be months or years before we can
            # take advantage of it.
            # We manage the max length of identifiers generated by DVT in clients.get_max_column_length()
            # and in all other cases re-use existing column names (i.e. we know the length is already fine).
            # Therefore the ugly hardcoding of 128 kicks the can down the road and unblocks a customer
            # who is working with Oracle 11g and a max identifier length of 30.
            max_identifier_length=128,
            # Pessimistic disconnect handling
            pool_pre_ping=True,
        )
        try:
            # Identify the session in Oracle as DVT, no-op if this fails.
            engine.raw_connection().connection.module = "DVT"
        except Exception:
            pass

        @sa.event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cur:
                cur.execute("ALTER SESSION SET TIME_ZONE='UTC'")
                # Standardise numeric formatting on en_US (issue 1033).
                cur.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,'")

        super().do_connect(engine)

    def _metadata(self, query) -> Iterable[Tuple[str, dt.DataType]]:
        if (
            re.search(r"^\s*SELECT\s", query, flags=re.MULTILINE | re.IGNORECASE)
            is not None
        ):
            query = f"({query})"

        with self.begin() as con:
            result = con.exec_driver_sql(f"SELECT * FROM {query} t0 WHERE ROWNUM <= 1")
            cursor = result.cursor
            yield from ((column[0], _get_type(column)) for column in cursor.description)

    def list_primary_key_columns(self, database: str, table: str) -> list:
        """Return a list of primary key column names."""
        list_pk_col_sql = """
            SELECT cc.column_name
            FROM all_cons_columns cc
            INNER JOIN all_constraints c ON (cc.owner = c.owner AND cc.constraint_name = c.constraint_name AND cc.table_name = c.table_name)
            WHERE c.owner = :1
            AND c.table_name = :2
            AND c.constraint_type = 'P'
            ORDER BY cc.position
        """
        with self.begin() as con:
            result = con.exec_driver_sql(
                list_pk_col_sql, parameters=(database.upper(), table.upper())
            )
            return [_[0] for _ in result.cursor.fetchall()]

    def raw_metadata(self, query) -> dict:
        if (
            re.search(r"^\s*SELECT\s", query, flags=re.MULTILINE | re.IGNORECASE)
            is not None
        ):
            query = f"({query})"

        with self.begin() as con:
            result = con.exec_driver_sql(f"SELECT * FROM {query} t0 WHERE ROWNUM <= 1")
            cursor = result.cursor
            return {column[0]: column[1].name for column in cursor.description}
