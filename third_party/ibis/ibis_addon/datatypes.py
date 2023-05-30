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

import ibis.expr.datatypes as dt
from ibis.backends.mssql.datatypes import _MSSQL_TYPE_MAP
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.mssql.base import MSDialect


# Register additional MSSQL data types
@dt.dtype.register(MSDialect, (mssql.VARBINARY, mssql.IMAGE))
def sa_mssql_binary(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)

@dt.dtype.register(MSDialect, (mssql.NCHAR, mssql.NTEXT, mssql.NVARCHAR))
def sa_mssql_binary(_, satype, nullable=True):
    return dt.String(nullable=nullable)

# Needs to be VARCHAR insteaad of NVARCHAR for Hash function
_MSSQL_TYPE_MAP[dt.String] = mssql.VARCHAR
