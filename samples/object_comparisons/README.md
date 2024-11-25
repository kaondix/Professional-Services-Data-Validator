# DVT for Object Comparisons

DVT is not intended for object comparisons, it is a data validation tool. But
with some creativity it is easy to see how DVT could become part of an object
validation workflow when provided with adequate dictionary queries.

The files within this sample folder demonstrate the principle, SQL statements
may need modifying to suit individual needs.

```
data-validation validate custom-query row -sc=ora_local -tc=pg_local \
-sqf="oracle/tables.sql" -tqf="postgres/tables.sql" \
--filters="schema = 'pso_data_validator'" \
--primary-keys=schema,name --concat='*'

data-validation validate custom-query row -sc=ora_local -tc=pg_local \
-sqf="oracle/views.sql" -tqf="postgres/views.sql" \
--filters="schema = 'pso_data_validator'" \
--primary-keys=schema,name --concat='*' \
--filter-status=fail

data-validation validate custom-query row -sc=ora_local -tc=pg_local \
-sqf="oracle/sequences.sql" -tqf="postgres/sequences.sql" \
--filters="schema = 'pso_data_validator'" \
--primary-keys=schema,name --concat='*' \
--filter-status=fail

data-validation validate custom-query row -sc=ora_local -tc=pg_local \
-sqf="oracle/triggers.sql" -tqf="postgres/triggers.sql" \
--filters="schema = 'pso_data_validator'" \
--primary-keys=schema,name --concat='*' \
--filter-status=fail

data-validation validate custom-query row -sc=ora_local -tc=pg_local \
-sqf="oracle/pks.sql" -tqf="postgres/pks.sql" \
--filters="schema = 'pso_data_validator'" \
--primary-keys=schema,tablename,name --concat='*' \
--filter-status=fail

data-validation validate custom-query row -sc=ora_local -tc=pg_local \
-sqf="oracle/fks.sql" -tqf="postgres/fks.sql" \
--filters="schema = 'pso_data_validator'" \
--primary-keys=schema,tablename,name --concat='*' \
--filter-status=fail
```

TODO Should probably make the above commands a simple bash script.
