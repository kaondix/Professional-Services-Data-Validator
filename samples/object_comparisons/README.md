# DVT for Object Comparisons

DVT is not intended for object comparisons, it is a data validation tool. But
with some creativity it is easy to see how DVT could become part of an object
validation workflow when provided with adequate dictionary queries.

The files within this sample folder demonstrate the general principle. SQL statements
may need modifying to suit individual needs.

The sample shell script `./reconcile.sh` shows how the files can be executed using DVT, for example:

```
./reconcile.sh oracle postgres
```

Example output:
```
Processing file: cons_fk.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤════════════════════╤════════════════════╤══════════════════╤═════════════════════╤══════════╕
│ validation_name   │ validation_type   │ source_table_name   │ source_column_name   │ source_agg_value   │ target_agg_value   │ pct_difference   │ validation_status   │ run_id   │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪════════════════════╪════════════════════╪══════════════════╪═════════════════════╪══════════╡
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧════════════════════╧════════════════════╧══════════════════╧═════════════════════╧══════════╛
Processing file: cons_nn.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤══════════════════════════════════════╤═════════════════════════════════════════════════╤══════════════════╤═════════════════════╤══════════════════════════════════════╕
│ validation_name   │ validation_type   │   source_table_name │ source_column_name   │ source_agg_value                     │ target_agg_value                                │   pct_difference │ validation_status   │ run_id                               │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪══════════════════════════════════════╪═════════════════════════════════════════════════╪══════════════════╪═════════════════════╪══════════════════════════════════════╡
│ concat__all       │ Custom-query      │                     │ concat__all          │ pso_data_validatoritems_priceitem_id │ nan                                             │              nan │ fail                │ c08302b6-a5c4-4b26-b428-8dfbedf5dab8 │
├───────────────────┼───────────────────┼─────────────────────┼──────────────────────┼──────────────────────────────────────┼─────────────────────────────────────────────────┼──────────────────┼─────────────────────┼──────────────────────────────────────┤
│ concat__all       │ Custom-query      │                 nan │ nan                  │ nan                                  │ pso_data_validatordvt_pg_types_id_seqlast_value │              nan │ fail                │ c08302b6-a5c4-4b26-b428-8dfbedf5dab8 │
├───────────────────┼───────────────────┼─────────────────────┼──────────────────────┼──────────────────────────────────────┼─────────────────────────────────────────────────┼──────────────────┼─────────────────────┼──────────────────────────────────────┤
│ concat__all       │ Custom-query      │                 nan │ nan                  │ nan                                  │ pso_data_validatordvt_pg_types_id_seqlog_cnt    │              nan │ fail                │ c08302b6-a5c4-4b26-b428-8dfbedf5dab8 │
├───────────────────┼───────────────────┼─────────────────────┼──────────────────────┼──────────────────────────────────────┼─────────────────────────────────────────────────┼──────────────────┼─────────────────────┼──────────────────────────────────────┤
│ concat__all       │ Custom-query      │                 nan │ nan                  │ nan                                  │ pso_data_validatordvt_pg_types_id_seqis_called  │              nan │ fail                │ c08302b6-a5c4-4b26-b428-8dfbedf5dab8 │
├───────────────────┼───────────────────┼─────────────────────┼──────────────────────┼──────────────────────────────────────┼─────────────────────────────────────────────────┼──────────────────┼─────────────────────┼──────────────────────────────────────┤
│ concat__all       │ Custom-query      │                 nan │ nan                  │ nan                                  │ pso_data_validatordvt_pg_typesid                │              nan │ fail                │ c08302b6-a5c4-4b26-b428-8dfbedf5dab8 │
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧══════════════════════════════════════╧═════════════════════════════════════════════════╧══════════════════╧═════════════════════╧══════════════════════════════════════╛
Processing file: cons_pk.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤═══════════════════════════════════╤══════════════════════════════════╤══════════════════╤═════════════════════╤══════════════════════════════════════╕
│ validation_name   │ validation_type   │   source_table_name │ source_column_name   │ source_agg_value                  │ target_agg_value                 │   pct_difference │ validation_status   │ run_id                               │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪═══════════════════════════════════╪══════════════════════════════════╪══════════════════╪═════════════════════╪══════════════════════════════════════╡
│ concat__all       │ Custom-query      │                 nan │ nan                  │ nan                               │ pso_data_validatordvt_pg_typesid │              nan │ fail                │ 7c297c95-ea7b-4c94-bd16-081427faedf9 │
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧═══════════════════════════════════╧══════════════════════════════════╧══════════════════╧═════════════════════╧══════════════════════════════════════╛
Processing file: sequences.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤════════════════════╤════════════════════╤══════════════════╤═════════════════════╤══════════╕
│ validation_name   │ validation_type   │ source_table_name   │ source_column_name   │ source_agg_value   │ target_agg_value   │ pct_difference   │ validation_status   │ run_id   │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪════════════════════╪════════════════════╪══════════════════╪═════════════════════╪══════════╡
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧════════════════════╧════════════════════╧══════════════════╧═════════════════════╧══════════╛
Processing file: tables.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤══════════════════════════════════╤═════════════════════════════════╤══════════════════╤═════════════════════╤══════════════════════════════════════╕
│ validation_name   │ validation_type   │   source_table_name │ source_column_name   │ source_agg_value                 │ target_agg_value                │   pct_difference │ validation_status   │ run_id                               │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪══════════════════════════════════╪═════════════════════════════════╪══════════════════╪═════════════════════╪══════════════════════════════════════╡
│ concat__all       │ Custom-query      │                     │ concat__all          │ pso_data_validatoritems_priceN   │ nan                             │              nan │ fail                │ 573c44b8-bd93-4337-8838-c2f4057501d1 │
├───────────────────┼───────────────────┼─────────────────────┼──────────────────────┼──────────────────────────────────┼─────────────────────────────────┼──────────────────┼─────────────────────┼──────────────────────────────────────┤
│ concat__all       │ Custom-query      │                 nan │ nan                  │ nan                              │ pso_data_validatordvt_pg_typesN │              nan │ fail                │ 573c44b8-bd93-4337-8838-c2f4057501d1 │
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧══════════════════════════════════╧═════════════════════════════════╧══════════════════╧═════════════════════╧══════════════════════════════════════╛
Processing file: triggers.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤════════════════════╤════════════════════╤══════════════════╤═════════════════════╤══════════╕
│ validation_name   │ validation_type   │ source_table_name   │ source_column_name   │ source_agg_value   │ target_agg_value   │ pct_difference   │ validation_status   │ run_id   │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪════════════════════╪════════════════════╪══════════════════╪═════════════════════╪══════════╡
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧════════════════════╧════════════════════╧══════════════════╧═════════════════════╧══════════╛
Processing file: views.sql
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤════════════════════╤════════════════════╤══════════════════╤═════════════════════╤══════════╕
│ validation_name   │ validation_type   │ source_table_name   │ source_column_name   │ source_agg_value   │ target_agg_value   │ pct_difference   │ validation_status   │ run_id   │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪════════════════════╪════════════════════╪══════════════════╪═════════════════════╪══════════╡
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧════════════════════╧════════════════════╧══════════════════╧═════════════════════╧══════════╛
```