from ibis.backends.bigquery import Backend as BigQueryBackend


def _list_primary_key_columns(self, database: str, table: str) -> list:
    """Return a list of primary key column names."""
    # TODO nj@2024-11-08 It's not clear if this is possible, we should revisit if it becomes a requirement.
    return None


BigQueryBackend.list_primary_key_columns = _list_primary_key_columns
