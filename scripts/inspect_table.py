"""
Inspect available columns in a WRDS table.

Usage:
    uv run -m scripts.inspect_table crsp.dsf
    uv run -m scripts.inspect_table crsp.stocknames --sample
"""

import sys
import warnings

from ingest.common.config_utils import _load_config
from ingest.common.wrds_client import WRDSClient


def inspect(table: str, sample: bool = False) -> None:
    providers = _load_config("providers")
    client = WRDSClient(providers["wrds"])
    client._connect()
    assert client._conn is not None

    schema, tbl = table.split(".", 1)
    sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{tbl}'
        ORDER BY ordinal_position
    """
    import pandas as pd

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        cols = pd.read_sql(sql, client._conn)

    print(f"\n{table} — {len(cols)} columns")
    print(cols.to_string(index=False))

    if sample:
        sample_sql = f"SELECT * FROM {table} LIMIT 3"
        df = pd.read_sql(sample_sql, client._conn)
        print(f"\nSample rows:")
        print(df.to_string())

    client.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run -m scripts.inspect_table <schema.table> [--sample]")
        sys.exit(1)

    table_arg = sys.argv[1]
    show_sample = "--sample" in sys.argv
    inspect(table_arg, sample=show_sample)
