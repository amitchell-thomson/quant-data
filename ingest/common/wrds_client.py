import os
import warnings

import pandas as pd
import psycopg2

from .base_client import BaseClient
from .provider import register


@register("wrds")
class WRDSClient(BaseClient):
    def __init__(self, provider_config: dict):
        """Initialise with provider config. Connection is lazy — opened on first fetch."""
        self._config = provider_config
        self._conn: psycopg2.extensions.connection | None = None

    def _connect(self):
        """Open WRDS connection via psycopg2. No-op if already connected. Password from ~/.pgpass."""
        if self._conn is not None:
            return
        username = os.getenv("WRDS_USERNAME")
        if not username:
            raise ValueError("WRDS_USERNAME environment variable not set")
        self._conn = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            dbname=self._config["dbname"],
            user=username,
        )

    def _get_date_column(self, dataset_cfg: dict) -> str:
        for col in dataset_cfg["primary_key"]:
            if dataset_cfg["columns"][col] == "date32":
                return col
        raise ValueError("No date32 column found in primary key")

    def fetch(self, dataset_cfg: dict, year: int | None = None) -> pd.DataFrame:
        self._connect()
        assert self._conn is not None

        cols = ", ".join(dataset_cfg["columns"].keys())
        table = dataset_cfg["source"]
        sql = f"SELECT {cols} FROM {table}"

        if year is not None:
            date_col = self._get_date_column(dataset_cfg)
            sql += f" WHERE {date_col} >= '{year}-01-01' AND {date_col} < '{year + 1}-01-01'"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pd.read_sql(sql, self._conn)

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None


if __name__ == "__main__":
    from ingest.common.utils import _get_dataset, _load_config

    providers = _load_config("providers")
    dataset = _get_dataset("equities.names")

    client = WRDSClient(providers["wrds"])
    df = client.fetch(dataset, year=1993)
    print(df.shape)
    print(df.head())
    client.close()
