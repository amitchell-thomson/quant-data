import os
import time
from datetime import date

import pandas as pd
import requests
from tenacity import Retrying, stop_after_attempt, wait_exponential

from .base_client import BaseClient
from .provider import register


@register("fred")
class FREDClient(BaseClient):
    def __init__(self, provider_config: dict):
        self._config = provider_config
        self._api_key = os.getenv("FRED_API_KEY")
        if not self._api_key:
            raise ValueError("FRED_API_KEY environment variable not set")
        self._session = requests.Session()
        self._min_interval = 60.0 / provider_config["rate_limit"]["requests_per_minute"]
        self._last_request = 0.0

    def _throttle(self):
        elapsed = time.monotonic() - self._last_request
        gap = self._min_interval - elapsed
        if gap > 0:
            time.sleep(gap)

    def _get(self, endpoint: str, params: dict) -> dict:
        self._throttle()
        full_params = {**params, "api_key": self._api_key, "file_type": "json"}
        url = f"{self._config['base_url']}{endpoint}"

        for attempt in Retrying(
            stop=stop_after_attempt(self._config["max_retries"]),
            wait=wait_exponential(multiplier=self._config["retry_backoff_factor"]),
            reraise=True,
        ):
            with attempt:
                response = self._session.get(url, params=full_params, timeout=self._config["timeout"])
                response.raise_for_status()  # raises HTTPError on 4xx/ 5xx status codes
                self._last_request = time.monotonic()
                return response.json()

        raise RuntimeError("unreachable")

    def _fetch_series(self, series_id: str, meta: dict, start: str | None, end: str | None, realtime: bool) -> pd.DataFrame:
        params: dict = {"series_id": series_id}
        if start:
            params["observation_start"] = start
        if end:
            params["observation_end"] = end
        if realtime:
            params["realtime_start"] = "1776-07-04"        # FRED's beginning of time
            params["realtime_end"] = date.today().isoformat()  # all vintages up to today

        data = self._get("/series/observations", params)
        df = pd.DataFrame(data["observations"])

        df = df[df["value"] != "."].copy()  # FRED uses "." for missing
        df["value"] = pd.to_numeric(df["value"])
        df["date"] = pd.to_datetime(df["date"])
        df["series_code"] = series_id
        df["series_name"] = meta["description"]
        df["category"] = meta["name"]

        cols = ["series_code", "date", "value", "series_name", "category"]
        if realtime:
            cols += ["realtime_start", "realtime_end"]
        return pd.DataFrame(df[cols])

    def fetch(
        self, dataset_cfg: dict, year: int | None = None, start: str | None = None, end: str | None = None, realtime: bool = False
    ) -> pd.DataFrame:
        """Fetch all series in dataset.cfg. year is ignored - FRED data is not year-partitioned"""
        frames = [self._fetch_series(series_id, meta, start, end, realtime) for series_id, meta in dataset_cfg["series"].items()]
        return pd.DataFrame(pd.concat(frames, ignore_index=True))


if __name__ == "__main__":
    from ingest.common.utils import _get_dataset, _load_config

    providers = _load_config("providers")
    dataset = _get_dataset("macro.rates")

    client = FREDClient(providers["fred"])
    df = client.fetch(dataset, realtime=True)
    print(df.shape)
    print(df.head())
    client.close()
