"""
Macroeconomic Data Ingestion — ALFRED (Archival)

Downloads all historical vintages of macro time series from the ALFRED API
(Archival Federal Reserve Economic Data).  Unlike FRED, which returns the
most-current values, ALFRED captures every release and revision so you can
query data exactly as it existed on any past date — enabling point-in-time
(backtest-safe) analysis.

Output files mirror the standard macro layout but with an `_alfred` suffix:
    macro/rates/rates_alfred.parquet
    macro/inflation/inflation_alfred.parquet
    macro/growth/growth_alfred.parquet
    macro/employment/employment_alfred.parquet
    macro/liquidity/liquidity_alfred.parquet

Schema adds two real-time columns to the base macro schema:
    realtime_start  — first date this value was the latest available revision
    realtime_end    — last date this value was the latest available revision
                      (null/9999-12-31 for the currently-live vintage)
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from .common.fred_client import FREDClient
from .common.io_parquet import upsert_parquet
from .common.logging import IngestionLogger
from .common.schema import (
    MACRO_ALFRED_PRIMARY_KEY,
    MACRO_ALFRED_REQUIRED_COLS,
    MACRO_ALFRED_SCHEMA,
)

# ALFRED real-time start — fetch all vintages from the earliest possible date.
# realtime_end defaults to today inside FREDClient, covering all revisions to now.
_ALFRED_RT_START = "1776-07-04"


def parse_alfred_data(
    observations: list[dict],
    series_code: str,
    series_name: str,
    category: str,
) -> pd.DataFrame:
    """
    Parse ALFRED observations into standardized format.

    ALFRED observations contain four fields per row:
        date            — the date for which the value was measured
        value           — the data value ("." means missing)
        realtime_start  — first date this value was the latest revision
        realtime_end    — last date this value was the latest revision

    Args:
        observations: Raw ALFRED observation dicts from the API
        series_code: FRED series ID (e.g., "DGS10")
        series_name: Human-readable name (e.g., "us_10y_treasury")
        category: Macro category (e.g., "rates", "inflation")

    Returns:
        DataFrame conforming to MACRO_ALFRED_SCHEMA, or empty DataFrame if
        no valid observations are present.
    """
    if not observations:
        return pd.DataFrame()

    df = pd.DataFrame(observations)

    # Guard: ensure expected columns are present
    required = {"date", "value", "realtime_start", "realtime_end"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"ALFRED response for {series_code} missing columns: {missing}"
        )

    df = df[["date", "value", "realtime_start", "realtime_end"]].copy()

    # Attach metadata
    df["series_code"] = series_code
    df["series_name"]  = series_name
    df["category"]     = category

    # Convert types
    df["date"]           = pd.to_datetime(df["date"])
    df["realtime_start"] = pd.to_datetime(df["realtime_start"])

    # realtime_end of "9999-12-31" means the vintage is still current; store as NaT
    df["realtime_end"] = df["realtime_end"].replace("9999-12-31", None)
    df["realtime_end"] = pd.to_datetime(df["realtime_end"], errors="coerce")

    # FRED/ALFRED uses "." for missing values — coerce to NaN
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Drop rows where the essential values are absent
    df = df.dropna(subset=["date", "realtime_start", "value"])  # type: ignore

    return df


def ingest_macro_alfred(
    config_path: Path | str,
    api_key: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    series: Optional[list[str]] = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Ingest archival vintage data for macroeconomic indicators from ALFRED.

    Uses the same series list configured for the standard FRED macro pipeline
    but calls the ALFRED endpoint (realtime_start / realtime_end parameters)
    to retrieve the full revision history.

    Args:
        config_path: Path to ingest.yaml config file
        api_key: FRED/ALFRED API key (same key works for both)
        start_date: Earliest observation date to fetch (YYYY-MM-DD)
        end_date: Latest observation date to fetch (YYYY-MM-DD)
        series: Optional list of series codes to restrict ingestion
        dry_run: If True, fetch data but do not write to disk
        force: If True, re-download even if data already exists
    """
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    data_root  = Path(config["data_root"])
    macro_root = data_root / "macro"
    log_path   = data_root / config["logging"]["ingestion_log_path"]

    # Resolve series list
    all_series = config["datasets"]["macro"]["series"]
    if series:
        all_series = [s for s in all_series if s["code"] in series]

    if not all_series:
        print("Error: No macro series configured or specified")
        sys.exit(1)

    # Date range defaults
    if not start_date:
        start_date = config["date_ranges"]["default_start"]
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n{'=' * 60}")
    print("Macroeconomic Data Ingestion — ALFRED (Archival Vintages)")
    print(f"{'=' * 60}")
    print(f"Date range:  {start_date} to {end_date}")
    print(f"Series:      {len(all_series)}")
    print(f"Dry run:     {dry_run}")
    print(f"{'=' * 60}\n")

    logger = IngestionLogger(log_path)

    fred_config = config["api"]["fred"]
    client = FREDClient(
        api_key=api_key,
        base_url=fred_config["base_url"],
        timeout=fred_config["timeout"],
        max_retries=fred_config["max_retries"],
        retry_backoff_factor=fred_config["retry_backoff_factor"],
        rate_limit_rpm=fred_config["rate_limit"]["requests_per_minute"],
    )

    all_data: list[pd.DataFrame] = []
    successful = 0
    failed     = 0

    for series_info in all_series:
        series_code = series_info["code"]
        series_name = series_info["name"]
        category    = series_info["category"]

        print(f"\nFetching ALFRED vintages for {series_name} ({series_code})...")

        try:
            observations = client.get_alfred_observations(
                series_id=series_code,
                observation_start=start_date,
                observation_end=end_date,
                realtime_start=_ALFRED_RT_START,
                # realtime_end defaults to "9999-12-31" — FRED's sentinel for all vintages
            )

            df = parse_alfred_data(observations, series_code, series_name, category)

            if df.empty:
                print("  No data found")
                failed += 1
                continue

            print(f"  Downloaded {len(df):,} vintage observations")
            all_data.append(df)
            successful += 1

        except Exception as e:
            print(f"  Error: {e}")
            failed += 1

    # Write data
    if not dry_run and all_data:
        print("\nWriting ALFRED macro data...")

        combined = pd.concat(all_data, ignore_index=True)
        print(f"  Total rows: {len(combined):,}")

        # Group by category and write to {category}/{category}_alfred.parquet
        for cat in combined["category"].unique():
            cat_df   = combined[combined["category"] == cat].copy()
            assert isinstance(cat_df, pd.DataFrame)
            cat_path = macro_root / cat
            cat_file = cat_path / f"{cat}_alfred.parquet"

            rows_new, rows_updated = upsert_parquet(
                cat_df,
                cat_file,
                MACRO_ALFRED_SCHEMA,
                MACRO_ALFRED_REQUIRED_COLS,
                MACRO_ALFRED_PRIMARY_KEY,
                f"macro_alfred_{cat}",
            )

            print(f"  {cat}_alfred: {rows_new} new, {rows_updated} updated")

        # Log results
        log_results = [
            {
                "ticker": None,
                "status": "success",
                "rows_ingested": len(combined),
                "start_date": start_date,
                "end_date": end_date,
            }
        ]
        logger.log_batch("macro_alfred", log_results)

        print("\n✓ ALFRED macro data ingestion complete")

    elif not all_data:
        print("\nNo data to write.")

    print(f"\nSuccessful: {successful}")
    print(f"Failed:     {failed}")

    client.close()


if __name__ == "__main__":
    import os
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("Error: FRED_API_KEY not found in environment")
        print("Get a free API key at https://fred.stlouisfed.org/docs/api/api_key.html")
        sys.exit(1)

    config_path = Path(__file__).parent.parent / "config" / "ingest.yaml"

    parser = argparse.ArgumentParser(
        description="Ingest archival vintage macro data from ALFRED"
    )
    parser.add_argument("--start",    help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",      help="End date (YYYY-MM-DD)")
    parser.add_argument("--series",   nargs="+", help="Specific FRED series IDs")
    parser.add_argument("--dry-run",  action="store_true", help="Dry run")
    parser.add_argument("--force",    action="store_true", help="Force re-download")

    args = parser.parse_args()

    ingest_macro_alfred(
        config_path=config_path,
        api_key=api_key,
        start_date=args.start,
        end_date=args.end,
        series=args.series,
        dry_run=args.dry_run,
        force=args.force,
    )
