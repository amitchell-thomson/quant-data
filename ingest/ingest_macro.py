"""
Macroeconomic Data Ingestion

Downloads macro time series (interest rates, inflation, GDP, etc.)
from the Federal Reserve Economic Data (FRED) API.
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
from .common.schema import MACRO_SCHEMA, MACRO_PRIMARY_KEY, MACRO_REQUIRED_COLS


def parse_fred_data(
    observations: list[dict],
    series_code: str,
    series_name: str,
    category: str,
) -> pd.DataFrame:
    """
    Parse FRED observations into standardized format.
    
    Args:
        observations: List of FRED observations
        series_code: FRED series ID (e.g., "DGS10")
        series_name: Human-readable name
        category: Category (e.g., "rates", "inflation")
        
    Returns:
        DataFrame with macro data
        
    Note:
        FRED returns observations with "date" and "value" fields.
        Value may be "." for missing data, which we convert to NaN.
    """
    if not observations:
        return pd.DataFrame()
    
    df = pd.DataFrame(observations)
    
    # Extract only date and value columns
    df = df[["date", "value"]].copy()
    
    # Add metadata
    df["series_code"] = series_code
    df["series_name"] = series_name
    df["category"] = category
    
    # Convert types
    df["date"] = pd.to_datetime(df["date"])
    
    # FRED uses "." for missing values; convert to numeric (becomes NaN)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    # Drop rows with missing values
    df = df.dropna(subset=["date", "value"])  # type: ignore
    
    return df


def ingest_macro(
    config_path: Path | str,
    api_key: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    series: Optional[list[str]] = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Ingest macroeconomic indicators from FRED.
    
    Args:
        config_path: Path to ingest.yaml config file
        api_key: FRED API key
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        series: Optional list of series codes to ingest
        dry_run: If True, don't write data
        force: If True, re-download even if data exists
    """
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    data_root = Path(config["data_root"])
    macro_root = data_root / "macro"
    log_path = data_root / config["logging"]["ingestion_log_path"]
    
    # Get series to ingest
    all_series = config["datasets"]["macro"]["series"]
    
    if series:
        # Filter to requested series
        all_series = [s for s in all_series if s["code"] in series]
    
    if not all_series:
        print("Error: No macro series configured or specified")
        sys.exit(1)
    
    # Date range
    if not start_date:
        start_date = config["date_ranges"]["default_start"]
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'=' * 60}")
    print("Macroeconomic Data Ingestion")
    print(f"{'=' * 60}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Series: {len(all_series)}")
    print(f"Dry run: {dry_run}")
    print(f"{'=' * 60}\n")
    
    # Initialize logger
    logger = IngestionLogger(log_path)
    
    # Create FRED client
    fred_config = config["api"]["fred"]
    client = FREDClient(
        api_key=api_key,
        base_url=fred_config["base_url"],
        timeout=fred_config["timeout"],
        max_retries=fred_config["max_retries"],
        retry_backoff_factor=fred_config["retry_backoff_factor"],
        rate_limit_rpm=fred_config["rate_limit"]["requests_per_minute"],
    )
    
    # Fetch each series
    all_data = []
    successful = 0
    failed = 0
    
    for series_info in all_series:
        series_code = series_info["code"]
        series_name = series_info["name"]
        category = series_info["category"]
        
        print(f"\nFetching {series_name} ({series_code})...")
        
        try:
            # Fetch data from FRED
            observations = client.get_series_observations(
                series_id=series_code,
                observation_start=start_date,
                observation_end=end_date,
            )
            
            df = parse_fred_data(observations, series_code, series_name, category)
            
            if df.empty:
                print(f"  No data found")
                failed += 1
                continue
            
            print(f"  Downloaded {len(df)} data points")
            all_data.append(df)
            successful += 1
            
        except Exception as e:
            print(f"  Error: {e}")
            failed += 1
    
    # Write data
    if not dry_run and all_data:
        print("\nWriting macro data...")
        
        combined = pd.concat(all_data, ignore_index=True)
        print(f"  Total rows: {len(combined):,}")
        
        # Group by category and write separate files
        for category in combined["category"].unique():
            category_df = combined[combined["category"] == category].copy()
            assert isinstance(category_df, pd.DataFrame)  # Type narrowing for linter
            category_path = macro_root / category
            category_file = category_path / f"{category}.parquet"
            
            rows_new, rows_updated = upsert_parquet(
                category_df,
                category_file,
                MACRO_SCHEMA,
                MACRO_REQUIRED_COLS,
                MACRO_PRIMARY_KEY,
                f"macro_{category}",
            )
            
            print(f"  {category}: {rows_new} new, {rows_updated} updated")
        
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
        logger.log_batch("macro", log_results)
        
        print("\n✓ Macro data ingestion complete")
    
    elif not all_data:
        print("\nNo data to write.")
    
    # Summary
    print(f"\nSuccessful: {successful}")
    print(f"Failed: {failed}")
    
    client.close()


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("Error: FRED_API_KEY not found in environment")
        print("Get a free API key at https://fred.stlouisfed.org/docs/api/api_key.html")
        sys.exit(1)
    
    config_path = Path(__file__).parent.parent / "config" / "ingest.yaml"
    
    # Parse CLI args
    import argparse
    parser = argparse.ArgumentParser(description="Ingest macro indicators from FRED")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--series", nargs="+", help="Specific FRED series IDs")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    
    args = parser.parse_args()
    
    ingest_macro(
        config_path=config_path,
        api_key=api_key,
        start_date=args.start,
        end_date=args.end,
        series=args.series,
        dry_run=args.dry_run,
        force=args.force,
    )
