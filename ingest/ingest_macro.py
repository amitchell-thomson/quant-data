"""
Macroeconomic Data Ingestion

Downloads macro time series (interest rates, inflation, GDP, etc.)
from EODHD's macro indicators API.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from .common.eodhd_client import EODHDClient
from .common.io_parquet import upsert_parquet
from .common.logging import IngestionLogger
from .common.schema import MACRO_SCHEMA, MACRO_PRIMARY_KEY, MACRO_REQUIRED_COLS


def parse_macro_data(
    data: list[dict],
    series_code: str,
    series_name: str,
    category: str,
) -> pd.DataFrame:
    """
    Parse macro indicator data from EODHD response.
    
    Args:
        data: List of date/value pairs
        series_code: Indicator code (e.g., "DGS10")
        series_name: Human-readable name
        category: Category (e.g., "rates", "inflation")
        
    Returns:
        DataFrame with macro data
    """
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Standardize columns
    column_mapping = {
        "date": "date",
        "value": "value",
    }
    df = df.rename(columns=column_mapping)
    
    # Add metadata
    df["series_code"] = series_code
    df["series_name"] = series_name
    df["category"] = category
    
    # Convert types
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    # Drop rows with missing values
    df = df.dropna(subset=["date", "value"])
    
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
    Ingest macroeconomic indicators.
    
    Args:
        config_path: Path to ingest.yaml config file
        api_key: EODHD API key
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
    
    # Create client
    client = EODHDClient(
        api_key=api_key,
        base_url=config["api"]["base_url"],
        timeout=config["api"]["timeout"],
        max_retries=config["api"]["max_retries"],
        retry_backoff_factor=config["api"]["retry_backoff_factor"],
        rate_limit_rps=config["api"]["rate_limit"]["requests_per_second"],
        rate_limit_burst=config["api"]["rate_limit"]["burst_size"],
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
            data = client.get_macro_indicator(
                indicator_code=series_code,
                country="USA",
                from_date=start_date,
                to_date=end_date,
            )
            
            df = parse_macro_data(data, series_code, series_name, category)
            
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
    
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        print("Error: EODHD_API_KEY not found in environment")
        sys.exit(1)
    
    config_path = Path(__file__).parent.parent / "config" / "ingest.yaml"
    
    # Parse CLI args
    import argparse
    parser = argparse.ArgumentParser(description="Ingest macro indicators")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--series", nargs="+", help="Specific series codes")
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
