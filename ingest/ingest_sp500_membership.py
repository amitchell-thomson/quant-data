"""
S&P 500 Index Membership Ingestion

Downloads the current and historical constituents of the S&P 500.
This is the authoritative source for determining which tickers to ingest.

Note: EODHD's historical constituents API may have limitations. For full
historical accuracy, consider manually maintaining a membership CSV with
start/end dates for each ticker.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from .common.eodhd_client import EODHDClient
from .common.io_parquet import upsert_parquet
from .common.logging import IngestionLogger, update_ticker_mapping
from .common.schema import (
    MEMBERSHIP_SCHEMA,
    MEMBERSHIP_PRIMARY_KEY,
    MEMBERSHIP_REQUIRED_COLS,
)


def parse_sp500_constituents(
    data: dict,
    index_name: str = "SP500",
) -> pd.DataFrame:
    """
    Parse S&P 500 constituents from EODHD fundamentals response.
    
    Args:
        data: Response from get_index_constituents()
        index_name: Name of the index
        
    Returns:
        DataFrame with membership records
    """
    # EODHD returns constituents under "Components" key
    components = data.get("Components", {})
    
    if not components:
        print("  Warning: No components found in index data")
        return pd.DataFrame()
    
    records = []
    for key, info in components.items():
        # Extract ticker code from the info dict
        ticker_code = info.get("Code")
        if not ticker_code:
            continue  # Skip if no ticker code
        
        # Add exchange suffix if not present
        ticker = ticker_code if "." in ticker_code else f"{ticker_code}.US"
        
        records.append({
            "ticker": ticker,
            "index_name": index_name,
            "start_date": None,  # EODHD doesn't provide historical start date
            "end_date": None,    # Still in index
            "company_name": info.get("Name"),
            "exchange": info.get("Exchange"),
            "sector": info.get("Sector"),
            "industry": info.get("Industry"),
        })
    
    df = pd.DataFrame(records)
    
    # Set start_date to today if not available (current membership snapshot)
    if "start_date" not in df.columns or df["start_date"].isna().all():
        df["start_date"] = datetime.now().date()
    
    return df


def ingest_sp500_membership(
    config_path: Path | str,
    api_key: str,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Ingest S&P 500 index membership.
    
    Args:
        config_path: Path to ingest.yaml config file
        api_key: EODHD API key
        dry_run: If True, don't write data
        force: If True, re-download even if data exists
    """
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    data_root = Path(config["data_root"])
    membership_file = data_root / config["universe"]["sp500"]["membership_file"]
    index_code = config["universe"]["sp500"]["index_code"]
    log_path = data_root / config["logging"]["ingestion_log_path"]
    ticker_mapping_path = data_root / config["logging"]["ticker_mapping_path"]
    
    print(f"\n{'=' * 60}")
    print("S&P 500 Membership Ingestion")
    print(f"{'=' * 60}")
    print(f"Index code: {index_code}")
    print(f"Output: {membership_file}")
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
    
    try:
        # Fetch current constituents
        print("Fetching current S&P 500 constituents...")
        index_data = client.get_index_constituents(index_code)
        
        # Parse into DataFrame
        df = parse_sp500_constituents(index_data, index_name="SP500")
        
        if df.empty:
            print("No constituents found. Exiting.")
            logger.log_run(
                dataset="membership",
                ticker=None,
                start_date=None,
                end_date=None,
                status="failed",
                rows_ingested=0,
                error_message="No constituents found",
            )
            return
        
        print(f"Found {len(df)} current constituents")
        print(f"\nSample tickers: {', '.join(df['ticker'].head(10).tolist())}")
        
        # Show sector breakdown
        if "sector" in df.columns and df["sector"].notna().any():
            print("\nSector breakdown:")
            sector_counts = df["sector"].value_counts()
            for sector, count in sector_counts.head(10).items():
                print(f"  {sector}: {count}")
        
        if dry_run:
            print("\n[DRY RUN] Would write data to:")
            print(f"  {membership_file}")
            print(f"  Rows: {len(df)}")
            return
        
        # Upsert to Parquet
        print(f"\nWriting to {membership_file}...")
        rows_new, rows_updated = upsert_parquet(
            df,
            membership_file,
            MEMBERSHIP_SCHEMA,
            MEMBERSHIP_REQUIRED_COLS,
            MEMBERSHIP_PRIMARY_KEY,
            "sp500_membership",
        )
        
        print(f"  New rows: {rows_new}")
        print(f"  Updated rows: {rows_updated}")
        
        # Update ticker mapping
        print("\nUpdating ticker mapping...")
        exchange_map = dict(zip(df["ticker"], df["exchange"]))
        update_ticker_mapping(ticker_mapping_path, df["ticker"].tolist(), exchange_map)
        
        # Log success
        logger.log_run(
            dataset="membership",
            ticker=None,
            start_date=None,
            end_date=None,
            status="success",
            rows_ingested=rows_new,
            rows_existing=rows_updated,
        )
        
        print("\n✓ S&P 500 membership ingestion complete")
        
    except Exception as e:
        print(f"\n✗ Error during ingestion: {e}")
        logger.log_run(
            dataset="membership",
            ticker=None,
            start_date=None,
            end_date=None,
            status="failed",
            rows_ingested=0,
            error_message=str(e),
        )
        raise
    
    finally:
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
    
    ingest_sp500_membership(
        config_path=config_path,
        api_key=api_key,
        dry_run="--dry-run" in sys.argv,
        force="--force" in sys.argv,
    )
