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
    
    Uses the HistoricalTickerComponents field to capture full membership history,
    including delisted and removed companies for survivorship-bias-free analysis.
    
    Args:
        data: Response from get_index_constituents()
        index_name: Name of the index
        
    Returns:
        DataFrame with membership records including historical constituents
    """
    # Prefer HistoricalTickerComponents for full historical membership
    historical_components = data.get("HistoricalTickerComponents", {})
    
    if historical_components:
        print(f"  Using HistoricalTickerComponents ({len(historical_components)} records)")
        records = []
        
        for key, info in historical_components.items():
            # Extract ticker code from the info dict
            ticker_code = info.get("Code")
            if not ticker_code:
                continue  # Skip if no ticker code
            
            # Add exchange suffix if not present
            ticker = ticker_code if "." in ticker_code else f"{ticker_code}.US"
            
            # Parse dates
            start_date = info.get("StartDate")  # e.g., "2000-06-05"
            end_date = info.get("EndDate")      # null if still active
            
            # Convert to datetime objects
            start_date = pd.to_datetime(start_date).date() if start_date else None
            end_date = pd.to_datetime(end_date).date() if end_date else None
            
            # Parse delisting status
            is_delisted = bool(info.get("IsDelisted", 0))
            
            records.append({
                "ticker": ticker,
                "index_name": index_name,
                "start_date": start_date,
                "end_date": end_date,
                "is_delisted": is_delisted,
                "company_name": info.get("Name"),
                "exchange": "US",  # Historical data doesn't include exchange details
                "sector": None,    # Not available in historical data
                "industry": None,  # Not available in historical data
            })
        
        df = pd.DataFrame(records)
        
        # Enrich with sector/industry from current Components if available
        current_components = data.get("Components", {})
        if current_components:
            # Create a mapping of ticker -> sector/industry
            enrichment = {}
            for key, info in current_components.items():
                ticker_code = info.get("Code")
                if ticker_code:
                    ticker = ticker_code if "." in ticker_code else f"{ticker_code}.US"
                    enrichment[ticker] = {
                        "sector": info.get("Sector"),
                        "industry": info.get("Industry"),
                        "exchange": info.get("Exchange", "US"),
                    }
            
            # Apply enrichment to active members
            for idx, row in df.iterrows():
                if row["ticker"] in enrichment and row["end_date"] is None:
                    df.at[idx, "sector"] = enrichment[row["ticker"]]["sector"]
                    df.at[idx, "industry"] = enrichment[row["ticker"]]["industry"]
                    df.at[idx, "exchange"] = enrichment[row["ticker"]]["exchange"]
        
        return df
    
    else:
        # Fallback to current Components only (legacy behavior)
        print("  Warning: HistoricalTickerComponents not found, using current Components only")
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
                "start_date": datetime.now().date(),  # Snapshot date
                "end_date": None,                     # Still in index
                "is_delisted": False,                 # Current members are not delisted
                "company_name": info.get("Name"),
                "exchange": info.get("Exchange", "US"),
                "sector": info.get("Sector"),
                "industry": info.get("Industry"),
            })
        
        df = pd.DataFrame(records)
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
        
        # Calculate statistics
        total_constituents = len(df)
        current_members = df["end_date"].isna().sum()
        removed_members = df["end_date"].notna().sum()
        
        # Breakdown of removed members
        if "is_delisted" in df.columns and removed_members > 0:
            removed_df = df[df["end_date"].notna()]
            delisted_count = removed_df["is_delisted"].sum()
            still_trading_count = (~removed_df["is_delisted"]).sum()
        else:
            delisted_count = 0
            still_trading_count = 0
        
        print(f"Found {total_constituents} total constituents:")
        print(f"  Current members: {current_members}")
        print(f"  Historical (removed): {removed_members}")
        if removed_members > 0:
            print(f"    - Delisted: {delisted_count}")
            print(f"    - Still trading: {still_trading_count}")
        print(f"\nSample tickers: {', '.join(df['ticker'].head(10).tolist())}")
        
        # Show sector breakdown for current members
        if "sector" in df.columns and df["sector"].notna().any():
            current_df = df[df["end_date"].isna()]
            print("\nSector breakdown (current members):")
            sector_counts = current_df["sector"].value_counts()
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
