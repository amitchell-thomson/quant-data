"""
Daily OHLCV Data Ingestion

Downloads daily OHLCV data for all tickers in the S&P 500 membership.
Partitioned by exchange for efficient storage and retrieval.

Survivorship-bias-safe: ingests data for all tickers (including delisted)
that appear in the membership history.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from tqdm import tqdm

from .common.eodhd_client import EODHDClient
from .common.io_parquet import normalize_ticker, read_parquet, upsert_by_partition
from .common.logging import IngestionLogger, print_ingestion_summary
from .common.schema import OHLCV_SCHEMA, OHLCV_PRIMARY_KEY, OHLCV_REQUIRED_COLS


def parse_ohlcv_data(
    data: list[dict],
    ticker: str,
    exchange: Optional[str] = None,
) -> pd.DataFrame:
    """
    Parse OHLCV data from EODHD response.
    
    Args:
        data: List of daily OHLCV records
        ticker: Ticker symbol
        exchange: Exchange (inferred if not provided)
        
    Returns:
        DataFrame with OHLCV data
    """
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Standardize column names
    column_mapping = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "adjusted_close": "adjusted_close",
        "volume": "volume",
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Add ticker and exchange
    df["ticker"] = ticker
    
    if exchange:
        df["exchange"] = exchange
    elif "." in ticker:
        # Try to infer from ticker suffix
        # For now, leave as None; will be filled by exchange mapping
        df["exchange"] = None
    else:
        df["exchange"] = None
    
    # Convert date to datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    
    # Convert numeric columns
    numeric_cols = ["open", "high", "low", "close", "adjusted_close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Filter out rows with missing required fields
    required = ["date", "open", "high", "low", "close", "volume"]
    df = df.dropna(subset=required)
    
    return df


def infer_exchange(ticker: str, exchange_map: dict[str, str]) -> Optional[str]:
    """
    Infer exchange from ticker or mapping.
    
    Args:
        ticker: Ticker symbol
        exchange_map: Dict mapping ticker -> exchange
        
    Returns:
        Exchange code or None
    """
    # Check mapping first
    if ticker in exchange_map:
        return exchange_map[ticker]
    
    # Try normalized ticker (strip .US_old)
    normalized = normalize_ticker(ticker)
    if normalized in exchange_map:
        return exchange_map[normalized]
    
    # Default to NYSE for US tickers (conservative guess)
    return "NYSE"


def fetch_ohlcv_for_ticker(
    client: EODHDClient,
    ticker: str,
    start_date: Optional[str],
    end_date: Optional[str],
    exchange: Optional[str],
) -> dict:
    """
    Fetch OHLCV data for a single ticker.
    
    Args:
        client: EODHD client
        ticker: Ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        exchange: Exchange code
        
    Returns:
        Dict with status, data, and metadata
    """
    try:
        data = client.get_eod_data(ticker, from_date=start_date, to_date=end_date)
        df = parse_ohlcv_data(data, ticker, exchange)
        
        if df.empty:
            return {
                "ticker": ticker,
                "status": "no_data",
                "rows_ingested": 0,
                "error_message": "No data returned",
            }
        
        return {
            "ticker": ticker,
            "status": "success",
            "data": df,
            "rows_ingested": len(df),
            "start_date": start_date,
            "end_date": end_date,
        }
    
    except Exception as e:
        return {
            "ticker": ticker,
            "status": "failed",
            "rows_ingested": 0,
            "error_message": str(e),
        }


def ingest_ohlcv(
    config_path: Path | str,
    api_key: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tickers: Optional[list[str]] = None,
    max_workers: int = 5,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Ingest daily OHLCV data for S&P 500 constituents.
    
    Args:
        config_path: Path to ingest.yaml config file
        api_key: EODHD API key
        start_date: Start date (YYYY-MM-DD), defaults to config
        end_date: End date (YYYY-MM-DD), defaults to today
        tickers: Optional list of specific tickers to ingest
        max_workers: Number of parallel workers
        dry_run: If True, don't write data
        force: If True, re-download even if data exists
    """
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    data_root = Path(config["data_root"])
    membership_file = data_root / config["universe"]["sp500"]["membership_file"]
    ohlcv_root = data_root / config["datasets"]["ohlcv"]["output_path"]
    log_path = data_root / config["logging"]["ingestion_log_path"]
    
    # Date range
    if not start_date:
        start_date = config["date_ranges"]["default_start"]
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'=' * 60}")
    print("OHLCV Data Ingestion")
    print(f"{'=' * 60}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Output: {ohlcv_root}")
    print(f"Workers: {max_workers}")
    print(f"Dry run: {dry_run}")
    print(f"{'=' * 60}\n")
    
    # Load membership to get ticker list
    if tickers is None:
        print("Loading S&P 500 membership...")
        membership_df = read_parquet(membership_file)
        
        if membership_df.empty:
            print("Error: No membership data found. Run membership ingestion first.")
            sys.exit(1)
        
        tickers = membership_df["ticker"].unique().tolist()
        exchange_map = dict(zip(membership_df["ticker"], membership_df["exchange"]))
        print(f"Found {len(tickers)} unique tickers in membership")
    else:
        exchange_map = {}
    
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
    
    # Fetch data in parallel
    print(f"\nFetching OHLCV data for {len(tickers)} tickers...")
    
    results = []
    successful = 0
    failed = 0
    total_rows = 0
    
    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                fetch_ohlcv_for_ticker,
                client,
                ticker,
                start_date,
                end_date,
                infer_exchange(ticker, exchange_map),
            ): ticker
            for ticker in tickers
        }
        
        # Process results with progress bar
        with tqdm(total=len(tickers), desc="Downloading", unit="ticker") as pbar:
            for future in as_completed(futures):
                ticker = futures[future]
                result = future.result()
                results.append(result)
                
                if result["status"] == "success":
                    successful += 1
                    total_rows += result["rows_ingested"]
                elif result["status"] == "failed":
                    failed += 1
                    if config["logging"]["log_level"] == "DEBUG":
                        print(f"\n  Failed: {ticker} - {result['error_message']}")
                
                pbar.update(1)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Combine all data
    if not dry_run and results:
        print("\nCombining and writing data...")
        
        all_data = []
        for result in results:
            if result["status"] == "success" and "data" in result:
                all_data.append(result["data"])
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Fill missing exchanges
            if combined_df["exchange"].isna().any():
                print("  Filling missing exchanges...")
                for idx, row in combined_df[combined_df["exchange"].isna()].iterrows():
                    combined_df.at[idx, "exchange"] = infer_exchange(row["ticker"], exchange_map)
            
            print(f"  Total rows: {len(combined_df):,}")
            
            # Upsert by partition (exchange)
            partition_results = upsert_by_partition(
                combined_df,
                ohlcv_root,
                "exchange",
                OHLCV_SCHEMA,
                OHLCV_REQUIRED_COLS,
                OHLCV_PRIMARY_KEY,
                "ohlcv",
            )
            
            print("\nPartition results:")
            for partition, (rows_new, rows_updated) in partition_results.items():
                print(f"  {partition}: {rows_new} new, {rows_updated} updated")
            
            # Log results
            logger.log_batch("ohlcv", results)
            
            print("\n✓ OHLCV ingestion complete")
        else:
            print("\nNo data to write.")
    
    # Print summary
    print_ingestion_summary(
        dataset="ohlcv",
        total_tickers=len(tickers),
        successful=successful,
        failed=failed,
        total_rows=total_rows,
        duration_seconds=duration,
    )
    
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
    parser = argparse.ArgumentParser(description="Ingest OHLCV data")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to ingest")
    parser.add_argument("--max-workers", type=int, default=5, help="Parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    
    args = parser.parse_args()
    
    ingest_ohlcv(
        config_path=config_path,
        api_key=api_key,
        start_date=args.start,
        end_date=args.end,
        tickers=args.tickers,
        max_workers=args.max_workers,
        dry_run=args.dry_run,
        force=args.force,
    )
