"""
Corporate Actions Ingestion (Dividends & Splits)

Downloads dividend and stock split history for all S&P 500 constituents.
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
from .common.io_parquet import read_parquet, upsert_parquet
from .common.logging import IngestionLogger, print_ingestion_summary
from .common.schema import (
    DIVIDENDS_SCHEMA,
    DIVIDENDS_PRIMARY_KEY,
    DIVIDENDS_REQUIRED_COLS,
    SPLITS_SCHEMA,
    SPLITS_PRIMARY_KEY,
    SPLITS_REQUIRED_COLS,
)


def parse_dividends(data: list[dict], ticker: str) -> pd.DataFrame:
    """
    Parse dividend data from EODHD response.
    
    Args:
        data: List of dividend records
        ticker: Ticker symbol
        
    Returns:
        DataFrame with dividend data
    """
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df["ticker"] = ticker
    
    # Standardize column names
    column_mapping = {
        "date": "ex_date",  # EODHD uses "date" for ex-dividend date
        "value": "amount",
        "paymentDate": "payment_date",
        "recordDate": "record_date",
        "declarationDate": "declared_date",
        "currency": "currency",
        "unadjustedValue": "amount",  # Use unadjusted if available
    }
    
    df = df.rename(columns=column_mapping)
    
    # Convert dates
    date_cols = ["ex_date", "payment_date", "record_date", "declared_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # Convert amount to float
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    
    # Add dividend type (default to Cash)
    if "dividend_type" not in df.columns:
        df["dividend_type"] = "Cash"
    
    # Filter out invalid records
    df = df.dropna(subset=["ex_date", "amount"])
    
    return df


def parse_splits(data: list[dict], ticker: str) -> pd.DataFrame:
    """
    Parse stock split data from EODHD response.
    
    Args:
        data: List of split records
        ticker: Ticker symbol
        
    Returns:
        DataFrame with split data
    """
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df["ticker"] = ticker
    
    # EODHD provides split as a string like "2/1" or numeric factor
    # Standardize column names
    column_mapping = {
        "date": "date",
        "split": "split_ratio",
    }
    
    df = df.rename(columns=column_mapping)
    
    # Convert date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    # Parse split ratio and calculate factor
    def parse_split_ratio(ratio_str):
        """Convert '2/1' -> (2.0, '2/1')"""
        if pd.isna(ratio_str):
            return None, None
        
        ratio_str = str(ratio_str).strip()
        
        # Already a numeric factor (e.g., "2.0")
        try:
            factor = float(ratio_str)
            # Infer ratio string
            if factor > 1:
                ratio = f"{int(factor)}/1"
            else:
                ratio = f"1/{int(1/factor)}"
            return factor, ratio
        except ValueError:
            pass
        
        # Parse fraction (e.g., "2/1", "3/2")
        if "/" in ratio_str:
            try:
                numerator, denominator = ratio_str.split("/")
                factor = float(numerator) / float(denominator)
                return factor, ratio_str
            except:
                return None, ratio_str
        
        return None, ratio_str
    
    if "split_ratio" in df.columns:
        parsed = df["split_ratio"].apply(parse_split_ratio)
        df["split_factor"] = parsed.apply(lambda x: x[0] if x else None)
        df["split_ratio"] = parsed.apply(lambda x: x[1] if x else None)
    
    # Filter out invalid records
    df = df.dropna(subset=["date", "split_ratio", "split_factor"])
    
    return df


def fetch_dividends_for_ticker(
    client: EODHDClient,
    ticker: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> dict:
    """Fetch dividend data for a single ticker."""
    try:
        data = client.get_dividends(ticker, from_date=start_date, to_date=end_date)
        df = parse_dividends(data, ticker)
        
        return {
            "ticker": ticker,
            "status": "success" if not df.empty else "no_data",
            "data": df,
            "rows_ingested": len(df),
        }
    except Exception as e:
        return {
            "ticker": ticker,
            "status": "failed",
            "rows_ingested": 0,
            "error_message": str(e),
        }


def fetch_splits_for_ticker(
    client: EODHDClient,
    ticker: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> dict:
    """Fetch split data for a single ticker."""
    try:
        data = client.get_splits(ticker, from_date=start_date, to_date=end_date)
        df = parse_splits(data, ticker)
        
        return {
            "ticker": ticker,
            "status": "success" if not df.empty else "no_data",
            "data": df,
            "rows_ingested": len(df),
        }
    except Exception as e:
        return {
            "ticker": ticker,
            "status": "failed",
            "rows_ingested": 0,
            "error_message": str(e),
        }


def ingest_corporate_actions(
    config_path: Path | str,
    api_key: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tickers: Optional[list[str]] = None,
    max_workers: int = 5,
    dry_run: bool = False,
    force: bool = False,
    dividends_only: bool = False,
    splits_only: bool = False,
) -> None:
    """
    Ingest corporate actions (dividends and splits).
    
    Args:
        config_path: Path to ingest.yaml config file
        api_key: EODHD API key
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        tickers: Optional list of specific tickers
        max_workers: Number of parallel workers
        dry_run: If True, don't write data
        force: If True, re-download even if data exists
        dividends_only: Only ingest dividends
        splits_only: Only ingest splits
    """
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    data_root = Path(config["data_root"])
    membership_file = data_root / config["universe"]["sp500"]["membership_file"]
    dividends_file = data_root / config["datasets"]["corporate_actions"]["dividends_path"]
    splits_file = data_root / config["datasets"]["corporate_actions"]["splits_path"]
    log_path = data_root / config["logging"]["ingestion_log_path"]
    
    # Date range
    if not start_date:
        start_date = config["date_ranges"]["default_start"]
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'=' * 60}")
    print("Corporate Actions Ingestion")
    print(f"{'=' * 60}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Dividends: {not splits_only}")
    print(f"Splits: {not dividends_only}")
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
        print(f"Found {len(tickers)} unique tickers in membership")
    
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
    
    start_time = datetime.now()
    
    # =========================================================================
    # Dividends
    # =========================================================================
    
    if not splits_only:
        print(f"\nFetching dividends for {len(tickers)} tickers...")
        
        div_results = []
        div_successful = 0
        div_failed = 0
        div_total_rows = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    fetch_dividends_for_ticker,
                    client,
                    ticker,
                    start_date,
                    end_date,
                ): ticker
                for ticker in tickers
            }
            
            with tqdm(total=len(tickers), desc="Dividends", unit="ticker") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    div_results.append(result)
                    
                    if result["status"] == "success":
                        div_successful += 1
                        div_total_rows += result["rows_ingested"]
                    elif result["status"] == "failed":
                        div_failed += 1
                    
                    pbar.update(1)
        
        # Write dividends
        if not dry_run and div_results:
            print("\nWriting dividend data...")
            
            all_divs = [r["data"] for r in div_results if r["status"] == "success" and "data" in r and not r["data"].empty]
            
            if all_divs:
                combined_divs = pd.concat(all_divs, ignore_index=True)
                print(f"  Total dividend rows: {len(combined_divs):,}")
                
                rows_new, rows_updated = upsert_parquet(
                    combined_divs,
                    dividends_file,
                    DIVIDENDS_SCHEMA,
                    DIVIDENDS_REQUIRED_COLS,
                    DIVIDENDS_PRIMARY_KEY,
                    "dividends",
                )
                
                print(f"  New: {rows_new}, Updated: {rows_updated}")
                
                # Log results
                logger.log_batch("dividends", div_results)
            else:
                print("  No dividend data to write.")
        
        print_ingestion_summary(
            dataset="dividends",
            total_tickers=len(tickers),
            successful=div_successful,
            failed=div_failed,
            total_rows=div_total_rows,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
        )
    
    # =========================================================================
    # Splits
    # =========================================================================
    
    if not dividends_only:
        print(f"\nFetching splits for {len(tickers)} tickers...")
        
        split_results = []
        split_successful = 0
        split_failed = 0
        split_total_rows = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    fetch_splits_for_ticker,
                    client,
                    ticker,
                    start_date,
                    end_date,
                ): ticker
                for ticker in tickers
            }
            
            with tqdm(total=len(tickers), desc="Splits", unit="ticker") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    split_results.append(result)
                    
                    if result["status"] == "success":
                        split_successful += 1
                        split_total_rows += result["rows_ingested"]
                    elif result["status"] == "failed":
                        split_failed += 1
                    
                    pbar.update(1)
        
        # Write splits
        if not dry_run and split_results:
            print("\nWriting split data...")
            
            all_splits = [r["data"] for r in split_results if r["status"] == "success" and "data" in r and not r["data"].empty]
            
            if all_splits:
                combined_splits = pd.concat(all_splits, ignore_index=True)
                print(f"  Total split rows: {len(combined_splits):,}")
                
                rows_new, rows_updated = upsert_parquet(
                    combined_splits,
                    splits_file,
                    SPLITS_SCHEMA,
                    SPLITS_REQUIRED_COLS,
                    SPLITS_PRIMARY_KEY,
                    "splits",
                )
                
                print(f"  New: {rows_new}, Updated: {rows_updated}")
                
                # Log results
                logger.log_batch("splits", split_results)
            else:
                print("  No split data to write.")
        
        print_ingestion_summary(
            dataset="splits",
            total_tickers=len(tickers),
            successful=split_successful,
            failed=split_failed,
            total_rows=split_total_rows,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
        )
    
    print("\n✓ Corporate actions ingestion complete")
    
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
    parser = argparse.ArgumentParser(description="Ingest corporate actions")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    parser.add_argument("--max-workers", type=int, default=5, help="Parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    parser.add_argument("--dividends-only", action="store_true", help="Only dividends")
    parser.add_argument("--splits-only", action="store_true", help="Only splits")
    
    args = parser.parse_args()
    
    ingest_corporate_actions(
        config_path=config_path,
        api_key=api_key,
        start_date=args.start,
        end_date=args.end,
        tickers=args.tickers,
        max_workers=args.max_workers,
        dry_run=args.dry_run,
        force=args.force,
        dividends_only=args.dividends_only,
        splits_only=args.splits_only,
    )
