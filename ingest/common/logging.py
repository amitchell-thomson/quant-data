"""
Ingestion logging utilities.

Tracks all ingestion runs in _meta/ingestion-log.parquet for:
- Reproducibility
- Debugging
- Monitoring data freshness
- Auditing
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .io_parquet import read_parquet, write_parquet
from .schema import INGESTION_LOG_SCHEMA, INGESTION_LOG_PRIMARY_KEY, INGESTION_LOG_REQUIRED_COLS


class IngestionLogger:
    """Logger for tracking ingestion runs."""
    
    def __init__(self, log_path: Path | str):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.run_id = str(uuid.uuid4())
        self.start_time = datetime.now()
    
    def log_run(
        self,
        dataset: str,
        ticker: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        status: str,
        rows_ingested: int,
        rows_existing: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Log a single ingestion run.
        
        Args:
            dataset: Dataset name (e.g., "ohlcv", "dividends")
            ticker: Ticker symbol (None for universe-level datasets)
            start_date: Query start date (YYYY-MM-DD)
            end_date: Query end date (YYYY-MM-DD)
            status: "success", "partial", or "failed"
            rows_ingested: Number of rows added
            rows_existing: Number of rows already present (skipped)
            error_message: Error details if failed
        """
        duration = (datetime.now() - self.start_time).total_seconds()
        
        log_entry = pd.DataFrame([{
            "run_id": self.run_id,
            "dataset": dataset,
            "ticker": ticker,
            "start_date": pd.to_datetime(start_date) if start_date else None,
            "end_date": pd.to_datetime(end_date) if end_date else None,
            "status": status,
            "rows_ingested": rows_ingested,
            "rows_existing": rows_existing,
            "error_message": error_message,
            "duration_seconds": duration,
            "timestamp": datetime.now(),
        }])
        
        # Append to existing log
        existing_log = read_parquet(self.log_path)
        
        if existing_log.empty:
            combined = log_entry
        else:
            combined = pd.concat([existing_log, log_entry], ignore_index=True)
        
        # Write back (no schema validation to allow flexibility)
        write_parquet(
            combined,
            self.log_path,
            INGESTION_LOG_SCHEMA,
            INGESTION_LOG_REQUIRED_COLS,
            INGESTION_LOG_PRIMARY_KEY,
            "ingestion_log",
            validate=True,
        )
    
    def log_batch(
        self,
        dataset: str,
        results: list[dict],
    ) -> None:
        """
        Log a batch of ingestion results.
        
        Args:
            dataset: Dataset name
            results: List of dicts with keys:
                - ticker
                - start_date
                - end_date
                - status
                - rows_ingested
                - rows_existing
                - error_message (optional)
        """
        duration = (datetime.now() - self.start_time).total_seconds()
        
        log_entries = []
        for result in results:
            log_entries.append({
                "run_id": self.run_id,
                "dataset": dataset,
                "ticker": result.get("ticker"),
                "start_date": pd.to_datetime(result.get("start_date")) if result.get("start_date") else None,
                "end_date": pd.to_datetime(result.get("end_date")) if result.get("end_date") else None,
                "status": result.get("status", "unknown"),
                "rows_ingested": result.get("rows_ingested", 0),
                "rows_existing": result.get("rows_existing", 0),
                "error_message": result.get("error_message"),
                "duration_seconds": duration / len(results),  # Approximate
                "timestamp": datetime.now(),
            })
        
        log_df = pd.DataFrame(log_entries)
        
        # Append to existing log
        existing_log = read_parquet(self.log_path)
        
        if existing_log.empty:
            combined = log_df
        else:
            combined = pd.concat([existing_log, log_df], ignore_index=True)
        
        # Write back
        write_parquet(
            combined,
            self.log_path,
            INGESTION_LOG_SCHEMA,
            INGESTION_LOG_REQUIRED_COLS,
            INGESTION_LOG_PRIMARY_KEY,
            "ingestion_log",
            validate=True,
        )
    
    def get_last_run(
        self,
        dataset: str,
        ticker: Optional[str] = None,
    ) -> Optional[pd.Series]:
        """
        Get the most recent successful run for a dataset/ticker.
        
        Args:
            dataset: Dataset name
            ticker: Optional ticker filter
            
        Returns:
            Series with log entry, or None if not found
        """
        log = read_parquet(self.log_path)
        
        if log.empty:
            return None
        
        # Filter
        mask = (log["dataset"] == dataset) & (log["status"] == "success")
        if ticker:
            mask &= (log["ticker"] == ticker)
        
        filtered = log[mask]
        
        if filtered.empty:
            return None
        
        # Return most recent
        return filtered.sort_values("timestamp", ascending=False).iloc[0]


def update_ticker_mapping(
    mapping_path: Path | str,
    tickers: list[str],
    exchange_map: Optional[dict[str, str]] = None,
) -> None:
    """
    Update the ticker mapping file with newly seen tickers.
    
    Args:
        mapping_path: Path to ticker_mapping.parquet
        tickers: List of tickers to add/update
        exchange_map: Optional dict mapping ticker -> exchange
    """
    from .io_parquet import normalize_ticker, is_delisted
    from .schema import TICKER_MAPPING_SCHEMA, TICKER_MAPPING_PRIMARY_KEY, TICKER_MAPPING_REQUIRED_COLS
    
    mapping_path = Path(mapping_path)
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read existing mapping
    existing = read_parquet(mapping_path)
    
    # Create new entries
    new_entries = []
    for ticker in tickers:
        normalized = normalize_ticker(ticker, strip_old=True)
        delisted = is_delisted(ticker)
        exchange = exchange_map.get(ticker) if exchange_map else None
        
        new_entries.append({
            "ticker": ticker,
            "ticker_normalized": normalized,
            "exchange": exchange,
            "is_delisted": delisted,
            "delisted_date": None,  # Could be populated from data
            "last_seen": datetime.now().date(),
        })
    
    new_df = pd.DataFrame(new_entries)
    
    if existing.empty:
        combined = new_df
    else:
        # Merge, keeping the latest last_seen date
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.sort_values("last_seen", ascending=False)
        combined = combined.drop_duplicates(subset=["ticker"], keep="first")
    
    # Write back
    write_parquet(
        combined,
        mapping_path,
        TICKER_MAPPING_SCHEMA,
        TICKER_MAPPING_REQUIRED_COLS,
        TICKER_MAPPING_PRIMARY_KEY,
        "ticker_mapping",
        validate=True,
    )


def print_ingestion_summary(
    dataset: str,
    total_tickers: int,
    successful: int,
    failed: int,
    total_rows: int,
    duration_seconds: float,
) -> None:
    """
    Print a summary of an ingestion run.
    
    Args:
        dataset: Dataset name
        total_tickers: Total tickers processed
        successful: Number of successful tickers
        failed: Number of failed tickers
        total_rows: Total rows ingested
        duration_seconds: Total duration
    """
    print("\n" + "=" * 60)
    print(f"Ingestion Summary: {dataset}")
    print("=" * 60)
    print(f"  Total tickers: {total_tickers}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total rows ingested: {total_rows:,}")
    print(f"  Duration: {duration_seconds:.2f} seconds")
    if total_tickers > 0:
        print(f"  Average: {duration_seconds / total_tickers:.2f} sec/ticker")
    print("=" * 60 + "\n")
