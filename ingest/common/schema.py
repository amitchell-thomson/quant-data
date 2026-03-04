"""
Parquet schema definitions for all datasets.

Ensures consistent data types, required columns, and primary keys.
Schema versioning allows for evolution while maintaining compatibility.
"""

from datetime import datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Schema version for tracking evolution
SCHEMA_VERSION = "1.0.0"


# =============================================================================
# OHLCV Schema
# =============================================================================

OHLCV_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),  # e.g., "AAPL.US"
    ("date", pa.date32(), False),
    ("open", pa.float64(), False),
    ("high", pa.float64(), False),
    ("low", pa.float64(), False),
    ("close", pa.float64(), False),
    ("adjusted_close", pa.float64(), True),
    ("volume", pa.int64(), False),
    ("exchange", pa.string(), True),  # NYSE, NASDAQ, AMEX
])

OHLCV_PRIMARY_KEY = ["ticker", "date"]
OHLCV_REQUIRED_COLS = ["ticker", "date", "open", "high", "low", "close", "volume"]


# =============================================================================
# Index Membership Schema (S&P 500)
# =============================================================================

MEMBERSHIP_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("index_name", pa.string(), False),  # "SP500"
    ("start_date", pa.date32(), True),   # Date added to index (null if unknown)
    ("end_date", pa.date32(), True),     # null = still in index
    ("is_delisted", pa.bool_(), True),   # True = no longer trades on exchange
    ("company_name", pa.string(), True),
    ("exchange", pa.string(), True),
    ("sector", pa.string(), True),
    ("industry", pa.string(), True),
])

MEMBERSHIP_PRIMARY_KEY = ["ticker", "index_name"]  # Simplified - each ticker appears once
MEMBERSHIP_REQUIRED_COLS = ["ticker", "index_name"]


# =============================================================================
# Corporate Actions: Dividends
# =============================================================================

DIVIDENDS_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("ex_date", pa.date32(), False),        # Ex-dividend date
    ("payment_date", pa.date32(), True),    # Payment date
    ("record_date", pa.date32(), True),     # Record date
    ("declared_date", pa.date32(), True),   # Declaration date
    ("amount", pa.float64(), False),        # Dividend amount per share
    ("currency", pa.string(), True),        # USD, etc.
    ("dividend_type", pa.string(), True),   # Cash, Stock, etc.
])

DIVIDENDS_PRIMARY_KEY = ["ticker", "ex_date"]
DIVIDENDS_REQUIRED_COLS = ["ticker", "ex_date", "amount"]


# =============================================================================
# Corporate Actions: Splits
# =============================================================================

SPLITS_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("date", pa.date32(), False),           # Effective date
    ("split_ratio", pa.string(), False),    # e.g., "2/1", "3/2"
    ("split_factor", pa.float64(), False),  # Numeric: 2.0, 1.5, etc.
])

SPLITS_PRIMARY_KEY = ["ticker", "date"]
SPLITS_REQUIRED_COLS = ["ticker", "date", "split_ratio", "split_factor"]


# =============================================================================
# Fundamentals: Annual & Quarterly Financial Statements
# =============================================================================

# Note: Fundamentals are complex and vary by company. We define a flexible schema
# that captures the most common fields. Uncommon fields are stored as additional columns.

FUNDAMENTALS_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("period_end_date", pa.date32(), False),  # Fiscal period end (e.g., 2023-12-31)
    ("report_date", pa.date32(), True),       # Date reported/filed (filing lag)
    ("period_type", pa.string(), False),      # "annual" or "quarterly"
    ("fiscal_year", pa.int32(), False),
    ("fiscal_quarter", pa.int32(), True),     # 1-4 for quarterly, null for annual
    ("currency", pa.string(), True),
    
    # Income Statement
    ("revenue", pa.float64(), True),
    ("cost_of_revenue", pa.float64(), True),
    ("gross_profit", pa.float64(), True),
    ("operating_expense", pa.float64(), True),
    ("operating_income", pa.float64(), True),
    ("ebitda", pa.float64(), True),
    ("net_income", pa.float64(), True),
    ("eps_basic", pa.float64(), True),
    ("eps_diluted", pa.float64(), True),
    
    # Balance Sheet
    ("total_assets", pa.float64(), True),
    ("total_liabilities", pa.float64(), True),
    ("total_equity", pa.float64(), True),
    ("cash_and_equivalents", pa.float64(), True),
    ("short_term_debt", pa.float64(), True),
    ("long_term_debt", pa.float64(), True),
    
    # Cash Flow Statement
    ("operating_cash_flow", pa.float64(), True),
    ("investing_cash_flow", pa.float64(), True),
    ("financing_cash_flow", pa.float64(), True),
    ("free_cash_flow", pa.float64(), True),
    ("capex", pa.float64(), True),
])

FUNDAMENTALS_PRIMARY_KEY = ["ticker", "period_end_date", "period_type"]
FUNDAMENTALS_REQUIRED_COLS = ["ticker", "period_end_date", "period_type", "fiscal_year"]


# =============================================================================
# Fundamentals: Ratios & Metrics
# =============================================================================

RATIOS_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("date", pa.date32(), False),  # As-of date for the metrics
    ("period_type", pa.string(), True),  # "annual", "quarterly", or "ttm" (trailing twelve months)
    
    # Valuation
    ("market_cap", pa.float64(), True),
    ("enterprise_value", pa.float64(), True),
    ("pe_ratio", pa.float64(), True),
    ("pb_ratio", pa.float64(), True),
    ("ps_ratio", pa.float64(), True),
    ("peg_ratio", pa.float64(), True),
    ("ev_to_ebitda", pa.float64(), True),
    ("ev_to_sales", pa.float64(), True),
    
    # Profitability
    ("gross_margin", pa.float64(), True),
    ("operating_margin", pa.float64(), True),
    ("profit_margin", pa.float64(), True),
    ("roa", pa.float64(), True),  # Return on Assets
    ("roe", pa.float64(), True),  # Return on Equity
    ("roic", pa.float64(), True), # Return on Invested Capital
    
    # Liquidity & Solvency
    ("current_ratio", pa.float64(), True),
    ("quick_ratio", pa.float64(), True),
    ("debt_to_equity", pa.float64(), True),
    ("debt_to_assets", pa.float64(), True),
    
    # Efficiency
    ("asset_turnover", pa.float64(), True),
    ("inventory_turnover", pa.float64(), True),
    ("receivables_turnover", pa.float64(), True),
])

RATIOS_PRIMARY_KEY = ["ticker", "date"]
RATIOS_REQUIRED_COLS = ["ticker", "date"]


# =============================================================================
# Fundamentals: Shares Outstanding & Market Cap
# =============================================================================

SHARES_OUTSTANDING_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("date", pa.date32(), False),
    ("shares_outstanding", pa.float64(), True),   # Basic shares
    ("shares_outstanding_diluted", pa.float64(), True),
    ("float_shares", pa.float64(), True),         # Publicly tradable
    ("market_cap", pa.float64(), True),
])

SHARES_OUTSTANDING_PRIMARY_KEY = ["ticker", "date"]
SHARES_OUTSTANDING_REQUIRED_COLS = ["ticker", "date"]


# =============================================================================
# Fundamentals: Company Classifications
# =============================================================================

CLASSIFICATIONS_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("company_name", pa.string(), True),
    ("exchange", pa.string(), True),
    ("sector", pa.string(), True),
    ("industry", pa.string(), True),
    ("gics_sector", pa.string(), True),
    ("gics_industry", pa.string(), True),
    ("country", pa.string(), True),
    ("currency", pa.string(), True),
    ("isin", pa.string(), True),
    ("cusip", pa.string(), True),
    ("last_updated", pa.date32(), True),
])

CLASSIFICATIONS_PRIMARY_KEY = ["ticker"]
CLASSIFICATIONS_REQUIRED_COLS = ["ticker"]


# =============================================================================
# Macro Time Series
# =============================================================================

MACRO_SCHEMA = pa.schema([
    ("series_code", pa.string(), False),  # e.g., "DGS10", "FEDFUNDS"
    ("date", pa.date32(), False),
    ("value", pa.float64(), False),
    ("series_name", pa.string(), True),
    ("category", pa.string(), True),  # rates, inflation, growth, etc.
])

MACRO_PRIMARY_KEY = ["series_code", "date"]
MACRO_REQUIRED_COLS = ["series_code", "date", "value"]


# =============================================================================
# Macro Time Series — ALFRED (Archival / Vintage)
# =============================================================================
# ALFRED extends FRED by recording the real-time period during which each value
# was the latest available.  The primary key is (series_code, date, realtime_start)
# because the same (series, date) pair can have multiple historical revisions.

MACRO_ALFRED_SCHEMA = pa.schema([
    ("series_code",    pa.string(),  False),  # e.g., "DGS10", "FEDFUNDS"
    ("date",           pa.date32(), False),   # Observation date
    ("realtime_start", pa.date32(), False),   # First date this value was latest
    ("realtime_end",   pa.date32(), True),    # Last date this value was latest; nullable for current vintage
    ("value",          pa.float64(), False),
    ("series_name",    pa.string(),  True),
    ("category",       pa.string(),  True),   # rates, inflation, growth, etc.
])

MACRO_ALFRED_PRIMARY_KEY   = ["series_code", "date", "realtime_start"]
MACRO_ALFRED_REQUIRED_COLS = ["series_code", "date", "realtime_start", "value"]


# =============================================================================
# Meta: Ingestion Log
# =============================================================================

INGESTION_LOG_SCHEMA = pa.schema([
    ("run_id", pa.string(), False),          # UUID for each ingestion run
    ("dataset", pa.string(), False),         # "ohlcv", "dividends", "membership", etc.
    ("ticker", pa.string(), True),           # null for universe-level datasets
    ("start_date", pa.date32(), True),       # Query start date
    ("end_date", pa.date32(), True),         # Query end date
    ("status", pa.string(), False),          # "success", "partial", "failed"
    ("rows_ingested", pa.int64(), True),     # Number of rows added
    ("rows_existing", pa.int64(), True),     # Rows already present (skipped)
    ("error_message", pa.string(), True),    # Error details if failed
    ("duration_seconds", pa.float64(), True),
    ("timestamp", pa.timestamp("ms"), False), # When the ingestion occurred
])

INGESTION_LOG_PRIMARY_KEY = ["run_id", "dataset", "ticker"]
INGESTION_LOG_REQUIRED_COLS = ["run_id", "dataset", "status", "timestamp"]


# =============================================================================
# Meta: Ticker Mapping
# =============================================================================

TICKER_MAPPING_SCHEMA = pa.schema([
    ("ticker", pa.string(), False),
    ("ticker_normalized", pa.string(), False),  # Stripped of .US_old, etc.
    ("exchange", pa.string(), True),
    ("is_delisted", pa.bool_(), False),
    ("delisted_date", pa.date32(), True),
    ("last_seen", pa.date32(), True),
])

TICKER_MAPPING_PRIMARY_KEY = ["ticker"]
TICKER_MAPPING_REQUIRED_COLS = ["ticker", "ticker_normalized", "is_delisted"]


# =============================================================================
# Schema Validation & Enforcement
# =============================================================================

def validate_dataframe(
    df: pd.DataFrame,
    schema: pa.Schema,
    required_cols: list[str],
    primary_key: list[str],
    dataset_name: str = "dataset",
) -> pd.DataFrame:
    """
    Validate and enforce schema on a DataFrame.
    
    Args:
        df: Input DataFrame
        schema: PyArrow schema to enforce
        required_cols: Columns that must be present
        primary_key: Columns forming the primary key (used for deduplication)
        dataset_name: Name for error messages
        
    Returns:
        Validated DataFrame with correct dtypes
        
    Raises:
        ValueError: If required columns are missing or schema is invalid
    """
    if df.empty:
        return df
    
    # Check required columns
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"{dataset_name}: Missing required columns: {missing_cols}"
        )
    
    # Deduplicate by primary key
    if primary_key:
        duplicates = df.duplicated(subset=primary_key, keep="last")
        if duplicates.any():
            n_dupes = duplicates.sum()
            print(f"  Warning: Removed {n_dupes} duplicate rows from {dataset_name}")
            df = df[~duplicates].copy()
    
    # Convert to PyArrow schema (coerce types)
    try:
        # Add missing columns with nulls
        for field in schema:
            if field.name not in df.columns:
                df[field.name] = None
        
        # Reorder columns to match schema
        schema_cols = [field.name for field in schema]
        df = df[schema_cols].copy()
        
        # Convert dtypes via PyArrow
        table = pa.Table.from_pandas(df, schema=schema, safe=False)
        df = table.to_pandas()
        
    except Exception as e:
        raise ValueError(f"{dataset_name}: Schema validation failed: {e}")
    
    return df


def get_schema_for_dataset(dataset: str) -> tuple[pa.Schema, list[str], list[str]]:
    """
    Get the schema, required columns, and primary key for a dataset.
    
    Args:
        dataset: Dataset name (e.g., "ohlcv", "dividends", "fundamentals_annual")
        
    Returns:
        (schema, required_cols, primary_key)
    """
    mapping = {
        "ohlcv": (OHLCV_SCHEMA, OHLCV_REQUIRED_COLS, OHLCV_PRIMARY_KEY),
        "membership": (MEMBERSHIP_SCHEMA, MEMBERSHIP_REQUIRED_COLS, MEMBERSHIP_PRIMARY_KEY),
        "dividends": (DIVIDENDS_SCHEMA, DIVIDENDS_REQUIRED_COLS, DIVIDENDS_PRIMARY_KEY),
        "splits": (SPLITS_SCHEMA, SPLITS_REQUIRED_COLS, SPLITS_PRIMARY_KEY),
        "fundamentals_annual": (FUNDAMENTALS_SCHEMA, FUNDAMENTALS_REQUIRED_COLS, FUNDAMENTALS_PRIMARY_KEY),
        "fundamentals_quarterly": (FUNDAMENTALS_SCHEMA, FUNDAMENTALS_REQUIRED_COLS, FUNDAMENTALS_PRIMARY_KEY),
        "ratios": (RATIOS_SCHEMA, RATIOS_REQUIRED_COLS, RATIOS_PRIMARY_KEY),
        "shares_outstanding": (SHARES_OUTSTANDING_SCHEMA, SHARES_OUTSTANDING_REQUIRED_COLS, SHARES_OUTSTANDING_PRIMARY_KEY),
        "classifications": (CLASSIFICATIONS_SCHEMA, CLASSIFICATIONS_REQUIRED_COLS, CLASSIFICATIONS_PRIMARY_KEY),
        "macro": (MACRO_SCHEMA, MACRO_REQUIRED_COLS, MACRO_PRIMARY_KEY),
        "macro_alfred": (MACRO_ALFRED_SCHEMA, MACRO_ALFRED_REQUIRED_COLS, MACRO_ALFRED_PRIMARY_KEY),
        "ingestion_log": (INGESTION_LOG_SCHEMA, INGESTION_LOG_REQUIRED_COLS, INGESTION_LOG_PRIMARY_KEY),
        "ticker_mapping": (TICKER_MAPPING_SCHEMA, TICKER_MAPPING_REQUIRED_COLS, TICKER_MAPPING_PRIMARY_KEY),
    }
    
    if dataset not in mapping:
        raise ValueError(f"Unknown dataset: {dataset}")
    
    return mapping[dataset]


def save_schema_version(output_dir: str) -> None:
    """Save the current schema version to disk for reproducibility."""
    import json
    from pathlib import Path
    
    version_file = Path(output_dir) / "schema-versions" / f"v{SCHEMA_VERSION}.json"
    version_file.parent.mkdir(parents=True, exist_ok=True)
    
    schema_info = {
        "version": SCHEMA_VERSION,
        "timestamp": datetime.now().isoformat(),
        "schemas": {
            "ohlcv": str(OHLCV_SCHEMA),
            "membership": str(MEMBERSHIP_SCHEMA),
            "dividends": str(DIVIDENDS_SCHEMA),
            "splits": str(SPLITS_SCHEMA),
            "fundamentals": str(FUNDAMENTALS_SCHEMA),
            "ratios": str(RATIOS_SCHEMA),
            "shares_outstanding": str(SHARES_OUTSTANDING_SCHEMA),
            "classifications": str(CLASSIFICATIONS_SCHEMA),
            "macro": str(MACRO_SCHEMA),
            "macro_alfred": str(MACRO_ALFRED_SCHEMA),
            "ingestion_log": str(INGESTION_LOG_SCHEMA),
            "ticker_mapping": str(TICKER_MAPPING_SCHEMA),
        },
    }
    
    with open(version_file, "w") as f:
        json.dump(schema_info, f, indent=2)
    
    print(f"  Saved schema version {SCHEMA_VERSION} to {version_file}")
