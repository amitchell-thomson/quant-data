"""
Parquet I/O utilities for append-only, survivorship-bias-safe data ingestion.

Provides functions to:
- Read existing Parquet files
- Merge new data with existing data (idempotent upserts)
- Write data with schema enforcement
- Handle partitioning by exchange, year, etc.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .schema import validate_dataframe


def read_parquet(
    file_path: Path | str,
    columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Read a Parquet file, returning an empty DataFrame if it doesn't exist.
    
    Args:
        file_path: Path to Parquet file
        columns: Optional list of columns to read
        
    Returns:
        DataFrame (empty if file doesn't exist)
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(file_path, columns=columns)
        return df
    except Exception as e:
        print(f"  Warning: Failed to read {file_path}: {e}")
        return pd.DataFrame()


def merge_dataframes(
    existing: pd.DataFrame,
    new: pd.DataFrame,
    primary_key: list[str],
    dataset_name: str = "dataset",
) -> tuple[pd.DataFrame, int, int]:
    """
    Merge new data into existing data, preferring new records on conflicts.
    
    This implements an "upsert" operation: existing rows are updated if
    the primary key matches, otherwise new rows are appended.
    
    Args:
        existing: Existing DataFrame
        new: New DataFrame to merge in
        primary_key: Columns forming the primary key
        dataset_name: Name for logging
        
    Returns:
        (merged_df, rows_new, rows_updated)
    """
    if new.empty:
        return existing, 0, 0
    
    if existing.empty:
        return new, len(new), 0
    
    # Ensure primary key columns exist in both
    missing_in_existing = set(primary_key) - set(existing.columns)
    missing_in_new = set(primary_key) - set(new.columns)
    
    if missing_in_existing or missing_in_new:
        raise ValueError(
            f"{dataset_name}: Primary key columns missing. "
            f"Existing: {missing_in_existing}, New: {missing_in_new}"
        )
    
    # Mark source for tracking
    existing = existing.copy()
    new = new.copy()
    
    # Normalize date columns to ensure consistent types before sorting
    # Parquet date32 can load as datetime.date objects, which can't be
    # compared with Timestamp objects during sort_values
    import warnings
    
    for col in existing.columns:
        if col in new.columns:
            existing_dtype = str(existing[col].dtype)
            new_dtype = str(new[col].dtype)
            
            # If either column is datetime-like or object (could be date), normalize
            if ('datetime' in existing_dtype or 'datetime' in new_dtype or 
                existing_dtype == 'object' or new_dtype == 'object'):
                try:
                    # Suppress the dateutil warning for mixed formats
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', message='Could not infer format')
                        existing[col] = pd.to_datetime(existing[col])
                        new[col] = pd.to_datetime(new[col])
                except (ValueError, TypeError):
                    pass  # Not a date column, leave as is
    
    existing["_source"] = "existing"
    new["_source"] = "new"
    
    # Concatenate and remove duplicates, keeping the "new" records
    merged = pd.concat([existing, new], ignore_index=True)
    
    # Sort by primary key + source (so "new" comes after "existing")
    # Then drop duplicates keeping the last (which will be "new")
    sort_cols = primary_key + ["_source"]
    merged = merged.sort_values(sort_cols)
    
    duplicates = merged.duplicated(subset=primary_key, keep="last")
    rows_updated = duplicates.sum()
    
    merged = merged[~duplicates].copy()
    
    # Count truly new rows (not updates)
    rows_new = (merged["_source"] == "new").sum()
    
    # Drop the tracking column
    merged = merged.drop(columns=["_source"])
    
    return merged, rows_new, rows_updated


def write_parquet(
    df: pd.DataFrame,
    file_path: Path | str,
    schema: pa.Schema,
    required_cols: list[str],
    primary_key: list[str],
    dataset_name: str = "dataset",
    validate: bool = True,
) -> None:
    """
    Write a DataFrame to Parquet with schema enforcement and compression.
    
    Args:
        df: DataFrame to write
        file_path: Output path
        schema: PyArrow schema to enforce
        required_cols: Required columns
        primary_key: Primary key columns (for validation)
        dataset_name: Name for error messages
        validate: Whether to validate schema before writing
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    if df.empty:
        print(f"  Warning: Not writing {file_path} (empty DataFrame)")
        return
    
    # Validate schema
    if validate:
        df = validate_dataframe(df, schema, required_cols, primary_key, dataset_name)
    
    # Convert to PyArrow Table and write
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    
    pq.write_table(
        table,
        file_path,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )


def upsert_parquet(
    new_df: pd.DataFrame,
    file_path: Path | str,
    schema: pa.Schema,
    required_cols: list[str],
    primary_key: list[str],
    dataset_name: str = "dataset",
) -> tuple[int, int]:
    """
    Upsert data into a Parquet file (read -> merge -> write).
    
    Args:
        new_df: New data to upsert
        file_path: Path to Parquet file
        schema: PyArrow schema
        required_cols: Required columns
        primary_key: Primary key for deduplication
        dataset_name: Name for logging
        
    Returns:
        (rows_new, rows_updated)
    """
    if new_df.empty:
        return 0, 0
    
    # Read existing data
    existing_df = read_parquet(file_path)
    
    # Merge
    merged_df, rows_new, rows_updated = merge_dataframes(
        existing_df, new_df, primary_key, dataset_name
    )
    
    # Write back
    write_parquet(
        merged_df,
        file_path,
        schema,
        required_cols,
        primary_key,
        dataset_name,
    )
    
    return rows_new, rows_updated


def read_partitioned_parquet(
    root_dir: Path | str,
    partition_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Read a partitioned Parquet dataset (e.g., partitioned by exchange or year).
    
    Args:
        root_dir: Root directory of partitioned dataset
        partition_cols: Partition column names
        
    Returns:
        Combined DataFrame from all partitions
    """
    root_dir = Path(root_dir)
    
    if not root_dir.exists():
        return pd.DataFrame()
    
    try:
        dataset = pq.ParquetDataset(root_dir)
        df = dataset.read().to_pandas()
        return df
    except Exception as e:
        print(f"  Warning: Failed to read partitioned dataset {root_dir}: {e}")
        return pd.DataFrame()


def write_partitioned_parquet(
    df: pd.DataFrame,
    root_dir: Path | str,
    partition_cols: list[str],
    schema: pa.Schema,
    basename_template: str = "part-{i}.parquet",
) -> None:
    """
    Write a DataFrame as a partitioned Parquet dataset.
    
    Args:
        df: DataFrame to write
        root_dir: Root directory for partitioned output
        partition_cols: Columns to partition by (e.g., ["exchange"])
        schema: PyArrow schema
        basename_template: Filename template for partitions
    """
    root_dir = Path(root_dir)
    root_dir.mkdir(parents=True, exist_ok=True)
    
    if df.empty:
        print(f"  Warning: Not writing {root_dir} (empty DataFrame)")
        return
    
    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    
    # Write partitioned dataset
    pq.write_to_dataset(
        table,
        root_path=str(root_dir),
        partition_cols=partition_cols,
        basename_template=basename_template,
        compression="snappy",
        use_dictionary=True,
        existing_data_behavior="overwrite_or_ignore",  # Idempotent
    )


def upsert_by_partition(
    new_df: pd.DataFrame,
    root_dir: Path | str,
    partition_col: str,
    schema: pa.Schema,
    required_cols: list[str],
    primary_key: list[str],
    dataset_name: str = "dataset",
) -> dict[str, tuple[int, int]]:
    """
    Upsert data into partitioned Parquet files (e.g., by exchange).
    
    For each partition value (e.g., each exchange), read the existing
    partition, merge, and write back.
    
    Args:
        new_df: New data to upsert
        root_dir: Root directory for partitions
        partition_col: Column to partition by (e.g., "exchange")
        schema: PyArrow schema
        required_cols: Required columns
        primary_key: Primary key for deduplication
        dataset_name: Name for logging
        
    Returns:
        Dict mapping partition value to (rows_new, rows_updated)
    """
    root_dir = Path(root_dir)
    results = {}
    
    if new_df.empty:
        return results
    
    # Group by partition column
    if partition_col not in new_df.columns:
        # No partition column, write to root
        partition_val = "default"
        file_path = root_dir / f"{partition_val}.parquet"
        rows_new, rows_updated = upsert_parquet(
            new_df, file_path, schema, required_cols, primary_key, dataset_name
        )
        results[partition_val] = (rows_new, rows_updated)
        return results
    
    for partition_val, group_df in new_df.groupby(partition_col):
        # Create partition directory
        partition_dir = root_dir / str(partition_val)
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Upsert into partition file
        file_path = partition_dir / f"{partition_val}.parquet"
        rows_new, rows_updated = upsert_parquet(
            group_df, file_path, schema, required_cols, primary_key, dataset_name
        )
        results[str(partition_val)] = (rows_new, rows_updated)
    
    return results


def normalize_ticker(ticker: str, strip_old: bool = True) -> str:
    """
    Normalize ticker symbols (handle .US_old suffix for delisted tickers).
    
    Args:
        ticker: Raw ticker (e.g., "AAPL.US" or "AAPL.US_old")
        strip_old: Whether to strip the "_old" suffix
        
    Returns:
        Normalized ticker
    """
    if strip_old and ticker.endswith("_old"):
        return ticker[:-4]  # Remove "_old"
    return ticker


def is_delisted(ticker: str) -> bool:
    """Check if a ticker is marked as delisted (_old suffix)."""
    return ticker.endswith("_old")
