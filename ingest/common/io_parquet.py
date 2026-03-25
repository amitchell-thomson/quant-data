from pathlib import Path

import pandas as pd
import pyarrow as pa

from .schema import validate_dataframe


def read_parquet(file_path: Path) -> pd.DataFrame:
    """Simple utility for reading parquet files. If the file does not exist, it returns an empty DataFrame"""
    if not file_path.exists():
        return pd.DataFrame()
    return pd.read_parquet(file_path)


def merge_dataframes(existing: pd.DataFrame, new: pd.DataFrame, primary_key: list) -> pd.DataFrame:
    """Merges two DataFrames, letting new data win on conflicts, deduplicating by primary_key"""
    merged = pd.concat([existing, new], ignore_index=True)
    return merged.drop_duplicates(subset=primary_key, keep="last")


def upsert_parquet(new_df: pd.DataFrame, file_path: Path, schema: pa.schema, required_cols, primary_key) -> None:
    """
    Main function that the everything else calls

    1. Read existing data from file_path
    2. Merge with new_df
    3. Validate the merged result against the schema
    4. Create the parent directory if it doesn't exist
    5. Write to parquet
    """
    existing = read_parquet(file_path)
    merged_df = merge_dataframes(existing, new_df, primary_key=primary_key)
    validated_df = validate_dataframe(merged_df, schema=schema, required_cols=required_cols, primary_key=primary_key)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_parquet(file_path, index=False, compression="snappy")
