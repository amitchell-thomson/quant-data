import pandas as pd
import pyarrow as pa

from .utils import _get_dataset, _load_config

TYPE_MAP = {
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "date32": pa.date32(),
    "bool": pa.bool_(),
}


def load_schema(dataset_key: str, include_alfred: bool = False) -> tuple[pa.Schema, list[str], list[str]]:
    """
    Takes input of dataset_key of the form "equities.daily".

    Returns (schema, required_cols, primary_key) for a dataset key like "equities.daily".
    """
    config = _load_config("datasets")
    dataset = _get_dataset(config, dataset_key)
    list_schema = [(name, TYPE_MAP[dtype]) for name, dtype in dataset["columns"].items()]
    if include_alfred:
        list_schema.extend([(name, TYPE_MAP[dtype]) for name, dtype in dataset["alfred_columns"].items()])
    schema = pa.schema(list_schema)
    required_cols = list(dataset["columns"].keys())
    primary_key = dataset["primary_key"]

    return schema, required_cols, primary_key


def validate_dataframe(df, schema: pa.Schema, required_cols: list, primary_key: list) -> pd.DataFrame:
    """
    Enforce schema, deduplicate by primary key.

    Returns a df with the required columns, matching the schema, and deduplicated by primary key.
    """
    # Check required columns are present, print which are missing (silently add extra columns )
    if missing_cols := set(required_cols) - set(df.columns):
        raise ValueError(f"DataFrame columns do not match required columns. Missing: {missing_cols}")

    # Cast columns to correct types
    try:
        df = df[required_cols]
        table = pa.Table.from_pandas(df, schema=schema)
        df = table.to_pandas()
    except Exception as e:
        raise ValueError(f"Error casting DataFrame to schema: {e}")

    # Deduplicate by primary key
    df = df.drop_duplicates(subset=primary_key, keep="last")
    return df
