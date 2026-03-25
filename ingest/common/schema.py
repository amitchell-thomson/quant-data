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


def load_schema(dataset_key: str) -> tuple[pa.Schema, list[str], list[str]]:
    """
    Takes input of dataset_key of the form "equities.daily".

    Returns (schema, required_cols, primary_key) for a dataset key like "equities.daily".
    """
    config = _load_config("datasets")
    dataset = _get_dataset(config, dataset_key)
    list_schema = [(name, TYPE_MAP[dtype]) for name, dtype in dataset["columns"].items()]
    schema = pa.schema(list_schema)
    required_cols = list(dataset["columns"].keys())
    primary_key = dataset["primary_key"]

    return schema, required_cols, primary_key


def validate_dataframe(df, schema, required_cols, primary_key):
    """
    Enforce schema, deduplicate by primary key.
    """
    ...


print(load_schema("equities.daily"))
