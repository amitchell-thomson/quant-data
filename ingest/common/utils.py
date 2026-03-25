import pathlib

import yaml


def _load_config(config_name: str) -> dict:
    """
    Load a config file from the "configs" directory. For example, load_config("ingest") will load "configs/ingest.yaml".
    """

    config_path = pathlib.Path(__file__).parent.parent.parent / "config" / f"{config_name}.yaml"
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file '{config_path}' not found")


def _get_dataset(config, dataset_key: str) -> dict:
    """
    Takes input of dataset_key of the form "equities.daily".

    Returns the dataset config.
    Raises KeyError with a clear message if not found.
    """
    try:
        level1, level2 = dataset_key.split(".")
    except ValueError:
        raise KeyError(f"Invalid dataset_key '{dataset_key}'. Expected format 'section.dataset'")

    try:
        return config[level1][level2]
    except KeyError:
        raise KeyError(f"Dataset '{dataset_key}' not found in config")
