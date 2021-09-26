import logging
import sys
from pathlib import Path

import yaml


def get_version() -> str:
    """Checks _version.py or build metadata for package version.

    Returns:
        str: Version string.
    """

    try:
        from ._version import version

        return version
    except ModuleNotFoundError:
        logging.debug("No _version.py found")

    # importlib is only available on Python 3.8+
    if sys.version_info >= (3, 8):
        # pylint: disable=no-member
        import importlib.metadata

        try:
            return importlib.metadata.version("dbt-metabase")
        except importlib.metadata.PackageNotFoundError:
            logging.warning("No version found in metadata")

    return "0.0.0-UNKONWN"


def load_config() -> dict:
    config_data = {}
    config_path = Path.home() / ".dbt-metabase"
    if (config_path / "config.yml").exists():
        with open(config_path / "config.yml", "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f).get("config", {})
    elif (config_path / "config.yaml").exists():
        with open(config_path / "config.yaml", "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f).get("config", {})
    return config_data
