import importlib.metadata
import logging

from .dbt import DbtReader
from .metabase import MetabaseClient

__all__ = ["DbtReader", "MetabaseClient"]

try:
    __version__ = importlib.metadata.version("dbt-metabase")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0-UNKONWN"
    logging.warning("No version found in metadata")
