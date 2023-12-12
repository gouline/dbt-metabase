import importlib.metadata
import logging

from .metabase import MetabaseClient
from .models.interface import DbtInterface

__all__ = ["DbtInterface", "MetabaseClient"]

try:
    __version__ = importlib.metadata.version("dbt-metabase")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0-UNKONWN"
    logging.warning("No version found in metadata")
