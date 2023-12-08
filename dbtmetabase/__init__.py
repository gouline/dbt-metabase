import importlib.metadata
import logging

from .models.interface import DbtInterface, MetabaseInterface

__all__ = ["MetabaseInterface", "DbtInterface"]

try:
    __version__ = importlib.metadata.version("dbt-metabase")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0-UNKONWN"
    logging.warning("No version found in metadata")
