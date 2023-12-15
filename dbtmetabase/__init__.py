import importlib.metadata
import logging

from .dbt import DbtReader
from .metabase import MetabaseClient

logger = logging.getLogger(__name__)

__all__ = ["DbtReader", "MetabaseClient"]

try:
    __version__ = importlib.metadata.version("dbt-metabase")
except importlib.metadata.PackageNotFoundError:
    logger.warning("No version found in metadata")
    __version__ = "0.0.0-UNKONWN"
