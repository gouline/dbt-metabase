import logging

from .dbt import DbtReader
from .metabase import MetabaseClient

logger = logging.getLogger(__name__)

__all__ = ["DbtReader", "MetabaseClient"]

try:
    from ._version import __version__ as version  # type: ignore

    __version__ = version
except ModuleNotFoundError:
    logging.warning("No _version.py file")
    __version__ = "0.0.0"
