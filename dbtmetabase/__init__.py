import logging

from .core import DbtMetabase
from .format import Filter

__all__ = [
    "DbtMetabase",
    "Filter",
]

try:
    from ._version import __version__ as version

    __version__ = version
except ModuleNotFoundError:
    logging.warning("No _version.py file")
    __version__ = "0.0.0"
