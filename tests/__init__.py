import logging

from dbtmetabase._format import setup_logging

from .test_core_exposures import *
from .test_core_models import *
from .test_manifest import *

setup_logging(level=logging.DEBUG, path=None)
