import logging

from dbtmetabase.format import setup_logging

from .test_exposures import *
from .test_format import *
from .test_manifest import *
from .test_metabase import *
from .test_models import *

setup_logging(level=logging.DEBUG, path=None)
