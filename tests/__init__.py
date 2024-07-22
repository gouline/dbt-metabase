import logging

from dbtmetabase.format import setup_logging
from tests.test_exposures import *
from tests.test_format import *
from tests.test_manifest import *
from tests.test_metabase import *
from tests.test_models import *

setup_logging(level=logging.DEBUG, path=None)
