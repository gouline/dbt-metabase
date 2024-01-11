import unittest
from pathlib import Path

from dbtmetabase.manifest import Manifest

from ._mocks import MANIFEST_MODELS


class TestManifest(unittest.TestCase):
    def setUp(self):
        self.reader = Manifest(path=Path("tests") / "fixtures" / "manifest.json")

    def test_read_models(self):
        self.assertEqual(MANIFEST_MODELS, self.reader.read_models())
