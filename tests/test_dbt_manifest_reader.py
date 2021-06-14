import unittest

from dbtmetabase.parsers.dbt_manifest import DbtManifestReader


class MockDbtManifestReader(DbtManifestReader):
    pass


class TestDbtManifestReader(unittest.TestCase):
    def setUp(self):
        self.reader = DbtManifestReader(project_path=".")

    def test_dummy(self):
        self.assertTrue(True)
