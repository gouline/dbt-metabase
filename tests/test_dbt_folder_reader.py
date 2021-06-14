import unittest

from dbtmetabase.parsers.dbt_folder import DbtFolderReader


class MockDbtFolderReader(DbtFolderReader):
    pass


class TestDbtFolderReader(unittest.TestCase):
    def setUp(self):
        self.reader = DbtFolderReader(project_path=".")

    def test_dummy(self):
        self.assertTrue(True)
