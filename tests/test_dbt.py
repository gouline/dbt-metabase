import unittest

from dbtmetabase.dbt import DbtReader


class MockDbtReader(DbtReader):
    pass


class TestDbtReader(unittest.TestCase):
    def setUp(self):
        self.reader = MockDbtReader(project_path=".")

    def test_dummy(self):
        self.assertTrue(True)
