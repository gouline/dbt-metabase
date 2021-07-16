import unittest

from dbtmetabase.metabase import MetabaseClient


class MockMetabaseClient(MetabaseClient):
    def get_session_id(self, user: str, password: str) -> str:
        return "dummy"


class TestMetabaseClient(unittest.TestCase):
    def setUp(self):
        self.client = MockMetabaseClient(
            host="localhost",
            user="dummy",
            password="dummy",
            use_http=True,
        )

    def test_dummy(self):
        self.assertTrue(True)
