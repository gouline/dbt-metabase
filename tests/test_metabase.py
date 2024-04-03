import unittest

from ._mocks import MockMetabase


class TestMetabase(unittest.TestCase):
    def setUp(self):
        self.metabase = MockMetabase(url="http://localhost")

    def test_metabase_find_database(self):
        db = self.metabase.find_database(name="unit_testing")
        assert db
        self.assertEqual(2, db["id"])
        self.assertIsNone(self.metabase.find_database(name="foo"))

    def test_metabase_get_collections(self):
        excluded = self.metabase.get_collections(exclude_personal=True)
        self.assertEqual(3, len(excluded))

        included = self.metabase.get_collections(exclude_personal=False)
        self.assertEqual(4, len(included))

    def test_metabase_get_collection_items(self):
        cards = self.metabase.get_collection_items(
            uid="3",
            models=("card",),
        )
        self.assertEqual({"card"}, {item["model"] for item in cards})

        dashboards = self.metabase.get_collection_items(
            uid="3",
            models=("dashboard",),
        )
        self.assertEqual({"dashboard"}, {item["model"] for item in dashboards})

        both = self.metabase.get_collection_items(
            uid="3",
            models=("card", "dashboard"),
        )
        self.assertEqual({"card", "dashboard"}, {item["model"] for item in both})

    def test_metabase_authorize_using_username_and_password(self):
        self.metabase = MockMetabase(url="http://localhost", session_id=None, api_key=None, username="user_1", password="password_1")
        headers = self.metabase.session.headers
        self.assertEquals("session_for_user_1", headers["X-Metabase-Session"])

    def test_metabase_authorize_using_session_id(self):
        self.metabase = MockMetabase(url="http://localhost", session_id="session_id_1")
        headers = self.metabase.session.headers
        self.assertEquals("session_id_1", headers["X-Metabase-Session"])
    
    def test_metabase_authorize_using_session_api_key(self):
        self.metabase = MockMetabase(url="http://localhost", session_id=None, api_key="api_key_1")
        headers = self.metabase.session.headers
        self.assertEquals("api_key_1", headers["X-API-KEY"])
