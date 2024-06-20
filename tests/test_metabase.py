import unittest

from ._mocks import MockMetabase


class TestMetabase(unittest.TestCase):
    def setUp(self):
        self.metabase = MockMetabase(url="http://localhost")

    def test_metabase_find_database(self):
        db = self.metabase.find_database(name="dbtmetabase")
        assert db
        self.assertEqual(2, db["id"])
        self.assertIsNone(self.metabase.find_database(name="foo"))

    def test_metabase_get_collections(self):
        excluded = self.metabase.get_collections(exclude_personal=True)
        self.assertEqual(1, len(excluded))

        included = self.metabase.get_collections(exclude_personal=False)
        self.assertEqual(2, len(included))

    def test_metabase_get_collection_items(self):
        cards = self.metabase.get_collection_items(
            uid="root",
            models=("card",),
        )
        self.assertEqual({"card"}, {item["model"] for item in cards})

        dashboards = self.metabase.get_collection_items(
            uid="root",
            models=("dashboard",),
        )
        self.assertEqual({"dashboard"}, {item["model"] for item in dashboards})

        both = self.metabase.get_collection_items(
            uid="root",
            models=("card", "dashboard"),
        )
        self.assertEqual({"card", "dashboard"}, {item["model"] for item in both})
