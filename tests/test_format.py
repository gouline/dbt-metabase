import unittest

from dbtmetabase.format import Filter, NullValue, dump_yaml, safe_description, safe_name

from ._mocks import FIXTURES_PATH, TMP_PATH


class TestFormat(unittest.TestCase):
    def test_filter(self):
        self.assertTrue(
            Filter(
                include=("alpHa", "bRavo"),
            ).match("Alpha")
        )
        self.assertTrue(Filter().match("Alpha"))
        self.assertTrue(Filter().match(""))
        self.assertFalse(
            Filter(
                include=("alpHa", "bRavo"),
                exclude=("alpha",),
            ).match("Alpha")
        )
        self.assertFalse(
            Filter(
                exclude=("alpha",),
            ).match("Alpha")
        )
        self.assertTrue(Filter(include="alpha").match("Alpha"))
        self.assertFalse(Filter(exclude="alpha").match("Alpha"))

    def test_filter_wildcard(self):
        self.assertTrue(Filter(include="stg_*").match("stg_orders"))
        self.assertTrue(Filter(include="STG_*").match("stg_ORDERS"))
        self.assertFalse(Filter(include="stg_*").match("orders"))
        self.assertTrue(Filter(include="order?").match("orders"))
        self.assertFalse(Filter(include="order?").match("ordersz"))
        self.assertTrue(Filter(include="*orders", exclude="stg_*").match("_orders"))
        self.assertFalse(Filter(include="*orders", exclude="stg_*").match("stg_orders"))

    def test_null_value(self):
        self.assertIsNotNone(NullValue)
        self.assertFalse(NullValue)
        self.assertIs(NullValue, NullValue)

    def test_safe_name(self):
        self.assertEqual(
            "somebody_s_2_collections_",
            safe_name("Somebody's 2 collections!"),
        )
        self.assertEqual(
            "somebody_s_2_collections_",
            safe_name("somebody_s_2_collections_"),
        )
        self.assertEqual("", safe_name(""))

    def test_safe_description(self):
        self.assertEqual(
            "Depends on\n\nQuestion ( #2 )!",
            safe_description("Depends on\n\nQuestion {{ #2 }}!"),
        )
        self.assertEqual(
            "Depends on\n\nQuestion ( #2 )!",
            safe_description("Depends on\n\nQuestion ( #2 )!"),
        )
        self.assertEqual(
            "Depends on\n\nQuestion { #2 }!",
            safe_description("Depends on\n\nQuestion { #2 }!"),
        )
        self.assertEqual(
            "(start_date) - cast((rolling_days))",
            safe_description("{{start_date}} - cast({{rolling_days}})"),
        )

    def test_dump_yaml(self):
        fixture_path = FIXTURES_PATH / "test_dump_yaml.yml"
        output_path = TMP_PATH / "test_dump_yaml.yml"
        with open(output_path, "w", encoding="utf-8") as f:
            dump_yaml(
                data={
                    "root": {
                        "attr1": "val1\nend",
                        "attr2": ["val2", "val3"],
                    },
                },
                stream=f,
            )
        with open(output_path, "r", encoding="utf-8") as f:
            actual = f.read()
        with open(fixture_path, "r", encoding="utf-8") as f:
            expected = f.read()
        self.assertEqual(expected, actual)
