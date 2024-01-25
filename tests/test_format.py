import unittest
from pathlib import Path

from dbtmetabase.format import Filter, NullValue, dump_yaml, safe_description, safe_name


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

    def test_dump_yaml(self):
        path = Path("tests") / "tmp" / "test_dump_yaml.yml"
        with open(path, "w", encoding="utf-8") as f:
            dump_yaml(
                data={
                    "root": {
                        "attr1": "val1\nend",
                        "attr2": ["val2", "val3"],
                    },
                },
                stream=f,
            )
        with open(path, "r", encoding="utf-8") as f:
            self.assertEqual(
                """root:
  attr1: 'val1

    end'
  attr2:
    - val2
    - val3
""",
                f.read(),
            )
