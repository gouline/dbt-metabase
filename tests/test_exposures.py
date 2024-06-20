import unittest
from operator import itemgetter
from pathlib import Path

import yaml

from ._mocks import FIXTURES_PATH, TMP_PATH, MockDbtMetabase


class TestExposures(unittest.TestCase):
    def setUp(self):
        self.c = MockDbtMetabase()
        TMP_PATH.mkdir(exist_ok=True)

    def _assert_exposures(self, expected_path: Path, actual_path: Path):
        with open(expected_path, encoding="utf-8") as f:
            expected = yaml.safe_load(f)
        with open(actual_path, encoding="utf-8") as f:
            actual = yaml.safe_load(f)

        self.assertEqual(
            sorted(expected["exposures"], key=itemgetter("name")),
            actual["exposures"],
        )

    def test_exposures(self):
        fixtures_path = FIXTURES_PATH / "exposure" / "default"
        output_path = TMP_PATH / "exposure" / "default"
        self.c.extract_exposures(
            output_path=str(output_path),
            output_grouping=None,
        )

        self._assert_exposures(
            fixtures_path / "exposures.yml",
            output_path / "exposures.yml",
        )

    def test_exposures_collection_grouping(self):
        fixtures_path = FIXTURES_PATH / "exposure" / "collection"
        output_path = TMP_PATH / "exposure" / "collection"
        self.c.extract_exposures(
            output_path=str(output_path),
            output_grouping="collection",
        )

        for file in fixtures_path.iterdir():
            self._assert_exposures(file, output_path / file.name)

    def test_exposures_grouping_type(self):
        fixtures_path = FIXTURES_PATH / "exposure" / "type"
        output_path = TMP_PATH / "exposure" / "type"
        self.c.extract_exposures(
            output_path=str(output_path),
            output_grouping="type",
        )

        for file in (fixtures_path / "card").iterdir():
            self._assert_exposures(file, output_path / "card" / file.name)

        for file in (fixtures_path / "dashboard").iterdir():
            self._assert_exposures(file, output_path / "dashboard" / file.name)

    def test_exposures_aliased_ref(self):
        for model in self.c.manifest.read_models():
            if not model.name.startswith("stg_"):
                model.alias = f"{model.name}_alias"

        aliases = [m.alias for m in self.c.manifest.read_models()]
        self.assertIn("orders_alias", aliases)
        self.assertIn("customers_alias", aliases)

        fixtures_path = FIXTURES_PATH / "exposure" / "default"
        output_path = TMP_PATH / "exposure" / "aliased"
        self.c.extract_exposures(
            output_path=str(output_path),
            output_grouping=None,
        )

        self._assert_exposures(
            fixtures_path / "exposures.yml",
            output_path / "exposures.yml",
        )
