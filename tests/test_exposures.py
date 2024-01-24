# pylint: disable=protected-access,no-member

from operator import itemgetter
from pathlib import Path

import yaml

from ._core import FIXTURES_PATH, TMP_PATH, TestCore


class TestExposures(TestCore):
    def setUp(self):
        super().setUp()
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

        self._assert_exposures(
            fixtures_path / "a_look_at_your_customers_table.yml",
            output_path / "a_look_at_your_customers_table.yml",
        )
        self._assert_exposures(
            fixtures_path / "our_analytics.yml",
            output_path / "our_analytics.yml",
        )

    def test_exposures_grouping_type(self):
        fixtures_path = FIXTURES_PATH / "exposure" / "type"
        output_path = TMP_PATH / "exposure" / "type"
        self.c.extract_exposures(
            output_path=str(output_path),
            output_grouping="type",
        )

        for i in range(1, 18):
            self._assert_exposures(
                fixtures_path / "card" / f"{i}.yml",
                output_path / "card" / f"{i}.yml",
            )

        for i in range(1, 2):
            self._assert_exposures(
                fixtures_path / "dashboard" / f"{i}.yml",
                output_path / "dashboard" / f"{i}.yml",
            )
