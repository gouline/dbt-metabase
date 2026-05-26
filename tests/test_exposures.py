from operator import itemgetter
from pathlib import Path
from typing import cast

import pytest
import yaml

from dbtmetabase._exposures import _build_model_refs, _Context, _Exposure
from dbtmetabase.format import safe_identifier
from dbtmetabase.manifest import Group, Model
from tests._mocks import FIXTURES_PATH, TMP_PATH, MockDbtMetabase, MockMetabase


def _assert_exposures(expected_path: Path, actual_path: Path):
    with open(expected_path, encoding="utf-8") as f:
        expected = yaml.safe_load(f)
    with open(actual_path, encoding="utf-8") as f:
        actual = yaml.safe_load(f)

    assert actual["exposures"] == sorted(expected["exposures"], key=itemgetter("name"))


@pytest.mark.parametrize("prefix", ["mbql4", "mbql5"])
def test_exposures_default(core: MockDbtMetabase, prefix: str):
    cast(MockMetabase, core.metabase).prefix = prefix

    fixtures_path = FIXTURES_PATH / prefix / "exposure" / "default"
    output_path = TMP_PATH / prefix / "exposure" / "default"
    core.extract_exposures(
        output_path=str(output_path),
        output_grouping=None,
        tags=["metabase"],
    )

    _assert_exposures(
        fixtures_path / "exposures.yml",
        output_path / "exposures.yml",
    )


@pytest.mark.parametrize("prefix", ["mbql4", "mbql5"])
def test_exposures_collection_grouping(core: MockDbtMetabase, prefix: str):
    cast(MockMetabase, core.metabase).prefix = prefix

    fixtures_path = FIXTURES_PATH / prefix / "exposure" / "collection"
    output_path = TMP_PATH / prefix / "exposure" / "collection"
    core.extract_exposures(
        output_path=str(output_path),
        output_grouping="collection",
    )

    for file in fixtures_path.iterdir():
        _assert_exposures(file, output_path / file.name)

    assert (output_path / f"{safe_identifier('коллекция')}.yml").exists()


@pytest.mark.parametrize("prefix", ["mbql4", "mbql5"])
def test_exposures_grouping_type(core: MockDbtMetabase, prefix: str):
    cast(MockMetabase, core.metabase).prefix = prefix

    fixtures_path = FIXTURES_PATH / prefix / "exposure" / "type"
    output_path = TMP_PATH / prefix / "exposure" / "type"
    core.extract_exposures(
        output_path=str(output_path),
        output_grouping="type",
    )

    for file in (fixtures_path / "card").iterdir():
        _assert_exposures(file, output_path / "card" / file.name)

    for file in (fixtures_path / "dashboard").iterdir():
        _assert_exposures(file, output_path / "dashboard" / file.name)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT * FROM database.schema.table0",
            {"database.schema.table0"},
        ),
        (
            "SELECT * FROM schema.table0",
            {"database.schema.table0"},
        ),
        (
            "SELECT * FROM table1",
            {"database.public.table1"},
        ),
        (
            'SELECT * FROM "schema".table0',
            {"database.schema.table0"},
        ),
        (
            'SELECT * FROM schema."table0"',
            {"database.schema.table0"},
        ),
        (
            'SELECT * FROM "schema"."table0"',
            {"database.schema.table0"},
        ),
        (
            "SELECT * FROM `schema.table0`",
            {"database.schema.table0"},
        ),
    ],
)
def test_extract_exposures_native_depends(
    core: MockDbtMetabase,
    query: str,
    expected: set,
):
    ctx = _Context(
        model_refs={
            "database.schema.table0": "model0",
            "database.public.table1": "model1",
        },
        database_names={1: "database"},
        table_names={},
    )
    exposure = _Exposure(
        model="card",
        uid="",
        label="",
    )
    core._extract_exposure_card(
        ctx=ctx,
        exposure=exposure,
        card={
            "dataset_query": {
                "type": "native",
                "database": 1,
                "native": {"query": query},
            }
        },
    )
    assert expected == exposure.depends
    assert query == exposure.native_query


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT * FROM schema.table0",
            {"schema.table0"},
        ),
        (
            "SELECT * FROM table1",
            {"public.table1"},
        ),
    ],
)
def test_extract_exposures_native_depends_without__database_name(
    core: MockDbtMetabase,
    query: str,
    expected: set,
):
    ctx = _Context(
        model_refs={
            "schema.table0": "model0",
            "public.table1": "model1",
        },
        database_names={1: ""},
        table_names={},
    )
    exposure = _Exposure(
        model="card",
        uid="",
        label="",
    )
    core._extract_exposure_card(
        ctx=ctx,
        exposure=exposure,
        card={
            "dataset_query": {
                "type": "native",
                "database": 1,
                "native": {"query": query},
            }
        },
    )
    assert expected == exposure.depends
    assert query == exposure.native_query


def test_model_refs_schema_alias_fallback_is_only_used_when_unique(
    core: MockDbtMetabase,
):
    unique_models = [
        Model(
            database="warehouse",
            schema="analytics",
            group=Group.nodes,
            name="orders",
            alias="orders",
        )
    ]
    unique_refs = _build_model_refs(unique_models)

    assert unique_refs["warehouse.analytics.orders"] == "ref('orders')"
    assert unique_refs["analytics.orders"] == "ref('orders')"

    ambiguous_models = [
        Model(
            database="warehouse",
            schema="analytics",
            group=Group.nodes,
            name="orders",
            alias="orders",
        ),
        Model(
            database="other_warehouse",
            schema="analytics",
            group=Group.nodes,
            name="orders_alt",
            alias="orders",
        ),
    ]
    ambiguous_refs = _build_model_refs(ambiguous_models)

    assert ambiguous_refs["warehouse.analytics.orders"] == "ref('orders')"
    assert ambiguous_refs["other_warehouse.analytics.orders"] == "ref('orders_alt')"
    assert "analytics.orders" not in ambiguous_refs


class _NullDatabaseMetabase:
    def __init__(self):
        self._database = {
            "id": 1,
            "name": "Athena connection",
            "details": {
                "dbname": None,
                "db": None,
                "catalog": None,
            },
        }

    def get_databases(self):
        return [self._database]

    def get_tables(self):
        return [
            {
                "id": 42,
                "name": "orders",
                "schema": "analytics",
                "db": self._database,
            }
        ]

    def get_collections(self, exclude_personal: bool):
        return [{"id": 7, "name": "Analytics", "slug": "analytics"}]

    def get_collection_items(self, uid: str, models: tuple[str, ...]):
        return [{"id": 99, "model": "card", "name": "Orders"}]

    def find_card(self, uid: int):
        return {
            "id": uid,
            "name": "Orders",
            "description": "Question over orders",
            "display": "table",
            "created_at": "2026-01-01T00:00:00Z",
            "dataset_query": {
                "type": "query",
                "query": {"source-table": 42},
            },
        }

    def find_dashboard(self, uid: int):
        return None

    def find_user(self, uid: int):
        return None

    def format_card_url(self, uid: str):
        return f"https://metabase.example.com/card/{uid}"

    def format_dashboard_url(self, uid: str):
        return f"https://metabase.example.com/dashboard/{uid}"


class _NullDatabaseCore(MockDbtMetabase):
    def __init__(self):
        self._manifest = type(
            "ManifestStub",
            (),
            {
                "read_models": lambda self: [
                    Model(
                        database="warehouse",
                        schema="analytics",
                        group=Group.nodes,
                        name="orders",
                        alias="orders",
                    )
                ]
            },
        )()
        self._metabase = _NullDatabaseMetabase()


def test_extract_exposures_handles_null_database_details(tmp_path: Path):
    core = _NullDatabaseCore()

    exposures = list(core.extract_exposures(output_path=str(tmp_path)))

    assert exposures[0]["body"]["depends_on"] == ["ref('orders')"]


class _UnsafeCollectionMetabase(_NullDatabaseMetabase):
    def get_collections(self, exclude_personal: bool):
        return [
            {
                "id": 7,
                "name": "Cash £ Reconciliation",
                "slug": "cash_%25C2%25A3_reconciliation",
            },
            {
                "id": 8,
                "name": "Cash £ Reconciliation",
                "slug": "cash_%C2%A3_reconciliation",
            },
            {"id": 9, "name": "分析", "slug": "%E5%88%86%E6%9E%90"},
        ]

    def get_collection_items(self, uid: str, models: tuple[str, ...]):
        return [{"id": uid, "model": "card", "name": f"Orders {uid}"}]

    def find_card(self, uid: int):
        return {
            "id": uid,
            "name": "Finance €",
            "description": "Question over orders",
            "display": "table",
            "created_at": "2026-01-01T00:00:00Z",
            "dataset_query": {
                "type": "query",
                "query": {"source-table": 42},
            },
        }


class _UnsafeCollectionCore(_NullDatabaseCore):
    def __init__(self):
        super().__init__()
        self._metabase = _UnsafeCollectionMetabase()


def test_extract_exposures_collection_grouping_uses_safe_identifiers(tmp_path: Path):
    core = _UnsafeCollectionCore()

    exposures = list(
        core.extract_exposures(
            output_path=str(tmp_path),
            output_grouping="collection",
        )
    )

    assert [exposure["body"]["name"] for exposure in exposures] == [
        "finance_euro_sign",
        "finance_euro_sign_1",
        "finance_euro_sign_2",
    ]

    assert (tmp_path / "cash_pound_sign_reconciliation.yml").exists()
    assert (tmp_path / "cash_pound_sign_reconciliation_8.yml").exists()
    assert (tmp_path / f"{safe_identifier('分析')}.yml").exists()
