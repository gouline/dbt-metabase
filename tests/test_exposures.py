from operator import itemgetter
from pathlib import Path

import pytest
import yaml

from dbtmetabase._exposures import _Context, _Exposure
from tests._mocks import FIXTURES_PATH, TMP_PATH, MockDbtMetabase


def _assert_exposures(expected_path: Path, actual_path: Path):
    with open(expected_path, encoding="utf-8") as f:
        expected = yaml.safe_load(f)
    with open(actual_path, encoding="utf-8") as f:
        actual = yaml.safe_load(f)

    assert actual["exposures"] == sorted(expected["exposures"], key=itemgetter("name"))


def test_exposures_default(core: MockDbtMetabase):
    fixtures_path = FIXTURES_PATH / "exposure" / "default"
    output_path = TMP_PATH / "exposure" / "default"
    core.extract_exposures(
        output_path=str(output_path),
        output_grouping=None,
        tags=["metabase"],
    )

    _assert_exposures(
        fixtures_path / "exposures.yml",
        output_path / "exposures.yml",
    )


def test_exposures_collection_grouping(core: MockDbtMetabase):
    fixtures_path = FIXTURES_PATH / "exposure" / "collection"
    output_path = TMP_PATH / "exposure" / "collection"
    core.extract_exposures(
        output_path=str(output_path),
        output_grouping="collection",
    )

    for file in fixtures_path.iterdir():
        _assert_exposures(file, output_path / file.name)


def test_exposures_grouping_type(core: MockDbtMetabase):
    fixtures_path = FIXTURES_PATH / "exposure" / "type"
    output_path = TMP_PATH / "exposure" / "type"
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
    core._exposure_card(
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


def test_mbql5_query_basic(core: MockDbtMetabase):
    """Test MBQL 5 query with a simple source-table (34)."""

    ctx = _Context(
        model_refs={
            "database.public.orders": "ref('orders')",
        },
        database_names={2: "database"},
        table_names={10: "database.public.orders"},
    )
    exposure = _Exposure(
        model="card",
        uid="34",
        label="Orders Count by Month (MBQL 5)",
    )
    card = core.metabase.find_card("34")
    assert card is not None
    core._exposure_card(ctx=ctx, exposure=exposure, card=card)
    assert {"database.public.orders"} == exposure.depends


def test_mbql5_query_with_source_card(core: MockDbtMetabase):
    """Test MBQL 5 query that references another card via source-card (38 -> 34)."""

    ctx = _Context(
        model_refs={
            "database.public.orders": "ref('orders')",
        },
        database_names={2: "database"},
        table_names={10: "database.public.orders"},
    )
    exposure = _Exposure(
        model="card",
        uid="38",
        label="Orders Filtered (MBQL 5 source-card)",
    )
    # Card 38 references card 34 via source-card, which references table 10
    card = core.metabase.find_card("38")
    assert card is not None
    core._exposure_card(ctx=ctx, exposure=exposure, card=card)
    assert {"database.public.orders"} == exposure.depends


def test_mbql5_query_with_joins(core: MockDbtMetabase):
    """Test MBQL 5 query with joins (35)."""

    ctx = _Context(
        model_refs={
            "database.public.orders": "ref('orders')",
            "database.public.customers": "ref('customers')",
        },
        database_names={2: "database"},
        table_names={
            10: "database.public.orders",
            12: "database.public.customers",
        },
    )
    exposure = _Exposure(
        model="card",
        uid="35",
        label="Orders + Customers (MBQL 5)",
    )
    card = core.metabase.find_card("35")
    assert card is not None
    core._exposure_card(ctx=ctx, exposure=exposure, card=card)
    assert {"database.public.orders", "database.public.customers"} == exposure.depends


def test_mbql5_native_query(core: MockDbtMetabase):
    """Test MBQL 5 native (SQL) query stage (36)."""

    ctx = _Context(
        model_refs={
            "database.public.orders": "ref('orders')",
        },
        database_names={2: "database"},
        table_names={},
    )
    exposure = _Exposure(
        model="card",
        uid="36",
        label="Orders SQL (MBQL 5)",
    )
    card = core.metabase.find_card("36")
    assert card is not None
    core._exposure_card(ctx=ctx, exposure=exposure, card=card)
    assert {"database.public.orders"} == exposure.depends
    assert (
        "SELECT * FROM public.orders WHERE status = 'completed'"
        == exposure.native_query
    )


def test_mbql5_multi_stage_query(core: MockDbtMetabase):
    """Test MBQL 5 query with multiple stages (card 37)."""
    ctx = _Context(
        model_refs={
            "database.public.orders": "ref('orders')",
        },
        database_names={2: "database"},
        table_names={10: "database.public.orders"},
    )
    exposure = _Exposure(
        model="card",
        uid="37",
        label="High Volume Orders (MBQL 5 Multi-Stage)",
    )
    card = core.metabase.find_card("37")
    assert card is not None
    core._exposure_card(ctx=ctx, exposure=exposure, card=card)
    assert {"database.public.orders"} == exposure.depends
