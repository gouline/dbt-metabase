from collections.abc import MutableSequence
from typing import cast
from unittest.mock import Mock

from dbtmetabase._models import _Context
from dbtmetabase.manifest import Column, Group, Model
from tests._mocks import MockDbtMetabase


def test_export(core: MockDbtMetabase):
    core.export_models(
        metabase_database="dbtmetabase",
        skip_sources=True,
        sync_timeout=1,
        order_fields=True,
    )


def test_export_hidden_table(core: MockDbtMetabase):
    core._manifest.read_models()
    model = core._manifest.find_model("stg_customers")
    assert model is not None
    model.visibility_type = "hidden"

    column = model.columns[0]
    column.name = "new_column_since_stale"
    columns = cast(MutableSequence[Column], model.columns)
    columns.append(column)

    core.export_models(
        metabase_database="dbtmetabase",
        skip_sources=True,
        sync_timeout=1,
        order_fields=True,
    )


def test_build_lookups(core: MockDbtMetabase):
    expected = {
        "PUBLIC.CUSTOMERS": {
            "CUSTOMER_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "FIRST_ORDER",
            "MOST_RECENT_ORDER",
            "NUMBER_OF_ORDERS",
            "CUSTOMER_LIFETIME_VALUE",
        },
        "PUBLIC.ORDERS": {
            "ORDER_ID",
            "CUSTOMER_ID",
            "ORDER_DATE",
            "STATUS",
            "AMOUNT",
            "CREDIT_CARD_AMOUNT",
            "COUPON_AMOUNT",
            "BANK_TRANSFER_AMOUNT",
            "GIFT_CARD_AMOUNT",
        },
        "PUBLIC.TRANSACTIONS": {
            "PAYMENT_ID",
            "PAYMENT_METHOD",
            "ORDER_ID",
            "AMOUNT",
        },
        "PUBLIC.RAW_CUSTOMERS": {"ID", "FIRST_NAME", "LAST_NAME"},
        "PUBLIC.RAW_ORDERS": {"ID", "USER_ID", "ORDER_DATE", "STATUS"},
        "PUBLIC.RAW_PAYMENTS": {"ID", "ORDER_ID", "PAYMENT_METHOD", "AMOUNT"},
        "PUBLIC.STG_CUSTOMERS": {"CUSTOMER_ID", "FIRST_NAME", "LAST_NAME"},
        "PUBLIC.STG_ORDERS": {
            "ORDER_ID",
            "STATUS",
            "ORDER_DATE",
            "CUSTOMER_ID",
            "SKU_ID",
        },
        "PUBLIC.STG_PAYMENTS": {
            "PAYMENT_ID",
            "PAYMENT_METHOD",
            "ORDER_ID",
            "AMOUNT",
        },
        "INVENTORY.SKUS": {"SKU_ID", "PRODUCT"},
    }

    actual_tables = core._get_metabase_tables(database_id="2")

    assert set(actual_tables.keys()) == set(expected.keys())

    for table, columns in expected.items():
        assert set(actual_tables[table]["fields"].keys()) == columns, f"table: {table}"


def test_column_decimals_zero(core: MockDbtMetabase):
    """Test that decimals=0 is sent to Metabase API."""
    core._manifest.read_models()
    column = core._manifest.find_column("customers", "customer_id")
    assert column is not None

    column.decimals = 0
    column.number_style = "decimal"

    core.export_models(
        metabase_database="dbtmetabase",
        skip_sources=True,
        sync_timeout=1,
        order_fields=False,
    )

    found = False
    for call in core._metabase.api_calls:
        if call["method"] == "put" and "json" in call["kwargs"]:
            data = call["kwargs"]["json"]
            if data.get("settings", {}).get("decimals") == 0:
                found = True
                break

    assert found, "decimals=0 should be in API call"


def test_multi_database_get_tables(core: MockDbtMetabase):
    """Test _get_metabase_tables handles multi-database format."""

    mock_metabase = Mock()
    mock_metabase.get_database_metadata.return_value = {
        "tables": [
            {
                "id": 1,
                "name": "my_model",
                "schema": "bronze",
                "db": "my_database.bronze",  # Multi-database format
                "fields": [{"id": 1, "name": "id"}, {"id": 2, "name": "name"}],
            },
            {
                "id": 2,
                "name": "other_model",
                "schema": "silver",
                "db": "my_database.silver",  # Multi-database format
                "fields": [{"id": 3, "name": "id"}, {"id": 4, "name": "value"}],
            },
            {
                "id": 3,
                "name": "legacy_model",
                "schema": "public",
                "db": "legacy_db",  # Non multi-database format
                "fields": [{"id": 5, "name": "id"}],
            },
        ]
    }

    core._metabase = mock_metabase

    # Test the method
    tables = core._get_metabase_tables("test_db_id")

    # Verify multi-database tables are keyed correctly
    assert "MY_DATABASE.BRONZE.MY_MODEL" in tables
    assert "MY_DATABASE.SILVER.OTHER_MODEL" in tables

    # Verify regular database format works (database prefix)
    assert "LEGACY_DB.PUBLIC.LEGACY_MODEL" in tables

    # Verify table structure is preserved
    bronze_table = tables["MY_DATABASE.BRONZE.MY_MODEL"]
    assert bronze_table["name"] == "my_model"
    assert bronze_table["schema"] == "BRONZE"
    assert "ID" in bronze_table["fields"]
    assert "NAME" in bronze_table["fields"]


def test_multi_database_model_matching():
    """Test that dbt models match correctly with multi-database table keys."""

    # Mock tables as they would be returned by _get_metabase_tables
    mock_tables = {
        "MY_DATABASE.BRONZE.MY_MODEL": {
            "id": 1,
            "name": "my_model",
            "schema": "BRONZE",
            "fields": {"ID": {"id": 1, "name": "id"}},
        },
        "LEGACY_DB.PUBLIC.LEGACY_MODEL": {
            "id": 2,
            "name": "legacy_model",
            "schema": "PUBLIC",
            "fields": {"ID": {"id": 2, "name": "id"}},
        },
    }

    # Test matching logic (simulating the sync loop logic)
    for model in [
        Model(
            database="my_database",
            schema="bronze",
            group=Group.nodes,
            name="my_model",
            alias="my_model",
            columns=[Column(name="id")],
        ),
        Model(
            database="legacy_db",  # Regular database
            schema="public",
            group=Group.nodes,
            name="legacy_model",
            alias="legacy_model",
            columns=[Column(name="id")],
        ),
    ]:
        schema_name = model.schema.upper()
        model_name = model.alias.upper()
        database_name = model.database.upper() if model.database else ""

        # Try multi-database format first
        table_key = (
            f"{database_name}.{schema_name}.{model_name}"
            if database_name
            else f"{schema_name}.{model_name}"
        )
        table = mock_tables.get(table_key)

        # Fallback to schema.table format if multi-database format not found
        if not table and database_name:
            table_key = f"{schema_name}.{model_name}"
            table = mock_tables.get(table_key)

        # Verify matching works
        assert table is not None, f"Model {model.name} should match a table"

        if model.database:
            # Multi-database
            assert table_key == f"{database_name}.{schema_name}.{model_name}"
        else:
            # Single-database
            assert table_key == f"{schema_name}.{model_name}"


def test_multi_database_foreign_key_resolution():
    """Test foreign key resolution works with multi-database table references."""

    ctx = _Context()
    ctx.tables = {
        "MY_DATABASE.BRONZE.USERS": {"fields": {"ID": {"id": 1, "name": "id"}}},
        "MY_DATABASE.SILVER.ORDERS": {
            "fields": {"USER_ID": {"id": 2, "name": "user_id"}}
        },
    }

    field = ctx.get_field("MY_DATABASE.BRONZE.USERS", "ID")
    assert field["id"] == 1

    field = ctx.get_field("MY_DATABASE.SILVER.ORDERS", "USER_ID")
    assert field["id"] == 2
