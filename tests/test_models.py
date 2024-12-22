from typing import MutableSequence, cast

from dbtmetabase.manifest import Column
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
