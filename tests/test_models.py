import unittest

from ._mocks import MockDbtMetabase


class TestModels(unittest.TestCase):
    def setUp(self):
        # pylint: disable=protected-access
        self.c = MockDbtMetabase()
        self.c._ModelsMixin__SYNC_PERIOD = 1  # type: ignore

    def test_export(self):
        self.c.export_models(
            metabase_database="dbtmetabase",
            skip_sources=True,
            sync_timeout=1,
            order_fields=True,
        )

    def test_export_hidden_table(self):
        # pylint: disable=protected-access
        self.c._manifest.read_models()
        model = self.c._manifest.find_model("stg_customers")
        model.visibility_type = "hidden"

        column = model.columns[0]
        column.name = "new_column_since_stale"
        model.columns.append(column)

        self.c.export_models(
            metabase_database="dbtmetabase",
            skip_sources=True,
            sync_timeout=1,
            order_fields=True,
        )

    def test_build_lookups(self):
        # pylint: disable=protected-access,no-member
        expected = {
            "PUBLIC.CUSTOMERS": [
                "CUSTOMER_ID",
                "FIRST_NAME",
                "LAST_NAME",
                "FIRST_ORDER",
                "MOST_RECENT_ORDER",
                "NUMBER_OF_ORDERS",
                "CUSTOMER_LIFETIME_VALUE",
            ],
            "PUBLIC.ORDERS": [
                "ORDER_ID",
                "CUSTOMER_ID",
                "ORDER_DATE",
                "STATUS",
                "AMOUNT",
                "CREDIT_CARD_AMOUNT",
                "COUPON_AMOUNT",
                "BANK_TRANSFER_AMOUNT",
                "GIFT_CARD_AMOUNT",
            ],
            "PUBLIC.RAW_CUSTOMERS": ["ID", "FIRST_NAME", "LAST_NAME"],
            "PUBLIC.RAW_ORDERS": ["ID", "USER_ID", "ORDER_DATE", "STATUS"],
            "PUBLIC.RAW_PAYMENTS": ["ID", "ORDER_ID", "PAYMENT_METHOD", "AMOUNT"],
            "PUBLIC.STG_CUSTOMERS": ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME"],
            "PUBLIC.STG_ORDERS": ["ORDER_ID", "STATUS", "ORDER_DATE", "CUSTOMER_ID"],
            "PUBLIC.STG_PAYMENTS": [
                "PAYMENT_ID",
                "PAYMENT_METHOD",
                "ORDER_ID",
                "AMOUNT",
            ],
        }

        actual_tables = self.c._ModelsMixin__get_tables(database_id="2")  # type: ignore

        self.assertEqual(list(expected.keys()), list(actual_tables.keys()))

        for table, columns in expected.items():
            self.assertEqual(columns, list(actual_tables[table]["fields"].keys()))
