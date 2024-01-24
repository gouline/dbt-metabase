# pylint: disable=protected-access,no-member

from ._core import TestCore


class TestModels(TestCore):
    def setUp(self):
        super().setUp()
        self.c._ModelsMixin__SYNC_PERIOD = 1  # type: ignore

    def test_export(self):
        self.c.export_models(
            metabase_database="unit_testing",
            skip_sources=True,
            sync_timeout=0,
        )

    def test_build_lookups(self):
        expected_tables = [
            "PUBLIC.CUSTOMERS",
            "PUBLIC.ORDERS",
            "PUBLIC.RAW_CUSTOMERS",
            "PUBLIC.RAW_ORDERS",
            "PUBLIC.RAW_PAYMENTS",
            "PUBLIC.STG_CUSTOMERS",
            "PUBLIC.STG_ORDERS",
            "PUBLIC.STG_PAYMENTS",
        ]
        actual_tables = self.c._ModelsMixin__get_tables(database_id="2")  # type: ignore
        self.assertEqual(expected_tables, list(actual_tables.keys()))

        expected_columns = [
            [
                "CUSTOMER_ID",
                "FIRST_NAME",
                "LAST_NAME",
                "FIRST_ORDER",
                "MOST_RECENT_ORDER",
                "NUMBER_OF_ORDERS",
                "CUSTOMER_LIFETIME_VALUE",
            ],
            [
                "ORDER_ID",
                "CUSTOMER_ID",
                "ORDER_DATE",
                "STATUS",
                "CREDIT_CARD_AMOUNT",
                "COUPON_AMOUNT",
                "BANK_TRANSFER_AMOUNT",
                "GIFT_CARD_AMOUNT",
                "AMOUNT",
            ],
            ["ID", "FIRST_NAME", "LAST_NAME"],
            ["ID", "USER_ID", "ORDER_DATE", "STATUS"],
            ["ID", "ORDER_ID", "PAYMENT_METHOD", "AMOUNT"],
            ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME"],
            ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS"],
            ["PAYMENT_ID", "ORDER_ID", "PAYMENT_METHOD", "AMOUNT"],
        ]
        for table, columns in zip(expected_tables, expected_columns):
            self.assertEqual(columns, list(actual_tables[table]["fields"].keys()))
