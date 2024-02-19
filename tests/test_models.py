import unittest

from ._mocks import MockDbtMetabase


class TestModels(unittest.TestCase):
    def setUp(self):
        # pylint: disable=protected-access
        self.c = MockDbtMetabase()
        self.c._ModelsMixin__SYNC_PERIOD = 1  # type: ignore

    def test_export(self):
        self.c.export_models(
            metabase_database="unit_testing",
            skip_sources=True,
            sync_timeout=0,
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
                "CREDIT_CARD_AMOUNT",
                "COUPON_AMOUNT",
                "BANK_TRANSFER_AMOUNT",
                "GIFT_CARD_AMOUNT",
                "AMOUNT",
            ],
            "PUBLIC.RAW_CUSTOMERS": ["ID", "FIRST_NAME", "LAST_NAME"],
            "PUBLIC.RAW_ORDERS": ["ID", "USER_ID", "ORDER_DATE", "STATUS"],
            "PUBLIC.RAW_PAYMENTS": ["ID", "ORDER_ID", "PAYMENT_METHOD", "AMOUNT"],
            "PUBLIC.STG_CUSTOMERS": ["CUSTOMER_ID"],
            "PUBLIC.STG_ORDERS": ["ORDER_ID", "STATUS"],
            "PUBLIC.STG_PAYMENTS": ["PAYMENT_ID", "PAYMENT_METHOD"],
        }

        actual_tables = self.c._ModelsMixin__get_tables(database_id="2")  # type: ignore

        self.assertEqual(list(expected.keys()), list(actual_tables.keys()))

        for table, columns in expected.items():
            self.assertEqual(columns, list(actual_tables[table]["fields"].keys()))
