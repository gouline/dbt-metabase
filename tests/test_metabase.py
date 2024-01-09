# pylint: disable=protected-access

import json
import logging
import unittest
from operator import itemgetter
from pathlib import Path

import yaml

from dbtmetabase.dbt import MetabaseColumn, MetabaseModel, ModelType
from dbtmetabase.metabase import MetabaseClient, _ExportModelsJob, _ExtractExposuresJob

FIXTURES_PATH = Path("tests") / "fixtures"

MODELS = [
    MetabaseModel(
        name="orders",
        schema="PUBLIC",
        description="This table has basic information about orders, as well as some derived facts based on payments",
        model_type=ModelType.nodes,
        columns=[
            MetabaseColumn(
                name="ORDER_ID",
                description="This is a unique identifier for an order",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="CUSTOMER_ID",
                description="Foreign key to the customers table",
                meta_fields={},
                semantic_type="type/FK",
                visibility_type=None,
                fk_target_table="PUBLIC.customers",
                fk_target_field="CUSTOMER_ID",
            ),
            MetabaseColumn(
                name="ORDER_DATE",
                description="Date (UTC) that the order was placed",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="STATUS",
                description="Orders can be one of the following statuses:\n\n| status         | description                                                                                                            |\n|----------------|------------------------------------------------------------------------------------------------------------------------|\n| placed         | The order has been placed but has not yet left the warehouse                                                           |\n| shipped        | The order has ben shipped to the customer and is currently in transit                                                  |\n| completed      | The order has been received by the customer                                                                            |\n| return_pending | The customer has indicated that they would like to return the order, but it has not yet been received at the warehouse |\n| returned       | The order has been returned by the customer and received at the warehouse                                              |",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="AMOUNT",
                description="Total amount (AUD) of the order",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="CREDIT_CARD_AMOUNT",
                description="Amount of the order (AUD) paid for by credit card",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="COUPON_AMOUNT",
                description="Amount of the order (AUD) paid for by coupon",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="BANK_TRANSFER_AMOUNT",
                description="Amount of the order (AUD) paid for by bank transfer",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="GIFT_CARD_AMOUNT",
                description="Amount of the order (AUD) paid for by gift card",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
        ],
    ),
    MetabaseModel(
        name="customers",
        schema="PUBLIC",
        description="This table has basic information about a customer, as well as some derived facts based on a customer's orders",
        model_type=ModelType.nodes,
        columns=[
            MetabaseColumn(
                name="CUSTOMER_ID",
                description="This is a unique identifier for a customer",
                meta_fields={},
                semantic_type="type/FK",
                visibility_type=None,
                fk_target_table="PUBLIC.orders",
                fk_target_field="CUSTOMER_ID",
            ),
            MetabaseColumn(
                name="FIRST_NAME",
                description="Customer's first name. PII.",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="LAST_NAME",
                description="Customer's last name. PII.",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="FIRST_ORDER",
                description="Date (UTC) of a customer's first order",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="MOST_RECENT_ORDER",
                description="Date (UTC) of a customer's most recent order",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="NUMBER_OF_ORDERS",
                description="Count of the number of orders a customer has placed",
                meta_fields={"display_name": "order_count"},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="TOTAL_ORDER_AMOUNT",
                description="Total value (AUD) of a customer's orders",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
        ],
    ),
    MetabaseModel(
        name="stg_orders",
        schema="PUBLIC",
        description="",
        model_type=ModelType.nodes,
        columns=[
            MetabaseColumn(
                name="ORDER_ID",
                description="",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="STATUS",
                description="",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
        ],
    ),
    MetabaseModel(
        name="stg_payments",
        schema="PUBLIC",
        description="",
        model_type=ModelType.nodes,
        columns=[
            MetabaseColumn(
                name="PAYMENT_ID",
                description="",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
            MetabaseColumn(
                name="PAYMENT_METHOD",
                description="",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            ),
        ],
    ),
    MetabaseModel(
        name="stg_customers",
        schema="PUBLIC",
        description="",
        model_type=ModelType.nodes,
        columns=[
            MetabaseColumn(
                name="CUSTOMER_ID",
                description="",
                meta_fields={},
                semantic_type=None,
                visibility_type=None,
                fk_target_table=None,
                fk_target_field=None,
            )
        ],
    ),
]


class MockMetabaseClient(MetabaseClient):
    def api(self, method: str, path: str, critical: bool = True, **kwargs):
        if method == "get":
            json_path = Path.joinpath(
                FIXTURES_PATH, "mock_api", *f"{path.lstrip('/')}.json".split("/")
            )
            if json_path.exists():
                with open(json_path, encoding="utf-8") as f:
                    return json.load(f)
            return {}


class TestMetabaseClient(unittest.TestCase):
    def setUp(self):
        self.client = MockMetabaseClient(
            url="http://localhost:3000",
            session_id="dummy",
        )
        logging.getLogger(__name__)
        logging.basicConfig(level=logging.DEBUG)

    def test_exposures(self):
        output_path = FIXTURES_PATH / "exposure"
        job = _ExtractExposuresJob(
            client=self.client,
            models=MODELS,
            output_path=str(output_path),
            output_grouping=None,
            include_personal_collections=False,
            collection_includes=None,
            collection_excludes=None,
        )
        job.execute()

        with open(output_path / "baseline_test_exposures.yml", encoding="utf-8") as f:
            expected = yaml.safe_load(f)
        with open(output_path / "exposures.yml", encoding="utf-8") as f:
            actual = yaml.safe_load(f)

        expected_exposures = sorted(expected["exposures"], key=itemgetter("name"))
        actual_exposures = sorted(actual["exposures"], key=itemgetter("name"))

        self.assertEqual(expected_exposures, actual_exposures)

    def test_build_lookups(self):
        job = _ExportModelsJob(
            client=self.client,
            database="unit_testing",
            models=[],
            exclude_sources=True,
            sync_timeout=0,
        )

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
        actual_tables = job._load_tables(database_id="2")
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
