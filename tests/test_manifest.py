import unittest
from typing import Optional, Sequence

from dbtmetabase.manifest import Column, Group, Manifest, Model

from ._mocks import FIXTURES_PATH


class TestManifest(unittest.TestCase):
    def test_v11_disabled(self):
        models = Manifest(FIXTURES_PATH / "manifest-v11-disabled.json").read_models()

        orders_mod = self._find_model(models, "orders")
        self.assertIsNone(orders_mod)

        customer_id_col = self._find_column(models, "customers", "customer_id")
        assert customer_id_col
        self.assertIsNone(customer_id_col.fk_target_table)
        self.assertIsNone(customer_id_col.fk_target_field)

    def test_v11(self):
        models = Manifest(FIXTURES_PATH / "manifest-v11.json").read_models()
        self.assertEqual(
            models,
            [
                Model(
                    database="dbtmetabase",
                    schema="public",
                    group=Group.nodes,
                    name="customers",
                    description="This table has basic information about a customer, as well as some derived facts based on a customer's orders",
                    display_name="clients",
                    unique_id="model.sandbox.customers",
                    columns=[
                        Column(
                            name="customer_id",
                            description="This is a unique identifier for a customer",
                        ),
                        Column(
                            name="first_name",
                            description="Customer's first name. PII.",
                        ),
                        Column(
                            name="last_name",
                            description="Customer's last name. PII.",
                        ),
                        Column(
                            name="first_order",
                            description="Date (UTC) of a customer's first order",
                        ),
                        Column(
                            name="most_recent_order",
                            description="Date (UTC) of a customer's most recent order",
                        ),
                        Column(
                            name="number_of_orders",
                            description="Count of the number of orders a customer has placed",
                            display_name="order_count",
                        ),
                        Column(
                            name="customer_lifetime_value",
                            description="Total value (AUD) of a customer's orders",
                        ),
                    ],
                ),
                Model(
                    database="dbtmetabase",
                    schema="public",
                    group=Group.nodes,
                    name="orders",
                    description="This table has basic information about orders, as well as some derived facts based on payments",
                    points_of_interest="Basic information only",
                    caveats="Some facts are derived from payments",
                    unique_id="model.sandbox.orders",
                    columns=[
                        Column(
                            name="order_id",
                            description="This is a unique identifier for an order",
                        ),
                        Column(
                            name="customer_id",
                            description="Foreign key to the customers table",
                            semantic_type="type/FK",
                            fk_target_table="public.customers",
                            fk_target_field="customer_id",
                        ),
                        Column(
                            name="order_date",
                            description="Date (UTC) that the order was placed",
                        ),
                        Column(
                            name="status",
                            description="",
                        ),
                        Column(
                            name="amount",
                            description="Total amount (AUD) of the order",
                        ),
                        Column(
                            name="credit_card_amount",
                            description="Amount of the order (AUD) paid for by credit card",
                        ),
                        Column(
                            name="coupon_amount",
                            description="Amount of the order (AUD) paid for by coupon",
                        ),
                        Column(
                            name="bank_transfer_amount",
                            description="Amount of the order (AUD) paid for by bank transfer",
                        ),
                        Column(
                            name="gift_card_amount",
                            description="Amount of the order (AUD) paid for by gift card",
                        ),
                    ],
                ),
                Model(
                    database="dbtmetabase",
                    schema="public",
                    group=Group.nodes,
                    name="stg_customers",
                    description="",
                    unique_id="model.sandbox.stg_customers",
                    columns=[
                        Column(
                            name="customer_id",
                            description="",
                        )
                    ],
                ),
                Model(
                    database="dbtmetabase",
                    schema="public",
                    group=Group.nodes,
                    name="stg_payments",
                    description="",
                    unique_id="model.sandbox.stg_payments",
                    columns=[
                        Column(
                            name="payment_id",
                            description="",
                        ),
                        Column(
                            name="payment_method",
                            description="",
                        ),
                    ],
                ),
                Model(
                    database="dbtmetabase",
                    schema="public",
                    group=Group.nodes,
                    name="stg_orders",
                    description="",
                    unique_id="model.sandbox.stg_orders",
                    columns=[
                        Column(
                            name="order_id",
                            description="",
                        ),
                        Column(
                            name="status",
                            description="",
                        ),
                    ],
                ),
            ],
        )

    def test_v2(self):
        models = Manifest(FIXTURES_PATH / "manifest-v2.json").read_models()
        self.assertEqual(
            models,
            [
                Model(
                    database="test",
                    schema="public",
                    group=Group.nodes,
                    name="orders",
                    description="This table has basic information about orders, as well as some derived facts based on payments",
                    unique_id="model.jaffle_shop.orders",
                    columns=[
                        Column(
                            name="order_id",
                            description="This is a unique identifier for an order",
                        ),
                        Column(
                            name="customer_id",
                            description="Foreign key to the customers table",
                            semantic_type="type/FK",
                            fk_target_table="public.customers",
                            fk_target_field="customer_id",
                        ),
                        Column(
                            name="order_date",
                            description="Date (UTC) that the order was placed",
                        ),
                        Column(
                            name="status",
                            description="Orders can be one of the following statuses:\n\n| status         | description                                                                                                            |\n|----------------|------------------------------------------------------------------------------------------------------------------------|\n| placed         | The order has been placed but has not yet left the warehouse                                                           |\n| shipped        | The order has ben shipped to the customer and is currently in transit                                                  |\n| completed      | The order has been received by the customer                                                                            |\n| return_pending | The customer has indicated that they would like to return the order, but it has not yet been received at the warehouse |\n| returned       | The order has been returned by the customer and received at the warehouse                                              |",
                        ),
                        Column(
                            name="amount",
                            description="Total amount (AUD) of the order",
                        ),
                        Column(
                            name="credit_card_amount",
                            description="Amount of the order (AUD) paid for by credit card",
                        ),
                        Column(
                            name="coupon_amount",
                            description="Amount of the order (AUD) paid for by coupon",
                        ),
                        Column(
                            name="bank_transfer_amount",
                            description="Amount of the order (AUD) paid for by bank transfer",
                        ),
                        Column(
                            name="gift_card_amount",
                            description="Amount of the order (AUD) paid for by gift card",
                        ),
                    ],
                ),
                Model(
                    database="test",
                    schema="public",
                    group=Group.nodes,
                    name="customers",
                    description="This table has basic information about a customer, as well as some derived facts based on a customer's orders",
                    unique_id="model.jaffle_shop.customers",
                    columns=[
                        Column(
                            name="customer_id",
                            description="This is a unique identifier for a customer",
                            semantic_type=None,  # This is a PK field, should not be detected as FK
                        ),
                        Column(
                            name="first_name",
                            description="Customer's first name. PII.",
                        ),
                        Column(
                            name="last_name",
                            description="Customer's last name. PII.",
                        ),
                        Column(
                            name="first_order",
                            description="Date (UTC) of a customer's first order",
                        ),
                        Column(
                            name="most_recent_order",
                            description="Date (UTC) of a customer's most recent order",
                        ),
                        Column(
                            name="number_of_orders",
                            description="Count of the number of orders a customer has placed",
                        ),
                        Column(
                            name="customer_lifetime_value",
                            description="Total value (AUD) of a customer's orders",
                        ),
                    ],
                ),
                Model(
                    database="test",
                    schema="public",
                    group=Group.nodes,
                    name="stg_orders",
                    description="",
                    unique_id="model.jaffle_shop.stg_orders",
                    columns=[
                        Column(
                            name="order_id",
                            description="",
                        ),
                        Column(
                            name="status",
                            description="",
                        ),
                    ],
                ),
                Model(
                    database="test",
                    schema="public",
                    group=Group.nodes,
                    name="stg_payments",
                    description="",
                    unique_id="model.jaffle_shop.stg_payments",
                    columns=[
                        Column(
                            name="payment_id",
                            description="",
                        ),
                        Column(
                            name="payment_method",
                            description="",
                        ),
                    ],
                ),
                Model(
                    database="test",
                    schema="public",
                    group=Group.nodes,
                    name="stg_customers",
                    description="",
                    unique_id="model.jaffle_shop.stg_customers",
                    tags=[],
                    columns=[
                        Column(
                            name="customer_id",
                            description="",
                        )
                    ],
                ),
            ],
        )

    @staticmethod
    def _find_model(models: Sequence[Model], model_name: str) -> Optional[Model]:
        filtered = [m for m in models if m.name == model_name]
        if filtered:
            return filtered[0]
        return None

    @staticmethod
    def _find_column(
        models: Sequence[Model],
        model_name: str,
        column_name: str,
    ) -> Optional[Column]:
        model = TestManifest._find_model(models=models, model_name=model_name)
        if model:
            filtered = [c for c in model.columns if c.name == column_name]
            if filtered:
                return filtered[0]
        return None
