from operator import attrgetter
from typing import Sequence

from dbtmetabase.manifest import Column, Group, Manifest, Model
from tests._mocks import FIXTURES_PATH, MockManifest


def test_v11_disabled():
    manifest = MockManifest(FIXTURES_PATH / "manifest-v11-disabled.json")
    manifest.read_models()

    orders_mod = manifest.find_model("orders")
    assert orders_mod is None

    customer_id_col = manifest.find_column("customers", "customer_id")
    assert customer_id_col is not None
    assert customer_id_col.fk_target_table is None
    assert customer_id_col.fk_target_field is None


def test_v12():
    models = Manifest(FIXTURES_PATH / "manifest-v12.json").read_models()
    _assert_models_equal(
        models,
        [
            Model(
                database="dbtmetabase",
                schema="public",
                group=Group.nodes,
                name="payments",
                alias="transactions",
                description="This table has basic information about payments",
                unique_id="model.sandbox.payments",
                columns=[
                    Column(
                        name="payment_id",
                        description="This is a unique identifier for a payment",
                        semantic_type="type/PK",
                    ),
                    Column(
                        name="payment_method",
                        description="",
                    ),
                    Column(
                        name="order_id",
                        description="Foreign key to the orders table",
                        semantic_type="type/FK",
                        fk_target_table="public.orders",
                        fk_target_field="order_id",
                    ),
                    Column(
                        name="amount",
                        description="",
                    ),
                ],
            ),
            Model(
                database="dbtmetabase",
                schema="public",
                group=Group.nodes,
                name="customers",
                alias="customers",
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
                alias="orders",
                description="This table has basic information about orders, as well as some derived facts based on payments",
                points_of_interest="Basic information only",
                caveats="Some facts are derived from payments",
                unique_id="model.sandbox.orders",
                columns=[
                    Column(
                        name="order_id",
                        description="This is a unique identifier for an order",
                        semantic_type="type/PK",
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
                alias="stg_customers",
                description="",
                unique_id="model.sandbox.stg_customers",
                visibility_type="hidden",
                columns=[
                    Column(
                        name="customer_id",
                        description="",
                    ),
                    Column(
                        name="first_name",
                        description="",
                    ),
                    Column(
                        name="last_name",
                        description="",
                    ),
                ],
            ),
            Model(
                database="dbtmetabase",
                schema="public",
                group=Group.nodes,
                name="stg_payments",
                alias="stg_payments",
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
                    Column(
                        name="order_id",
                        description="",
                    ),
                    Column(
                        name="amount",
                        description="",
                    ),
                ],
            ),
            Model(
                database="dbtmetabase",
                schema="public",
                group=Group.nodes,
                name="stg_orders",
                alias="stg_orders",
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
                    Column(
                        name="order_date",
                        description="",
                    ),
                    Column(
                        name="customer_id",
                        description="",
                    ),
                    Column(
                        name="sku_id",
                        description="",
                        semantic_type="type/FK",
                        fk_target_table="inventory.skus",
                        fk_target_field="sku_id",
                    ),
                ],
            ),
            Model(
                database="dbtmetabase",
                schema="inventory",
                group=Group.sources,
                name="skus",
                alias="skus",
                description="",
                unique_id="source.sandbox.inventory.skus",
                source="inventory",
            ),
        ],
    )


def test_v2():
    models = Manifest(FIXTURES_PATH / "manifest-v2.json").read_models()
    _assert_models_equal(
        models,
        [
            Model(
                database="test",
                schema="public",
                group=Group.nodes,
                name="orders",
                alias="orders",
                description="Basic and derived order information from payments",
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
                        description="Order status: placed, shipped, completed, return_pending, or returned.",
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
                alias="customers",
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
                alias="stg_orders",
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
                alias="stg_payments",
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
                alias="stg_customers",
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


def _assert_models_equal(
    first: Sequence[Model],
    second: Sequence[Model],
):
    assert len(first) == len(second), "mismatched model count"

    first = sorted(first, key=attrgetter("name"))
    second = sorted(second, key=attrgetter("name"))

    for i, first_model in enumerate(first):
        second_model = second[i]
        assert first_model.name == second_model.name, "wrong model"
        assert len(first_model.columns) == len(second_model.columns), (
            f"mismatched column count in {first_model.name}"
        )
        for j, first_column in enumerate(first_model.columns):
            second_column = second_model.columns[j]
            assert first_column.name == second_column.name, (
                f"wrong column in model {first_model.name}"
            )
            assert first_column == second_column, (
                f"mismatched column {first_model.name}.{first_column.name}"
            )
        assert first_model == second_model, f"mismatched model {first_model.name}"

    assert first == second
