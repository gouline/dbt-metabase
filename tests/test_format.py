from dbtmetabase.format import Filter, NullValue, dump_yaml, safe_description, safe_name
from tests._mocks import FIXTURES_PATH, TMP_PATH


def test_filter():
    assert Filter(include=("alpHa", "bRavo")).match("Alpha")
    assert Filter().match("Alpha")
    assert Filter().match("")
    assert not Filter(include=("alpHa", "bRavo"), exclude=("alpha",)).match("Alpha")
    assert not Filter(exclude=("alpha",)).match("Alpha")
    assert Filter(include="alpha").match("Alpha")
    assert not Filter(exclude="alpha").match("Alpha")


def test_filter_wildcard():
    assert Filter(include="stg_*").match("stg_orders")
    assert Filter(include="STG_*").match("stg_ORDERS")
    assert not Filter(include="stg_*").match("orders")
    assert Filter(include="order?").match("orders")
    assert not Filter(include="order?").match("ordersz")
    assert Filter(include="*orders", exclude="stg_*").match("_orders")
    assert not Filter(include="*orders", exclude="stg_*").match("stg_orders")


def test_null_value():
    assert NullValue is not None
    assert not NullValue
    assert NullValue is NullValue


def test_safe_name():
    assert safe_name("Somebody's 2 collections!") == "somebody_s_2_collections_"
    assert safe_name("somebody_s_2_collections_") == "somebody_s_2_collections_"
    assert safe_name("") == ""


def test_safe_description():
    assert (
        safe_description("Depends on\n\nQuestion {{ #2 }}!")
        == "Depends on\n\nQuestion ( #2 )!"
    )
    assert (
        safe_description("Depends on\n\nQuestion ( #2 )!")
        == "Depends on\n\nQuestion ( #2 )!"
    )
    assert (
        safe_description("Depends on\n\nQuestion { #2 }!")
        == "Depends on\n\nQuestion { #2 }!"
    )
    assert (
        safe_description("{{start_date}} - cast({{rolling_days}})")
        == "(start_date) - cast((rolling_days))"
    )


def test_dump_yaml():
    fixture_path = FIXTURES_PATH / "test_dump_yaml.yml"
    output_path = TMP_PATH / "test_dump_yaml.yml"
    with open(output_path, "w", encoding="utf-8") as f:
        dump_yaml(
            data={
                "root": {
                    "attr1": "val1\nend",
                    "attr2": ["val2", "val3"],
                },
            },
            stream=f,
        )
    with open(output_path, "r", encoding="utf-8") as f:
        actual = f.read()
    with open(fixture_path, "r", encoding="utf-8") as f:
        expected = f.read()
    assert actual == expected
