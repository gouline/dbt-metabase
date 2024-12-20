from tests._mocks import MockMetabase


def test_metabase_find_database(metabase: MockMetabase):
    db = metabase.find_database(name="dbtmetabase")
    assert db
    assert db["id"] == 2
    assert metabase.find_database(name="foo") is None


def test_metabase_get_collections(metabase: MockMetabase):
    excluded = metabase.get_collections(exclude_personal=True)
    assert len(excluded) == 2

    included = metabase.get_collections(exclude_personal=False)
    assert len(included) == 3


def test_metabase_get_collection_items(metabase: MockMetabase):
    cards = metabase.get_collection_items(
        uid="root",
        models=("card",),
    )
    assert {item["model"] for item in cards} == {"card"}

    dashboards = metabase.get_collection_items(
        uid="root",
        models=("dashboard",),
    )
    assert {item["model"] for item in dashboards} == {"dashboard"}

    both = metabase.get_collection_items(
        uid="root",
        models=("card", "dashboard"),
    )
    assert {item["model"] for item in both} == {"card", "dashboard"}
