#!/usr/bin/env python
import time

import requests
from molot import envarg, envarg_int, evaluate, shell, target

POSTGRES_HOST = envarg("POSTGRES_HOST")
POSTGRES_PORT = envarg_int("POSTGRES_PORT")
POSTGRES_DB = envarg("POSTGRES_DB")
POSTGRES_USER = envarg("POSTGRES_USER")
POSTGRES_PASSWORD = envarg("POSTGRES_PASSWORD")
MB_HOST = envarg("MB_HOST")
MB_PORT = envarg_int("MB_PORT")
MB_SETUP_TOKEN = envarg("MB_SETUP_TOKEN")
MB_USER = envarg("MB_USER")
MB_PASSWORD = envarg("MB_PASSWORD")
MB_NAME = envarg("MB_NAME", "dbtmetabase")


@target(
    description="initial setup",
    depends=["metabase_setup", "dbt_run", "metabase_content"],
)
def init():
    pass


@target(description="run dbt project")
def dbt_run():
    shell("dbt seed --profiles-dir .")
    shell("dbt run --profiles-dir .")


@target(description="set up Metabase user and database")
def metabase_setup():
    requests.post(
        url=f"http://{MB_HOST}:{MB_PORT}/api/setup",
        json={
            "token": MB_SETUP_TOKEN,
            "user": {
                "site_name": MB_NAME,
                "first_name": MB_NAME,
                "last_name": None,
                "email": MB_USER,
                "password_confirm": MB_PASSWORD,
                "password": MB_PASSWORD,
            },
            "database": {
                "engine": "postgres",
                "name": POSTGRES_DB,
                "details": {
                    "host": POSTGRES_HOST,
                    "port": POSTGRES_PORT,
                    "dbname": POSTGRES_DB,
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD,
                    "schema-filters-type": "all",
                    "ssl": False,
                    "tunnel-enabled": False,
                    "advanced-options": False,
                },
                "is_on_demand": False,
                "is_full_sync": True,
                "is_sample": False,
                "cache_ttl": None,
                "refingerprint": False,
                "auto_run_queries": True,
                "schedules": {},
            },
            "prefs": {
                "site_name": MB_NAME,
                "site_locale": "en",
                "allow_tracking": "false",
            },
        },
        timeout=10,
    ).raise_for_status()


@target(description="add mock content to Metabase")
def metabase_content():
    session_id = requests.post(
        url=f"http://{MB_HOST}:{MB_PORT}/api/session",
        json={"username": MB_USER, "password": MB_PASSWORD},
        timeout=10,
    ).json()["id"]

    headers = {"X-Metabase-Session": session_id}

    database_id = ""
    databases = requests.get(
        url=f"http://{MB_HOST}:{MB_PORT}/api/database",
        headers=headers,
        json={},
        timeout=10,
    ).json()["data"]
    for db in databases:
        if db["name"] == POSTGRES_DB:
            database_id = db["id"]
            break

    requests.post(
        url=f"http://{MB_HOST}:{MB_PORT}/api/database/{database_id}/sync_schema",
        headers=headers,
        json={},
        timeout=10,
    )

    time.sleep(5)

    tables_fields = requests.get(
        url=f"http://{MB_HOST}:{MB_PORT}/api/database/{database_id}?include=tables.fields",
        headers=headers,
        timeout=10,
    ).json()

    customers_table_id = ""
    first_order_field_id = ""
    for table in tables_fields["tables"]:
        if table["name"] == "customers":
            customers_table_id = table["id"]
            for field in table["fields"]:
                if field["name"] == "first_order":
                    first_order_field_id = field["id"]
                    break

    requests.post(
        url=f"http://{MB_HOST}:{MB_PORT}/api/card",
        headers=headers,
        json={
            "name": "Customers",
            "dataset_query": {
                "database": database_id,
                "type": "query",
                "query": {
                    "source-table": customers_table_id,
                    "aggregation": [["count"]],
                    "breakout": [
                        [
                            "field",
                            first_order_field_id,
                            {"base-type": "type/Date", "temporal-unit": "month"},
                        ]
                    ],
                },
            },
            "display": "line",
            "description": "Customers test",
            "visualization_settings": {
                "graph.dimensions": ["first_order"],
                "graph.metrics": ["count"],
            },
            "collection_id": None,
            "collection_position": None,
            "result_metadata": [
                {
                    "description": None,
                    "semantic_type": None,
                    "coercion_strategy": None,
                    "unit": "month",
                    "name": "first_order",
                    "settings": None,
                    "fk_target_field_id": None,
                    "field_ref": [
                        "field",
                        first_order_field_id,
                        {"base-type": "type/Date", "temporal-unit": "month"},
                    ],
                    "effective_type": "type/DateTimeWithLocalTZ",
                    "id": first_order_field_id,
                    "visibility_type": "normal",
                    "display_name": "First Order",
                    "fingerprint": {
                        "global": {"distinct-count": 47, "nil%": 0.38},
                        "type": {
                            "type/DateTime": {
                                "earliest": "2018-01-01",
                                "latest": "2018-04-07",
                            }
                        },
                    },
                    "base_type": "type/DateTimeWithLocalTZ",
                },
                {
                    "display_name": "Count",
                    "semantic_type": "type/Quantity",
                    "field_ref": ["aggregation", 0],
                    "name": "count",
                    "base_type": "type/BigInteger",
                    "effective_type": "type/BigInteger",
                    "fingerprint": {
                        "global": {"distinct-count": 4, "nil%": 0},
                        "type": {
                            "type/Number": {
                                "min": 2,
                                "q1": 11.298221281347036,
                                "q3": 27.5,
                                "max": 38,
                                "sd": 12.96148139681572,
                                "avg": 20,
                            }
                        },
                    },
                },
            ],
        },
        timeout=10,
    )


evaluate()
