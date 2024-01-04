#!/usr/bin/env python

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


@target(description="set up Metabase user and database")
def setup_metabase():
    requests.post(
        f"http://{MB_HOST}:{MB_PORT}/api/setup",
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


@target(description="run dbt project")
def run_dbt():
    shell("dbt run --profiles-dir .")


@target(description="initial setup", depends=["setup_metabase", "run_dbt"])
def init():
    pass


evaluate()
