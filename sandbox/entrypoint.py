#!/usr/bin/env python
import logging

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

MB_API_URL = f"http://{MB_HOST}:{MB_PORT}/api"


@target(
    description="initial setup",
    depends=["dbt_run", "metabase_setup"],
)
def init():
    pass


@target(description="run dbt project")
def dbt_run():
    shell("dbt seed --profiles-dir .")
    shell("dbt run --profiles-dir .")


@target(description="set up Metabase user and database")
def metabase_setup():
    setup_resp = requests.post(
        url=f"{MB_API_URL}/setup",
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
            "prefs": {
                "site_name": MB_NAME,
                "site_locale": "en",
                "allow_tracking": "false",
            },
        },
        timeout=10,
    )
    if setup_resp.status_code == 200:
        logging.info("Metabase setup successful")
    elif setup_resp.status_code == 403:
        logging.info("Metabase already set up")
    else:
        raise requests.HTTPError(f"Error: {setup_resp.reason}", response=setup_resp)

    session_id = requests.post(
        url=f"{MB_API_URL}/session",
        json={"username": MB_USER, "password": MB_PASSWORD},
        timeout=10,
    ).json()["id"]

    headers = {"X-Metabase-Session": session_id}

    database_id = ""
    sample_database_id = ""
    databases = requests.get(
        url=f"{MB_API_URL}/database",
        headers=headers,
        timeout=10,
    ).json()["data"]
    for db in databases:
        if db["name"] == POSTGRES_DB and db["engine"] == "postgres":
            database_id = db["id"]
        elif db["name"] == "Sample Database" and db["engine"] == "h2":
            sample_database_id = db["id"]

    if sample_database_id:
        logging.info("Archiving Metabase sample database %s", sample_database_id)
        requests.delete(
            url=f"{MB_API_URL}/database/{sample_database_id}",
            headers=headers,
            timeout=10,
        ).raise_for_status()

    collections = requests.get(
        url=f"{MB_API_URL}/collection",
        headers=headers,
        timeout=10,
    ).json()
    for collection in collections:
        if collection.get("is_sample") == True and collection.get("archived") == False:
            logging.info("Deleting Metabase sample collection %s", collection["id"])
            requests.put(
                url=f"{MB_API_URL}/collection/{collection['id']}",
                headers=headers,
                json={"archived": True},
                timeout=10,
            ).raise_for_status()

    database_body = {
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
    }
    if not database_id:
        logging.info("Creating Metabase database")
        database_id = requests.post(
            url=f"{MB_API_URL}/database",
            headers=headers,
            json=database_body,
            timeout=10,
        ).json()["id"]
    else:
        logging.info("Updating Metabase database %s", database_id)
        requests.put(
            url=f"{MB_API_URL}/database/{database_id}",
            headers=headers,
            json=database_body,
            timeout=10,
        ).raise_for_status()

    logging.info("Triggering Metabase database sync")
    requests.post(
        url=f"{MB_API_URL}/database/{database_id}/sync_schema",
        headers=headers,
        json={},
        timeout=10,
    ).raise_for_status()


evaluate()
