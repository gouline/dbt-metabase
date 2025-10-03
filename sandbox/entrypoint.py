#!/usr/bin/env python
import logging
import os

import requests
from molot import envarg, envarg_int, evaluate, shell, target

POSTGRES_HOST = envarg("POSTGRES_HOST")
POSTGRES_PORT = envarg_int("POSTGRES_PORT")
POSTGRES_DB = envarg("POSTGRES_DB")
POSTGRES_USER = envarg("POSTGRES_USER")
POSTGRES_PASSWORD = envarg("POSTGRES_PASSWORD")

MB_HOST = os.getenv("MB_HOST", "localhost")
MB_PORT = envarg_int("MB_PORT")
MB_SETUP_TOKEN = envarg("MB_SETUP_TOKEN")
MB_USER = envarg("MB_USER")
MB_PASSWORD = envarg("MB_PASSWORD")
MB_NAME = envarg("MB_NAME", "dbtmetabase")

# Databricks (optional)
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA")
DATABRICKS_MB_DB_NAME = os.getenv("DATABRICKS_MB_DB_NAME")

MB_API_URL = f"http://{MB_HOST}:{MB_PORT}/api"


@target(
    description="initial setup",
    depends=["dbt_run", "metabase_setup"],
)
def init():
    pass


@target(
    description="initial setup with databricks",
    depends=["dbt_run", "metabase_setup", "databricks_setup"],
)
def init_databricks():
    pass


@target(description="run dbt project")
def dbt_run():
    # Check if we should use Databricks target
    target_arg = ""
    if all(
        [DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_CATALOG]
    ):
        target_arg = " --target databricks"
        logging.info("Using Databricks target for dbt commands")
    else:
        logging.info("Using default PostgreSQL target for dbt commands")

    shell(f"dbt seed --profiles-dir .{target_arg}")
    shell(f"dbt run --profiles-dir .{target_arg}")


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
        if collection.get("is_sample") and not collection.get("archived"):
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
        logging.info(f"{MB_API_URL}/database/{database_id}")
        response = requests.put(
            url=f"{MB_API_URL}/database/{database_id}",
            headers=headers,
            json=database_body,
            timeout=10,
        )
        if response.status_code != 200:
            logging.error("Error updating database: %s", response.text)
            logging.error("Database body: %s", database_body)
        response.raise_for_status()

    logging.info("Triggering Metabase database sync")
    requests.post(
        url=f"{MB_API_URL}/database/{database_id}/sync_schema",
        headers=headers,
        json={},
        timeout=10,
    ).raise_for_status()


@target(
    description="set up Databricks: initialize database and configure Metabase connection"
)
def databricks_setup():
    if not all(
        [
            DATABRICKS_HOST,
            DATABRICKS_HTTP_PATH,
            DATABRICKS_TOKEN,
            DATABRICKS_CATALOG,
            DATABRICKS_MB_DB_NAME,
            DATABRICKS_SCHEMA,
        ]
    ):
        logging.info("Databricks not configured, skipping")
        return

    # Step 1: Initialize Databricks tables first
    logging.info("=== STEP 1: Initializing Databricks schemas and tables ===")
    try:
        from databricks import sql
    except ImportError:
        logging.error(
            "databricks-sql-connector not installed. "
            "Run: pip install databricks-sql-connector"
        )
        return

    # Read the init SQL file
    init_sql_path = "databricks-initdb/init.sql"
    if os.path.exists(init_sql_path):
        with open(init_sql_path, "r", encoding="utf-8") as f:
            init_sql = f.read()

        # Replace variables in SQL
        init_sql = init_sql.replace("${DATABRICKS_CATALOG}", DATABRICKS_CATALOG)
        init_sql = init_sql.replace("${DATABRICKS_SCHEMA}", DATABRICKS_SCHEMA)

        # Execute SQL using Databricks SQL connector
        logging.info("Creating Databricks schemas and tables...")
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as connection:
            with connection.cursor() as cursor:
                statements = [
                    stmt.strip() for stmt in init_sql.split(";") if stmt.strip()
                ]
                for statement in statements:
                    logging.info("Executing: %s...", statement[:50])
                    cursor.execute(statement)
        logging.info("Databricks initialization completed")
    else:
        logging.warning("Init SQL file not found: %s", init_sql_path)

    # Step 2: Set up Metabase connection (now that tables exist)
    logging.info("=== STEP 2: Setting up Metabase connection ===")

    # Use MB_HOST for sandbox
    mb_url = f"http://{MB_HOST}:{MB_PORT}/api"

    session_id = requests.post(
        url=f"{mb_url}/session",
        json={"username": MB_USER, "password": MB_PASSWORD},
        timeout=10,
    ).json()["id"]

    headers = {"X-Metabase-Session": session_id}

    databricks_database_id = ""
    databases = requests.get(
        url=f"{mb_url}/database",
        headers=headers,
        timeout=10,
    ).json()["data"]
    for db in databases:
        if db["name"] == DATABRICKS_MB_DB_NAME and db["engine"] == "databricks":
            databricks_database_id = db["id"]
            break

    database_body = {
        "engine": "databricks",
        "name": DATABRICKS_MB_DB_NAME,
        "details": {
            "host": DATABRICKS_HOST,
            "http-path": DATABRICKS_HTTP_PATH,
            "token": DATABRICKS_TOKEN,
            "catalog": DATABRICKS_CATALOG,
            "use-multiple-catalogs": True,
            "use-m2m-auth": False,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}",
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

    if not databricks_database_id:
        logging.info(
            "Creating Databricks database '%s' in Metabase", DATABRICKS_MB_DB_NAME
        )
        logging.info(
            "Database config: host=%s, catalog=%s", DATABRICKS_HOST, DATABRICKS_CATALOG
        )
        databricks_database_id = requests.post(
            url=f"{mb_url}/database",
            headers=headers,
            json=database_body,
            timeout=10,
        ).json()["id"]
        logging.info("Created Databricks database with ID: %s", databricks_database_id)
    else:
        logging.info(
            "Found existing Databricks database '%s' (ID: %s)",
            DATABRICKS_MB_DB_NAME,
            databricks_database_id,
        )
        logging.info("Updating connection details (non-destructive)")
        logging.info(
            "New config: host=%s, catalog=%s", DATABRICKS_HOST, DATABRICKS_CATALOG
        )
        requests.put(
            url=f"{mb_url}/database/{databricks_database_id}",
            headers=headers,
            json=database_body,
            timeout=10,
        ).raise_for_status()
        logging.info("Successfully updated Databricks database connection")

    logging.info("Triggering Databricks database sync")
    sync_response = requests.post(
        url=f"{mb_url}/database/{databricks_database_id}/sync_schema",
        headers=headers,
        json={},
        timeout=30,
    )
    if sync_response.status_code == 200:
        logging.info("Database sync completed successfully")
    else:
        logging.warning(
            "Sync failed: %s - %s", sync_response.status_code, sync_response.text
        )

    # Also trigger a full rescan to be thorough
    logging.info("Triggering full database rescan")
    rescan_response = requests.post(
        url=f"{mb_url}/database/{databricks_database_id}/rescan_values",
        headers=headers,
        json={},
        timeout=30,
    )
    if rescan_response.status_code != 200:
        logging.warning(
            "Rescan failed: %s - %s", rescan_response.status_code, rescan_response.text
        )

    # Give Metabase a moment to process the sync
    import time

    logging.info("Waiting 3 seconds for sync to complete")
    time.sleep(3)

    # Debug: Check what tables and fields Metabase actually has
    logging.info("Checking Metabase table metadata...")
    tables_response = requests.get(
        url=f"{mb_url}/database/{databricks_database_id}",
        headers=headers,
        timeout=10,
    )
    if tables_response.status_code == 200:
        db_data = tables_response.json()
        logging.info("Database found: %s", db_data.get("name", "Unknown"))

        # Get table metadata
        tables_response = requests.get(
            url=f"{mb_url}/database/{databricks_database_id}/metadata",
            headers=headers,
            timeout=10,
        )
        if tables_response.status_code == 200:
            metadata = tables_response.json()
            for table in metadata.get("tables", []):
                table_name = table.get("name", "Unknown")
                field_count = len(table.get("fields", []))
                logging.info("Table: %s, Fields: %d", table_name, field_count)
                if field_count == 0:
                    logging.warning("Table %s has no fields!", table_name)
                else:
                    field_names = [f.get("name") for f in table.get("fields", [])]
                    logging.info("Fields in %s: %s", table_name, field_names)
        else:
            logging.warning("Failed to get metadata: %s", tables_response.text)
    else:
        logging.warning("Failed to get database info: %s", tables_response.text)


evaluate()
