from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
import yaml
from requests.adapters import HTTPAdapter, Retry

from .dbt import (
    METABASE_MODEL_DEFAULT_SCHEMA,
    MetabaseColumn,
    MetabaseModel,
    ModelType,
    NullValue,
)
from .logger.logging import logger


class MetabaseArgumentError(ValueError):
    """Invalid Metabase arguments supplied."""


class MetabaseRuntimeError(RuntimeError):
    """Metabase execution failed."""


class _MetabaseClientJob:
    """Scoped abstraction for jobs depending on the Metabase client."""

    def __init__(self, client: MetabaseClient):
        self.client = client


class _ExportModelsJob(_MetabaseClientJob):
    """Job abstraction for exporting models."""

    _SYNC_PERIOD = 5

    def __init__(
        self,
        client: MetabaseClient,
        database: str,
        models: List[MetabaseModel],
        exclude_sources: bool,
        sync_timeout: int,
    ):
        super().__init__(client)

        self.database = database
        self.models = [
            model
            for model in models
            if model.model_type != ModelType.sources or not exclude_sources
        ]
        self.sync_timeout = sync_timeout

        self.tables: Mapping[str, MutableMapping] = {}
        self.updates: MutableMapping[str, MutableMapping[str, Any]] = {}

    def execute(self):
        success = True

        database_id = None
        for database in self.client.api("get", "/api/database"):
            if database["name"].upper() == self.database.upper():
                database_id = database["id"]
                break
        if not database_id:
            raise MetabaseRuntimeError(f"Cannot find database by name {self.database}")

        if self.sync_timeout:
            self.client.api("post", f"/api/database/{database_id}/sync_schema")
            time.sleep(self._SYNC_PERIOD)

        deadline = int(time.time()) + self.sync_timeout
        synced = False
        while not synced:
            tables = self._load_tables(database_id)

            synced = True
            for model in self.models:
                schema_name = model.schema.upper()
                model_name = model.name.upper()
                table_key = f"{schema_name}.{model_name}"

                table = tables.get(table_key)
                if not table:
                    logger().warning(
                        "Model %s not found in %s schema", table_key, schema_name
                    )
                    synced = False
                    continue

                for column in model.columns:
                    column_name = column.name.upper()

                    field = table.get("fields", {}).get(column_name)
                    if not field:
                        logger().warning(
                            "Column %s not found in %s model", column_name, table_key
                        )
                        synced = False
                        continue

            self.tables = tables

            if int(time.time()) < deadline:
                time.sleep(self._SYNC_PERIOD)

        if not synced and self.sync_timeout:
            raise MetabaseRuntimeError("Unable to sync models between dbt and Metabase")

        for model in self.models:
            success &= self._export_model(model)

        for update in self.updates.values():
            self.client.api(
                "put",
                f"/api/{update['kind']}/{update['id']}",
                json=update["body"],
            )
            logger().info(
                "Update to %s %s applied successfully",
                update["kind"],
                update["id"],
            )

        if not success:
            raise MetabaseRuntimeError(
                "Model export encountered non-critical errors, check output"
            )

    def queue_update(self, entity: MutableMapping, delta: Mapping):
        entity.update(delta)

        key = f"{entity['kind']}.{entity['id']}"
        update = self.updates.get(key, {})
        update["kind"] = entity["kind"]
        update["id"] = entity["id"]

        body = update.get("body", {})
        body.update(delta)
        update["body"] = body

        self.updates[key] = update

    def _export_model(self, model: MetabaseModel) -> bool:
        """Exports one dbt model to Metabase database schema.

        Arguments:
            model {dict} -- One dbt model read from project.

        Returns:
            bool -- True if exported successfully, false if there were errors.
        """

        success = True

        schema_name = model.schema.upper()
        model_name = model.name.upper()
        table_key = f"{schema_name}.{model_name}"

        api_table = self.tables.get(table_key)
        if not api_table:
            logger().error("Table %s does not exist in Metabase", table_key)
            return False

        # Empty strings not accepted by Metabase
        model_display_name = model.display_name or None
        model_description = model.description or None
        model_points_of_interest = model.points_of_interest or None
        model_caveats = model.caveats or None
        model_visibility = model.visibility_type or None

        body_table = {}

        # Update if specified, otherwise reset one that had been set
        if api_table.get("display_name") != model_display_name and (
            model_display_name or api_table.get("display_name") != api_table.get("name")
        ):
            body_table["display_name"] = model_display_name

        if api_table.get("description") != model_description:
            body_table["description"] = model_description
        if api_table.get("points_of_interest") != model_points_of_interest:
            body_table["points_of_interest"] = model_points_of_interest
        if api_table.get("caveats") != model_caveats:
            body_table["caveats"] = model_caveats
        if api_table.get("visibility_type") != model_visibility:
            body_table["visibility_type"] = model_visibility

        if body_table:
            self.queue_update(entity=api_table, delta=body_table)
            logger().info("Table %s will be updated", table_key)
        else:
            logger().info("Table %s is up-to-date", table_key)

        for column in model.columns:
            success &= self._export_column(schema_name, model_name, column)

        return success

    def _export_column(
        self,
        schema_name: str,
        model_name: str,
        column: MetabaseColumn,
    ) -> bool:
        """Exports one dbt column to Metabase database schema.

        Arguments:
            schema_name {str} -- Target schema name.s
            model_name {str} -- One dbt model name read from project.
            column {dict} -- One dbt column read from project.

        Returns:
            bool -- True if exported successfully, false if there were errors.
        """

        success = True

        table_key = f"{schema_name}.{model_name}"
        column_name = column.name.upper()

        api_field = self.tables.get(table_key, {}).get("fields", {}).get(column_name)
        if not api_field:
            logger().error(
                "Field %s.%s does not exist in Metabase",
                table_key,
                column_name,
            )
            return False

        if "special_type" in api_field:
            semantic_type_key = "special_type"
        else:
            semantic_type_key = "semantic_type"

        fk_target_field_id = None
        if column.semantic_type == "type/FK":
            # Target table could be aliased if we parse_ref() on a source, so we caught aliases during model parsing
            # This way we can unpack any alias mapped to fk_target_table when using yml project reader
            target_table = (
                column.fk_target_table.upper()
                if column.fk_target_table is not None
                else None
            )
            target_field = (
                column.fk_target_field.upper()
                if column.fk_target_field is not None
                else None
            )

            if not target_table or not target_field:
                logger().info(
                    "Passing on fk resolution for %s. Target field %s was not resolved during dbt model parsing.",
                    table_key,
                    target_field,
                )

            else:
                logger().debug(
                    "Looking for field %s in table %s",
                    target_field,
                    target_table,
                )

                fk_target_field = (
                    self.tables.get(target_table, {})
                    .get("fields", {})
                    .get(target_field)
                )
                if fk_target_field:
                    fk_target_field_id = fk_target_field.get("id")
                    if fk_target_field.get(semantic_type_key) != "type/PK":
                        logger().info(
                            "Target field %s will be set to PK for %s column FK",
                            fk_target_field_id,
                            column_name,
                        )
                        body_fk_target_field = {
                            semantic_type_key: "type/PK",
                        }
                        self.queue_update(
                            entity=fk_target_field, delta=body_fk_target_field
                        )
                    else:
                        logger().info(
                            "Target field %s is already PK, needed for %s column",
                            fk_target_field_id,
                            column_name,
                        )
                else:
                    logger().error(
                        "Unable to find foreign key target %s.%s",
                        target_table,
                        target_field,
                    )
                    success = False

        # Empty strings not accepted by Metabase
        column_description = column.description or None
        column_display_name = column.display_name or None
        column_visibility = column.visibility_type or "normal"

        # Preserve this relationship by default
        if api_field["fk_target_field_id"] and not fk_target_field_id:
            fk_target_field_id = api_field["fk_target_field_id"]

        body_field: MutableMapping[str, Optional[Any]] = {}

        # Update if specified, otherwise reset one that had been set
        if api_field.get("display_name") != column_display_name and (
            column_display_name
            or api_field.get("display_name") != api_field.get("name")
        ):
            body_field["display_name"] = column_display_name

        if api_field.get("description") != column_description:
            body_field["description"] = column_description
        if api_field.get("visibility_type") != column_visibility:
            body_field["visibility_type"] = column_visibility
        if api_field.get("fk_target_field_id") != fk_target_field_id:
            body_field["fk_target_field_id"] = fk_target_field_id
        if (
            api_field.get("has_field_values") != column.has_field_values
            and column.has_field_values
        ):
            body_field["has_field_values"] = column.has_field_values
        if (
            api_field.get("coercion_strategy") != column.coercion_strategy
            and column.coercion_strategy
        ):
            body_field["coercion_strategy"] = column.coercion_strategy

        settings = api_field.get("settings") or {}
        if settings.get("number_style") != column.number_style and column.number_style:
            settings["number_style"] = column.number_style

        if settings:
            body_field["settings"] = settings

        # Allow explicit null type to override detected one
        if api_field.get(semantic_type_key) != column.semantic_type and (
            column.semantic_type or column.semantic_type is NullValue
        ):
            body_field[semantic_type_key] = column.semantic_type or None

        if body_field:
            self.queue_update(entity=api_field, delta=body_field)
            logger().info("Field %s.%s will be updated", model_name, column_name)
        else:
            logger().info("Field %s.%s is up-to-date", model_name, column_name)

        return success

    def _load_tables(self, database_id: str) -> Mapping[str, MutableMapping]:
        tables = {}

        metadata = self.client.api(
            "get",
            f"/api/database/{database_id}/metadata",
            params={"include_hidden": True},
        )

        bigquery_schema = metadata.get("details", {}).get("dataset-id")

        for table in metadata.get("tables", []):
            # table[schema] is null for bigquery datasets
            table["schema"] = (
                table.get("schema") or bigquery_schema or METABASE_MODEL_DEFAULT_SCHEMA
            ).upper()

            fields = {}
            for field in table.get("fields", []):
                new_field = field.copy()
                new_field["kind"] = "field"

                field_name = field["name"].upper()
                fields[field_name] = new_field

            new_table = table.copy()
            new_table["kind"] = "table"
            new_table["fields"] = fields

            schema_name = table["schema"].upper()
            table_name = table["name"].upper()
            tables[f"{schema_name}.{table_name}"] = new_table

        return tables


class _ExtractExposuresJob(_MetabaseClientJob):
    _RESOURCE_VERSION = 2

    # This regex is looking for from and join clauses, and extracting the table part.
    # It won't recognize some valid sql table references, such as `from "table with spaces"`.
    _EXPOSURE_PARSER = re.compile(r"[FfJj][RrOo][OoIi][MmNn]\s+([\w.\"]+)")
    _CTE_PARSER = re.compile(
        r"[Ww][Ii][Tt][Hh]\s+\b(\w+)\b\s+as|[)]\s*[,]\s*\b(\w+)\b\s+as"
    )

    class DbtDumper(yaml.Dumper):
        def increase_indent(self, flow=False, indentless=False):
            return super().increase_indent(flow, indentless=False)

    def __init__(
        self,
        client: MetabaseClient,
        models: List[MetabaseModel],
        output_path: str,
        output_name: str,
        include_personal_collections: bool,
        collection_excludes: Optional[Iterable],
    ):
        super().__init__(client)

        self.model_refs = {model.name.upper(): model.ref for model in models}
        self.output_file = Path(output_path).expanduser() / f"{output_name}.yml"
        self.include_personal_collections = include_personal_collections
        self.collection_excludes = collection_excludes or []

        self.table_names: Mapping = {}
        self.models_exposed: List = []
        self.native_query: str = ""

    def execute(self) -> Mapping:
        """Extracts exposures in Metabase downstream of dbt models and sources as parsed by dbt reader.

        Returns:
            Mapping: JSON object representation of all exposures parsed.
        """

        self.table_names = {
            table["id"]: table["name"] for table in self.client.api("get", "/api/table")
        }

        documented_exposure_names = []
        parsed_exposures = []

        for collection in self.client.api("get", "/api/collection"):
            # Exclude collections by name or personal collections (unless included)
            if collection["name"] in self.collection_excludes or (
                collection.get("personal_owner_id")
                and not self.include_personal_collections
            ):
                continue

            # Iter through collection
            logger().info("Exploring collection %s", collection["name"])
            for item in self.client.api(
                "get", f"/api/collection/{collection['id']}/items"
            ):
                # Ensure collection item is of parsable type
                exposure_type = item["model"]
                exposure_id = item["id"]
                if exposure_type not in ("card", "dashboard"):
                    continue

                # Prepare attributes for population through _extract_card_exposures calls
                self.models_exposed = []
                self.native_query = ""
                native_query = ""

                exposure = self.client.api("get", f"/api/{exposure_type}/{exposure_id}")
                exposure_name = exposure.get("name", "Exposure [Unresolved Name]")
                logger().info(
                    "Introspecting exposure: %s",
                    exposure_name,
                )

                header = None
                creator_name = None
                creator_email = None

                # Process exposure
                if exposure_type == "card":
                    # Build header for card and extract models to self.models_exposed
                    header = "### Visualization: {}\n\n".format(
                        exposure.get("display", "Unknown").title()
                    )

                    # Parse Metabase question
                    self._extract_card_exposures(exposure_id, exposure)
                    native_query = self.native_query

                elif exposure_type == "dashboard":
                    # We expect this dict key in order to iter through questions
                    if "ordered_cards" not in exposure:
                        continue

                    # Build header for dashboard and extract models for each question to self.models_exposed
                    header = "### Dashboard Cards: {}\n\n".format(
                        str(len(exposure["ordered_cards"]))
                    )

                    # Iterate through dashboard questions
                    for dashboard_item in exposure["ordered_cards"]:
                        dashboard_item_reference = dashboard_item.get("card", {})
                        if "id" not in dashboard_item_reference:
                            continue

                        # Parse Metabase question
                        self._extract_card_exposures(dashboard_item_reference["id"])

                if not self.models_exposed:
                    logger().info("No models mapped to exposure")

                # Extract creator info
                if "creator" in exposure:
                    creator_email = exposure["creator"]["email"]
                    creator_name = exposure["creator"]["common_name"]
                elif "creator_id" in exposure:
                    # If a metabase user is deactivated, the API returns a 404
                    try:
                        creator = self.client.api(
                            "get", f"/api/user/{exposure['creator_id']}"
                        )
                    except requests.exceptions.HTTPError as error:
                        creator = {}
                        if error.response is None or error.response.status_code != 404:
                            raise

                    creator_name = creator.get("common_name")
                    creator_email = creator.get("email")

                exposure_label = exposure_name
                # Only letters, numbers and underscores allowed in model names in dbt docs DAG / no duplicate model names
                exposure_name = re.sub(r"[^\w]", "_", exposure_name).lower()
                enumer = 1
                while exposure_name in documented_exposure_names:
                    exposure_name = f"{exposure_name}_{enumer}"
                    enumer += 1

                # Construct exposure
                parsed_exposures.append(
                    self._build_exposure(
                        exposure_type=exposure_type,
                        exposure_id=exposure_id,
                        name=exposure_name,
                        label=exposure_label,
                        header=header or "",
                        created_at=exposure["created_at"],
                        creator_name=creator_name or "",
                        creator_email=creator_email or "",
                        description=exposure.get("description", ""),
                        native_query=native_query,
                    )
                )

                documented_exposure_names.append(exposure_name)

        # Output dbt YAML
        result = {
            "version": self._RESOURCE_VERSION,
            "exposures": parsed_exposures,
        }
        with open(self.output_file, "w", encoding="utf-8") as docs:
            yaml.dump(
                result,
                docs,
                Dumper=self.DbtDumper,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        return result

    def _extract_card_exposures(
        self,
        card_id: int,
        exposure: Optional[Mapping] = None,
    ):
        """Extracts exposures from Metabase questions populating `self.models_exposed`

        Arguments:
            card_id {int} -- Id of Metabase question used to pull question from api

        Keyword Arguments:
            exposure {str} -- JSON api response from a question in Metabase, allows us to use the object if already in memory

        Returns:
            None -- self.models_exposed is populated through this method.
        """

        # If an exposure is not passed, pull from id
        if not exposure:
            exposure = self.client.api("get", f"/api/card/{card_id}")

        query = exposure.get("dataset_query", {})

        if query.get("type") == "query":
            # Metabase GUI derived query
            source_table_id = query.get("query", {}).get(
                "source-table", exposure.get("table_id")
            )

            if str(source_table_id).startswith("card__"):
                # Handle questions based on other question in virtual db
                self._extract_card_exposures(int(source_table_id.split("__")[-1]))
            else:
                # Normal question
                source_table = self.table_names.get(source_table_id)
                if source_table:
                    logger().info(
                        "Model extracted from Metabase question: %s",
                        source_table,
                    )
                    self.models_exposed.append(source_table)

            # Find models exposed through joins
            for query_join in query.get("query", {}).get("joins", []):
                # Handle questions based on other question in virtual db
                if str(query_join.get("source-table", "")).startswith("card__"):
                    self._extract_card_exposures(
                        int(query_join.get("source-table").split("__")[-1])
                    )
                    continue

                # Joined model parsed
                joined_table = self.table_names.get(query_join.get("source-table"))
                if joined_table:
                    logger().info(
                        "Model extracted from Metabase question join: %s",
                        joined_table,
                    )
                    self.models_exposed.append(joined_table)

        elif query.get("type") == "native":
            # Metabase native query
            native_query = query["native"].get("query")
            ctes: List[str] = []

            # Parse common table expressions for exclusion
            for matched_cte in re.findall(self._CTE_PARSER, native_query):
                ctes.extend(group.upper() for group in matched_cte if group)

            # Parse SQL for exposures through FROM or JOIN clauses
            for sql_ref in re.findall(self._EXPOSURE_PARSER, native_query):
                # Grab just the table / model name
                clean_exposure = sql_ref.split(".")[-1].strip('"').upper()

                # Scrub CTEs (qualified sql_refs can not reference CTEs)
                if clean_exposure in ctes and "." not in sql_ref:
                    continue
                # Verify this is one of our parsed refable models so exposures dont break the DAG
                if not self.model_refs.get(clean_exposure):
                    continue

                if clean_exposure:
                    logger().info(
                        "Model extracted from native query: %s",
                        clean_exposure,
                    )
                    self.models_exposed.append(clean_exposure)
                    self.native_query = native_query

    def _build_exposure(
        self,
        exposure_type: str,
        exposure_id: int,
        name: str,
        label: str,
        header: str,
        created_at: str,
        creator_name: str,
        creator_email: str,
        description: str = "",
        native_query: str = "",
    ) -> Mapping:
        """Builds an exposure object representation as defined here: https://docs.getdbt.com/reference/exposure-properties

        Arguments:
            exposure_type {str} -- Model type in Metabase being either `card` or `dashboard`
            exposure_id {str} -- Card or Dashboard id in Metabase
            name {str} -- Name of exposure
            label {str} -- Title of the card or dashboard in Metabase
            header {str} -- The header goes at the top of the description and is useful for prefixing metadata
            created_at {str} -- Timestamp of exposure creation derived from Metabase
            creator_name {str} -- Creator name derived from Metabase
            creator_email {str} -- Creator email derived from Metabase

        Keyword Arguments:
            description {str} -- The description of the exposure as documented in Metabase. (default: No description provided in Metabase)
            native_query {str} -- If exposure contains SQL, this arg will include the SQL in the dbt exposure documentation. (default: {""})

        Returns:
            Mapping -- JSON object representation of single exposure.
        """

        # Ensure model type is compatible
        assert exposure_type in (
            "card",
            "dashboard",
        ), "Cannot construct exposure for object type of {}".format(exposure_type)

        if native_query:
            # Format query into markdown code block
            native_query = "#### Query\n\n```\n{}\n```\n\n".format(
                "\n".join(
                    sql_line
                    for sql_line in self.native_query.strip().split("\n")
                    if sql_line.strip() != ""
                )
            )

        if not description:
            description = "No description provided in Metabase\n\n"

        # Format metadata as markdown
        metadata = (
            "#### Metadata\n\n"
            + "Metabase Id: __{}__\n\n".format(exposure_id)
            + "Created On: __{}__".format(created_at)
        )

        # Build description
        description = (
            header + ("{}\n\n".format(description.strip())) + native_query + metadata
        )

        # Output exposure
        return {
            "name": name,
            "label": label,
            "description": description,
            "type": "analysis" if exposure_type == "card" else "dashboard",
            "url": f"{self.client.url}/{exposure_type}/{exposure_id}",
            "maturity": "medium",
            "owner": {
                "name": creator_name,
                "email": creator_email,
            },
            "depends_on": list(
                {
                    self.model_refs[exposure.upper()]
                    for exposure in list({m for m in self.models_exposed})
                    if exposure.upper() in self.model_refs
                }
            ),
        }


class MetabaseClient:
    """Metabase API client."""

    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        session_id: Optional[str] = None,
        verify: bool = True,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        http_timeout: int = 15,
        http_headers: Optional[dict] = None,
        http_adapter: Optional[HTTPAdapter] = None,
    ):
        """New Metabase client.

        Args:
            url (str): Metabase URL, e.g. "https://metabase.example.com".
            username (Optional[str], optional): Metabase username (required unless providing session_id). Defaults to None.
            password (Optional[str], optional): Metabase password (required unless providing session_id). Defaults to None.
            session_id (Optional[str], optional): Metabase session ID. Defaults to None.
            verify (bool, optional): Verify the TLS certificate at the Metabase end. Defaults to True.
            cert (Optional[Union[str, Tuple[str, str]]], optional): Path to a custom certificate. Defaults to None.
            http_timeout (int, optional): HTTP request timeout in secs. Defaults to 15.
            http_headers (Optional[dict], optional): Additional HTTP headers. Defaults to None.
            http_adapter (Optional[HTTPAdapter], optional): Custom requests HTTP adapter. Defaults to None.
        """

        self.url = url.rstrip("/")

        self.http_timeout = http_timeout

        self.session = requests.Session()
        self.session.verify = verify
        self.session.cert = cert

        if http_headers:
            self.session.headers.update(http_headers)

        self.session.mount(
            self.url,
            http_adapter or HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5)),
        )

        if not session_id:
            if username and password:
                session = self.api(
                    "post",
                    "/api/session",
                    json={"username": username, "password": password},
                )
                session_id = str(session["id"])
            else:
                raise MetabaseArgumentError("Credentials or session ID required")
        self.session.headers["X-Metabase-Session"] = session_id

        logger().info("Session established successfully")

    def api(
        self,
        method: str,
        path: str,
        critical: bool = True,
        **kwargs,
    ) -> Mapping:
        """Unified way of calling Metabase API.

        Args:
            method (str): HTTP verb, e.g. get, post, put.
            path (str): Relative path of endpoint, e.g. /api/database.
            critical (bool, optional): Raise on any HTTP errors. Defaults to True.

        Returns:
            Mapping: JSON payload of the endpoint.
        """

        response = self.session.request(
            method,
            f"{self.url}{path}",
            timeout=self.http_timeout,
            **kwargs,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            if critical:
                logger().error("HTTP request failed: %s", response.text)
                raise
            return {}

        response_json = response.json()
        if "data" in response_json:
            # Since X.40.0 responses are encapsulated in "data" with pagination parameters
            return response_json["data"]

        return response_json

    def export_models(
        self,
        database: str,
        models: List[MetabaseModel],
        exclude_sources: bool = False,
        sync_timeout: int = 30,
    ):
        """Exports dbt models to Metabase database schema.

        Args:
            database (str): Metabase database name.
            models (List[MetabaseModel]): List of dbt models read from project.
            exclude_sources (bool, optional): Exclude dbt sources from export. Defaults to False.
        """
        _ExportModelsJob(
            client=self,
            database=database,
            models=models,
            exclude_sources=exclude_sources,
            sync_timeout=sync_timeout,
        ).execute()

    def extract_exposures(
        self,
        models: List[MetabaseModel],
        output_path: str = ".",
        output_name: str = "metabase_exposures",
        include_personal_collections: bool = True,
        collection_excludes: Optional[Iterable] = None,
    ) -> Mapping:
        """Extracts exposures in Metabase downstream of dbt models and sources as parsed by dbt reader.

        Args:
            models (List[MetabaseModel]): List of dbt models.
            output_path (str, optional): Path for output YAML. Defaults to ".".
            output_name (str, optional): Name for output YAML. Defaults to "metabase_exposures".
            include_personal_collections (bool, optional): Include personal Metabase collections. Defaults to True.
            collection_excludes (Optional[Iterable], optional): Exclude certain Metabase collections. Defaults to None.

        Returns:
            Mapping: _description_
        """
        return _ExtractExposuresJob(
            client=self,
            models=models,
            output_path=output_path,
            output_name=output_name,
            include_personal_collections=include_personal_collections,
            collection_excludes=collection_excludes,
        ).execute()
