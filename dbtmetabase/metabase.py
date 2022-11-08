import json
import os
import re
import time
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

import requests
import yaml
from requests.adapters import HTTPAdapter, Retry

from .logger.logging import logger
from .models import exceptions
from .models.metabase import (
    METABASE_MODEL_DEFAULT_SCHEMA,
    MetabaseColumn,
    MetabaseModel,
    ModelType,
    NullValue,
)


class MetabaseClient:
    """Metabase API client."""

    _SYNC_PERIOD_SECS = 5

    class _Metadata:
        """Mutable state of metadata (tables/fields) for lookups and updates."""

        def __init__(self, tables: Optional[Iterable[Dict]] = None):
            self.tables = {}
            self.updates: MutableMapping[str, MutableMapping[str, Any]] = {}

            if tables:
                for table in tables:
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
                    self.tables[f"{schema_name}.{table_name}"] = new_table

        def get_table(self, table_key: str) -> Optional[MutableMapping]:
            """Looks up table by key.

            Args:
                table_key (str): Table key of form SCHEMA.TABLE.

            Returns:
                Optional[MutableMapping]: Table description or none.
            """
            return self.tables.get(table_key)

        def get_field(self, table_key: str, field_key: str) -> Optional[MutableMapping]:
            """Looks up field by table and key.

            Args:
                table_key (str): Table key of form SCHEMA.TABLE.
                field_key (str): Field key.

            Returns:
                Optional[MutableMapping]: Field description or none.
            """
            return self.tables.get(table_key, {}).get("fields", {}).get(field_key)

        def update(self, entity: MutableMapping, delta: Mapping):
            """Updates entity (table or field) with arguments and stages API update.

            Args:
                entity (MutableMapping): Current state of entity.
                delta (Mapping): Fields that need to change.
            """
            entity.update(delta)

            key = f"{entity['kind']}.{entity['id']}"
            update = self.updates.get(key, {})
            update["kind"] = entity["kind"]
            update["id"] = entity["id"]

            body = update.get("body", {})
            body.update(delta)
            update["body"] = body

            self.updates[key] = update

        def pop_updates(self) -> Iterable[MutableMapping]:
            """Clears and returns currently staged updates.

            Returns:
                Iterable[MutableMapping]: List of updates.
            """
            updates = self.updates.values()
            self.updates = {}
            return updates

        def __bool__(self) -> bool:
            return bool(self.tables)

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        use_http: bool = False,
        verify: Union[str, bool] = None,
        session_id: str = None,
        exclude_sources: bool = False,
    ):
        """Constructor.

        Arguments:
            host {str} -- Metabase hostname.
            user {str} -- Metabase username.
            password {str} -- Metabase password.

        Keyword Arguments:
            use_http {bool} -- Use HTTP instead of HTTPS. (default: {False})
            verify {Union[str, bool]} -- Path to certificate or disable verification. (default: {None})
            session_id {str} -- Metabase session ID. (default: {None})
            exclude_sources {bool} -- Exclude exporting sources. (default: {False})
        """
        self.base_url = f"{'http' if use_http else 'https'}://{host}"
        self.session = requests.Session()
        self.session.verify = verify
        adaptor = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5))
        self.session.mount(self.base_url, adaptor)
        session_header = session_id or self.get_session_id(user, password)
        self.session.headers["X-Metabase-Session"] = session_header

        self.exclude_sources = exclude_sources
        self.collections: Iterable = []
        self.tables: Iterable = []
        self.table_map: MutableMapping = {}
        self.models_exposed: List = []
        self.native_query: str = ""

        # This regex is looking for from and join clauses, and extracting the table part.
        # It won't recognize some valid sql table references, such as `from "table with spaces"`.
        self.exposure_parser = re.compile(r"[FfJj][RrOo][OoIi][MmNn]\s+([\w.\"]+)")
        self.cte_parser = re.compile(
            r"[Ww][Ii][Tt][Hh]\s+\b(\w+)\b\s+as|[)]\s*[,]\s*\b(\w+)\b\s+as"
        )
        self.metadata = self._Metadata()

        logger().info(":ok_hand: Session established successfully")

    def get_session_id(self, user: str, password: str) -> str:
        """Obtains new session ID from API.

        Arguments:
            user {str} -- Metabase username.
            password {str} -- Metabase password.

        Returns:
            str -- Session ID.
        """

        return self.api(
            "post",
            "/api/session",
            json={"username": user, "password": password},
        )["id"]

    def sync_and_wait(
        self,
        database: str,
        models: Sequence,
        timeout: Optional[int],
    ) -> bool:
        """Synchronize with the database and wait for schema compatibility.

        Arguments:
            database {str} -- Metabase database name.
            models {list} -- List of dbt models read from project.
            timeout {int} -- Timeout before giving up in seconds.

        Returns:
            bool -- True if schema compatible with models, false if still incompatible.

        Raises:
            MetabaseUnableToSync if
                - the timeout provided is not sufficient
                - the database cannot be found
                - a timeout was provided but sync was unsuccessful
        """
        allow_sync_failure = False
        if not timeout:
            timeout = 30
            allow_sync_failure = True

        if timeout < self._SYNC_PERIOD_SECS:
            raise exceptions.MetabaseUnableToSync(
                f"Timeout provided {timeout} secs, must be at least {self._SYNC_PERIOD_SECS}"
            )

        self.metadata = MetabaseClient._Metadata()

        database_id = self.find_database_id(database)
        if not database_id:
            raise exceptions.MetabaseUnableToSync(
                f"Cannot find database by name {database}"
            )

        self.api("post", f"/api/database/{database_id}/sync_schema")

        deadline = int(time.time()) + timeout
        sync_successful = False
        while True:
            self.metadata = self.build_metadata(database_id)
            sync_successful = self.models_compatible(models)
            time_after_wait = int(time.time()) + self._SYNC_PERIOD_SECS
            if not sync_successful and time_after_wait <= deadline:
                time.sleep(self._SYNC_PERIOD_SECS)
            else:
                break
        if not sync_successful and not allow_sync_failure:
            raise exceptions.MetabaseUnableToSync(
                "Unable to align models between dbt target models and Metabase"
            )
        return sync_successful

    def models_compatible(self, models: Sequence[MetabaseModel]) -> bool:
        """Checks if models compatible with the Metabase database schema.

        Arguments:
            models {list} -- List of dbt models read from project.

        Returns:
            bool -- True if schema compatible with models, false otherwise.
        """

        are_models_compatible = True
        for model in models:
            if model.model_type == ModelType.sources and self.exclude_sources:
                continue

            schema_name = model.schema.upper()
            model_name = model.name.upper()

            lookup_key = f"{schema_name}.{model_name}"

            table = self.metadata.get_table(lookup_key)
            if not table:
                logger().warning(
                    "Model %s not found in %s schema", lookup_key, schema_name
                )
                are_models_compatible = False
                continue

            for column in model.columns:
                column_name = column.name.upper()

                field = self.metadata.get_field(lookup_key, column_name)
                if not field:
                    logger().warning(
                        "Column %s not found in %s model", column_name, lookup_key
                    )
                    are_models_compatible = False

        return are_models_compatible

    def export_models(
        self,
        database: str,
        models: Sequence[MetabaseModel],
        aliases,
    ):
        """Exports dbt models to Metabase database schema.

        Arguments:
            database {str} -- Metabase database name.
            models {list} -- List of dbt models read from project.
            aliases {dict} -- Provided by reader class. Shuttled down to column exports to resolve FK refs against relations to aliased source tables
        """

        if not self.metadata:
            database_id = self.find_database_id(database)
            if not database_id:
                logger().critical("Cannot find database by name %s", database)
                return
            self.metadata = self.build_metadata(database_id)

        for model in models:
            if model.model_type == ModelType.sources and self.exclude_sources:
                logger().info(":fast_forward: Skipping %s source", model.unique_id)
                continue

            self.export_model(model, aliases)

        for update in self.metadata.pop_updates():
            self.api(
                "put",
                f"/api/{update['kind']}/{update['id']}",
                json=update["body"],
            )
            logger().info(
                ":satellite: Update to %s %s applied successfully",
                update["kind"],
                update["id"],
            )

    def export_model(self, model: MetabaseModel, aliases: dict):
        """Exports one dbt model to Metabase database schema.

        Arguments:
            model {dict} -- One dbt model read from project.
            aliases {dict} -- Provided by reader class. Shuttled down to column exports to resolve FK refs against relations to aliased source tables
        """

        schema_name = model.schema.upper()
        model_name = model.name.upper()

        lookup_key = f"{schema_name}.{aliases.get(model_name, model_name)}"

        api_table = self.metadata.get_table(lookup_key)
        if not api_table:
            logger().error(
                ":cross_mark: Table %s does not exist in Metabase", lookup_key
            )
            return

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
            self.metadata.update(entity=api_table, delta=body_table)
            logger().info(":raising_hands: Table %s will be updated", lookup_key)
        else:
            logger().info(":thumbs_up: Table %s is up-to-date", lookup_key)

        for column in model.columns:
            self.export_column(schema_name, model_name, column, aliases)

    def export_column(
        self,
        schema_name: str,
        model_name: str,
        column: MetabaseColumn,
        aliases: dict,
    ):
        """Exports one dbt column to Metabase database schema.

        Arguments:
            model_name {str} -- One dbt model name read from project.
            column {dict} -- One dbt column read from project.
            aliases {dict} -- Provided by reader class. Used to resolve FK refs against relations to aliased source tables
        """

        table_lookup_key = f"{schema_name}.{model_name}"
        column_name = column.name.upper()

        api_field = self.metadata.get_field(table_lookup_key, column_name)
        if not api_field:
            logger().error(
                "Field %s.%s does not exist in Metabase", table_lookup_key, column_name
            )
            return

        if "special_type" in api_field:
            semantic_type_key = "special_type"
        else:
            semantic_type_key = "semantic_type"

        fk_target_field_id = None
        if column.semantic_type == "type/FK":
            # Target table could be aliased if we parse_ref() on a source, so we caught aliases during model parsing
            # This way we can unpack any alias mapped to fk_target_table when using yml folder parser
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
                    ":bow: Passing on fk resolution for %s. Target field %s was not resolved during dbt model parsing.",
                    table_lookup_key,
                    target_field,
                )

            else:
                was_aliased = (
                    aliases.get(target_table.split(".", 1)[-1])
                    if target_table
                    else None
                )
                if was_aliased:
                    target_table = ".".join(
                        [target_table.split(".", 1)[0], was_aliased]
                    )

                logger().debug(
                    ":magnifying_glass_tilted_right: Looking for field %s in table %s",
                    target_field,
                    target_table,
                )

                fk_target_field = self.metadata.get_field(target_table, target_field)
                if fk_target_field:
                    fk_target_field_id = fk_target_field.get("id")
                    if fk_target_field.get(semantic_type_key) != "type/PK":
                        logger().info(
                            ":key: Target field %s will be set to PK for %s column FK",
                            fk_target_field_id,
                            column_name,
                        )
                        body_fk_target_field = {
                            semantic_type_key: "type/PK",
                        }
                        self.metadata.update(
                            entity=fk_target_field, delta=body_fk_target_field
                        )
                    else:
                        logger().info(
                            ":thumbs_up: Target field %s is already PK, needed for %s column",
                            fk_target_field_id,
                            column_name,
                        )
                else:
                    logger().error(
                        ":cross_mark: Unable to find foreign key target %s.%s",
                        target_table,
                        target_field,
                    )

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
            self.metadata.update(entity=api_field, delta=body_field)
            logger().info(
                ":sparkles: Field %s.%s will be updated", model_name, column_name
            )
        else:
            logger().info(
                ":thumbs_up: Field %s.%s is up-to-date", model_name, column_name
            )

    def find_database_id(self, name: str) -> Optional[str]:
        """Finds Metabase database ID by name.

        Arguments:
            name {str} -- Metabase database name.

        Returns:
            str -- Metabase database ID.
        """

        for database in self.api("get", "/api/database"):
            if database["name"].upper() == name.upper():
                return database["id"]
        return None

    def build_metadata(self, database_id: str) -> _Metadata:
        """Builds metadata lookups.

        Arguments:
            database_id {str} -- Metabase database ID.

        Returns:
            _Metadata -- Metadata lookup object.
        """

        tables = []

        metadata = self.api(
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

            tables.append(table)

        return MetabaseClient._Metadata(tables)

    def extract_exposures(
        self,
        models: List[MetabaseModel],
        output_path: str = ".",
        output_name: str = "metabase_exposures",
        include_personal_collections: bool = True,
        collection_excludes: Iterable = None,
    ) -> Mapping:
        """Extracts exposures in Metabase downstream of dbt models and sources as parsed by dbt reader

        Arguments:
            models {List[MetabaseModel]} -- List of models as output by dbt reader

        Keyword Arguments:
            output_path {str} -- The path to output the generated yaml. (default: ".")
            output_name {str} -- The name of the generated yaml. (default: {"metabase_exposures"})
            include_personal_collections {bool} -- Include personal collections in Metabase processing. (default: {True})
            collection_excludes {str} -- List of collections to exclude by name. (default: {None})

        Returns:
            List[Mapping] -- JSON object representation of all exposures parsed.
        """

        _RESOURCE_VERSION = 2

        class DbtDumper(yaml.Dumper):
            def increase_indent(self, flow=False, indentless=False):
                indentless = False
                return super(DbtDumper, self).increase_indent(flow, indentless)

        if collection_excludes is None:
            collection_excludes = []

        refable_models = {node.name.upper(): node.ref for node in models}

        self.collections = self.api("get", "/api/collection")
        self.tables = self.api("get", "/api/table")
        self.table_map = {table["id"]: table["name"] for table in self.tables}

        documented_exposure_names = []
        parsed_exposures = []

        for collection in self.collections:

            # Exclude collections by name
            if collection["name"] in collection_excludes:
                continue

            # Optionally exclude personal collections
            if not include_personal_collections and collection.get("personal_owner_id"):
                continue

            # Iter through collection
            logger().info(":sparkles: Exploring collection %s", collection["name"])
            for item in self.api("get", f"/api/collection/{collection['id']}/items"):

                # Ensure collection item is of parsable type
                exposure_type = item["model"]
                exposure_id = item["id"]
                if exposure_type not in ("card", "dashboard"):
                    continue

                # Prepare attributes for population through _extract_card_exposures calls
                self.models_exposed = []
                self.native_query = ""
                native_query = ""

                exposure = self.api("get", f"/api/{exposure_type}/{exposure_id}")
                exposure_name = exposure.get("name", "Exposure [Unresolved Name]")
                logger().info(
                    "\n:bow_and_arrow: Introspecting exposure: %s",
                    exposure_name,
                )

                # Process exposure
                if exposure_type == "card":

                    # Build header for card and extract models to self.models_exposed
                    header = "### Visualization: {}\n\n".format(
                        exposure.get("display", "Unknown").title()
                    )

                    # Parse Metabase question
                    self._extract_card_exposures(exposure_id, exposure, refable_models)
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
                        self._extract_card_exposures(
                            dashboard_item_reference["id"],
                            refable_models=refable_models,
                        )

                if not self.models_exposed:
                    logger().info(":bow: No models mapped to exposure")

                # Extract creator info
                if "creator" in exposure:
                    creator_email = exposure["creator"]["email"]
                    creator_name = exposure["creator"]["common_name"]
                elif "creator_id" in exposure:
                    # If a metabase user is deactivated, the API returns a 404
                    try:
                        creator = self.api("get", f"/api/user/{exposure['creator_id']}")
                    except requests.exceptions.HTTPError as error:
                        creator = {}
                        if error.response.status_code != 404:
                            raise

                    creator_email = creator.get("email")
                    creator_name = creator.get("common_name")

                # No spaces allowed in model names in dbt docs DAG / No duplicate model names
                exposure_name = exposure_name.replace(" ", "_")
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
                        header=header,
                        created_at=exposure["created_at"],
                        creator_name=creator_name,
                        creator_email=creator_email,
                        refable_models=refable_models,
                        description=exposure.get("description", ""),
                        native_query=native_query,
                    )
                )

                documented_exposure_names.append(exposure_name)

        # Output dbt YAML
        with open(
            os.path.expanduser(os.path.join(output_path, f"{output_name}.yml")),
            "w",
            encoding="utf-8",
        ) as docs:
            yaml.dump(
                {"version": _RESOURCE_VERSION, "exposures": parsed_exposures},
                docs,
                Dumper=DbtDumper,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )

        # Return object
        return {"version": _RESOURCE_VERSION, "exposures": parsed_exposures}

    def _extract_card_exposures(
        self,
        card_id: int,
        exposure: Optional[Mapping] = None,
        refable_models: Optional[MutableMapping] = None,
    ):
        """Extracts exposures from Metabase questions populating `self.models_exposed`

        Arguments:
            card_id {int} -- Id of Metabase question used to pull question from api

        Keyword Arguments:
            exposure {str} -- JSON api response from a question in Metabase, allows us to use the object if already in memory

        Returns:
            None -- self.models_exposed is populated through this method.
        """

        if refable_models is None:
            refable_models = {}

        # If an exposure is not passed, pull from id
        if not exposure:
            exposure = self.api("get", f"/api/card/{card_id}")

        query = exposure.get("dataset_query", {})

        if query.get("type") == "query":
            # Metabase GUI derived query
            source_table_id = query.get("query", {}).get(
                "source-table", exposure.get("table_id")
            )

            if str(source_table_id).startswith("card__"):
                # Handle questions based on other question in virtual db
                self._extract_card_exposures(
                    int(source_table_id.split("__")[-1]), refable_models=refable_models
                )
            else:
                # Normal question
                source_table = self.table_map.get(source_table_id)
                if source_table:
                    logger().info(
                        ":direct_hit: Model extracted from Metabase question: %s",
                        source_table,
                    )
                    self.models_exposed.append(source_table)

            # Find models exposed through joins
            for query_join in query.get("query", {}).get("joins", []):

                # Handle questions based on other question in virtual db
                if str(query_join.get("source-table", "")).startswith("card__"):
                    self._extract_card_exposures(
                        int(query_join.get("source-table").split("__")[-1]),
                        refable_models=refable_models,
                    )
                    continue

                # Joined model parsed
                joined_table = self.table_map.get(query_join.get("source-table"))
                if joined_table:
                    logger().info(
                        ":direct_hit: Model extracted from Metabase question join: %s",
                        joined_table,
                    )
                    self.models_exposed.append(joined_table)

        elif query.get("type") == "native":
            # Metabase native query
            native_query = query.get("native").get("query")
            ctes: List[str] = []

            # Parse common table expressions for exclusion
            for matched_cte in re.findall(self.cte_parser, native_query):
                ctes.extend(group.upper() for group in matched_cte if group)

            # Parse SQL for exposures through FROM or JOIN clauses
            for sql_ref in re.findall(self.exposure_parser, native_query):

                # Grab just the table / model name
                clean_exposure = sql_ref.split(".")[-1].strip('"').upper()

                # Scrub CTEs (qualified sql_refs can not reference CTEs)
                if clean_exposure in ctes and "." not in sql_ref:
                    continue
                # Verify this is one of our parsed refable models so exposures dont break the DAG
                if not refable_models.get(clean_exposure):
                    continue

                if clean_exposure:
                    logger().info(
                        ":direct_hit: Model extracted from native query: %s",
                        clean_exposure,
                    )
                    self.models_exposed.append(clean_exposure)
                    self.native_query = native_query

    def _build_exposure(
        self,
        exposure_type: str,
        exposure_id: int,
        name: str,
        header: str,
        created_at: str,
        creator_name: str,
        creator_email: str,
        refable_models: Mapping,
        description: str = "",
        native_query: str = "",
    ) -> Mapping:
        """Builds an exposure object representation as defined here: https://docs.getdbt.com/reference/exposure-properties

        Arguments:
            exposure_type {str} -- Model type in Metabase being either `card` or `dashboard`
            exposure_id {str} -- Card or Dashboard id in Metabase
            name {str} -- Name of exposure as the title of the card or dashboard in Metabase
            header {str} -- The header goes at the top of the description and is useful for prefixing metadata
            created_at {str} -- Timestamp of exposure creation derived from Metabase
            creator_name {str} -- Creator name derived from Metabase
            creator_email {str} -- Creator email derived from Metabase
            refable_models {str} -- List of dbt models from dbt parser which can validly be referenced, parsed exposures are always checked against this list to avoid generating invalid yaml

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
            "description": description,
            "type": "analysis" if exposure_type == "card" else "dashboard",
            "url": f"{self.base_url}/{exposure_type}/{exposure_id}",
            "maturity": "medium",
            "owner": {
                "name": creator_name,
                "email": creator_email or "",
            },
            "depends_on": [
                refable_models[exposure.upper()]
                for exposure in list({m for m in self.models_exposed})
                if exposure.upper() in refable_models
            ],
        }

    def api(
        self,
        method: str,
        path: str,
        critical: bool = True,
        **kwargs,
    ) -> Mapping:
        """Unified way of calling Metabase API.

        Arguments:
            method {str} -- HTTP verb, e.g. get, post, put.
            path {str} -- Relative path of endpoint, e.g. /api/database.

        Keyword Arguments:
            authenticated {bool} -- Includes session ID when true. (default: {True})
            critical {bool} -- Raise on any HTTP errors. (default: {True})

        Returns:
            Any -- JSON payload of the endpoint.
        """

        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            timeout=15,
            **kwargs,
        )

        if critical:
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                if "json" in kwargs and "password" in kwargs["json"]:
                    logger().error("HTTP request failed. Response: %s", response.text)
                else:
                    logger().error(
                        "HTTP request failed. Payload: %s. Response: %s",
                        kwargs.get("json"),
                        response.text,
                    )
                raise
        elif not response.ok:
            return {}

        response_json = json.loads(response.text)

        # Since X.40.0 responses are encapsulated in "data" with pagination parameters
        if "data" in response_json:
            return response_json["data"]

        return response_json
