import json
import logging
from typing import (
    Sequence,
    Optional,
    Tuple,
    Iterable,
    MutableMapping,
    Union,
    List,
    Mapping,
)

import requests
import time

from .models.metabase import MetabaseModel, MetabaseColumn

import re
import yaml
import os


class MetabaseClient:
    """Metabase API client."""

    _SYNC_PERIOD_SECS = 5

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        use_http: bool = False,
        verify: Union[str, bool] = None,
    ):
        """Constructor.

        Arguments:
            host {str} -- Metabase hostname.
            user {str} -- Metabase username.
            password {str} -- Metabase password.

        Keyword Arguments:
            use_http {bool} -- Use HTTP instead of HTTPS. (default: {False})
            verify {Union[str, bool]} -- Path to certificate or disable verification. (default: {None})
        """

        self.host = host
        self.protocol = "http" if use_http else "https"
        self.verify = verify
        self.session_id = self.get_session_id(user, password)
        self.collections: Iterable = []
        self.tables: Iterable = []
        self.table_map: MutableMapping = {}
        self.models_exposed: List = []
        self.native_query: str = ""
        self.exposure_parser = re.compile(r"[FfJj][RrOo][OoIi][MmNn]\s+\b(\w+)\b")
        self.cte_parser = re.compile(
            r"[Ww][Ii][Tt][Hh]\s+\b(\w+)\b\s+as|[)]\s*[,]\s*\b(\w+)\b\s+as"
        )
        logging.info("Session established successfully")

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
            authenticated=False,
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

        Keyword Arguments:
            timeout {int} -- Timeout before giving up in seconds. (default: {30})

        Returns:
            bool -- True if schema compatible with models, false if still incompatible.
        """
        if timeout is None:
            timeout = 30

        if timeout < self._SYNC_PERIOD_SECS:
            logging.critical(
                "Timeout provided %d secs, must be at least %d",
                timeout,
                self._SYNC_PERIOD_SECS,
            )
            return False

        database_id = self.find_database_id(database)
        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return False

        self.api("post", f"/api/database/{database_id}/sync_schema")

        deadline = int(time.time()) + timeout
        sync_successful = False
        while True:
            sync_successful = self.models_compatible(database_id, models)
            time_after_wait = int(time.time()) + self._SYNC_PERIOD_SECS
            if not sync_successful and time_after_wait <= deadline:
                time.sleep(self._SYNC_PERIOD_SECS)
            else:
                break
        return sync_successful

    def models_compatible(self, database_id: str, models: Sequence) -> bool:
        """Checks if models compatible with the Metabase database schema.

        Arguments:
            database_id {str} -- Metabase database ID.
            models {list} -- List of dbt models read from project.

        Returns:
            bool -- True if schema compatible with models, false otherwise.
        """

        _, field_lookup = self.build_metadata_lookups(database_id)

        are_models_compatible = True
        for model in models:

            schema_name = model.schema.upper()
            model_name = model.name.upper()

            lookup_key = f"{schema_name}.{model_name}"

            if lookup_key not in field_lookup:
                logging.warning(
                    "Model %s not found in %s schema", lookup_key, schema_name
                )
                are_models_compatible = False
            else:
                table_lookup = field_lookup[lookup_key]
                for column in model.columns:
                    column_name = column.name.upper()
                    if column_name not in table_lookup:
                        logging.warning(
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

        database_id = self.find_database_id(database)
        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return

        table_lookup, field_lookup = self.build_metadata_lookups(database_id)

        for model in models:
            self.export_model(model, table_lookup, field_lookup, aliases)

    def export_model(
        self,
        model: MetabaseModel,
        table_lookup: dict,
        field_lookup: dict,
        aliases: dict,
    ):
        """Exports one dbt model to Metabase database schema.

        Arguments:
            model {dict} -- One dbt model read from project.
            table_lookup {dict} -- Dictionary of Metabase tables indexed by name.
            field_lookup {dict} -- Dictionary of Metabase fields indexed by name, indexed by table name.
            aliases {dict} -- Provided by reader class. Shuttled down to column exports to resolve FK refs against relations to aliased source tables
        """

        schema_name = model.schema.upper()
        model_name = model.name.upper()

        lookup_key = f"{schema_name}.{aliases.get(model_name, model_name)}"

        api_table = table_lookup.get(lookup_key)
        if not api_table:
            logging.error("Table %s does not exist in Metabase", lookup_key)
            return

        # Empty strings not accepted by Metabase
        if not model.description:
            model_description = None
        else:
            model_description = model.description

        table_id = api_table["id"]
        if api_table["description"] != model_description and model_description:
            # Update with new values
            self.api(
                "put",
                f"/api/table/{table_id}",
                json={"description": model_description},
            )
            logging.info("Updated table %s successfully", lookup_key)
        elif not model_description:
            logging.info("No model description provided for table %s", lookup_key)
        else:
            logging.info("Table %s is up-to-date", lookup_key)

        for column in model.columns:
            self.export_column(schema_name, model_name, column, field_lookup, aliases)

    def export_column(
        self,
        schema_name: str,
        model_name: str,
        column: MetabaseColumn,
        field_lookup: dict,
        aliases: dict,
    ):
        """Exports one dbt column to Metabase database schema.

        Arguments:
            model_name {str} -- One dbt model name read from project.
            column {dict} -- One dbt column read from project.
            field_lookup {dict} -- Dictionary of Metabase fields indexed by name, indexed by table name.
            aliases {dict} -- Provided by reader class. Used to resolve FK refs against relations to aliased source tables
        """

        table_lookup_key = f"{schema_name}.{model_name}"
        column_name = column.name.upper()

        field = field_lookup.get(table_lookup_key, {}).get(column_name)
        if not field:
            logging.error(
                "Field %s.%s does not exist in Metabase", table_lookup_key, column_name
            )
            return

        field_id = field["id"]

        api_field = self.api("get", f"/api/field/{field_id}")

        if "special_type" in api_field:
            semantic_type = "special_type"
        else:
            semantic_type = "semantic_type"

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
                logging.info(
                    "Passing on fk resolution for %s. Target field %s was not resolved during dbt model parsing.",
                    table_lookup_key,
                    target_field,
                )

            else:
                # Now we can trust our parse_ref even if it is pointing to something like source("salesforce", "my_cool_table_alias")
                # just as easily as a simple ref("stg_salesforce_cool_table") -> the dict is empty if parsing from manifest.json
                was_aliased = (
                    aliases.get(target_table.split(".", 1)[-1])
                    if target_table
                    else None
                )
                if was_aliased:
                    target_table = ".".join(
                        [target_table.split(".", 1)[0], was_aliased]
                    )

                logging.info(
                    "Looking for field %s in table %s", target_field, target_table
                )
                fk_target_field_id = (
                    field_lookup.get(target_table, {}).get(target_field, {}).get("id")
                )

                if fk_target_field_id:
                    logging.info(
                        "Setting target field %s to PK in order to facilitate FK ref for %s column",
                        fk_target_field_id,
                        column_name,
                    )
                    self.api(
                        "put",
                        f"/api/field/{fk_target_field_id}",
                        json={semantic_type: "type/PK"},
                    )
                else:
                    logging.error(
                        "Unable to find foreign key target %s.%s",
                        target_table,
                        target_field,
                    )

        # Nones are not accepted, default to normal
        if not column.visibility_type:
            column.visibility_type = "normal"

        # Empty strings not accepted by Metabase
        if not column.description:
            column_description = None
        else:
            column_description = column.description

        if (
            api_field["description"] != column_description
            or api_field[semantic_type] != column.semantic_type
            or api_field["visibility_type"] != column.visibility_type
            or api_field["fk_target_field_id"] != fk_target_field_id
        ):
            # Update with new values
            self.api(
                "put",
                f"/api/field/{field_id}",
                json={
                    "description": column_description,
                    semantic_type: column.semantic_type,
                    "visibility_type": column.visibility_type,
                    "fk_target_field_id": fk_target_field_id,
                },
            )
            logging.info("Updated field %s.%s successfully", model_name, column_name)
        else:
            logging.info("Field %s.%s is up-to-date", model_name, column_name)

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

    def build_metadata_lookups(
        self, database_id: str, schemas_to_exclude: Iterable = None
    ) -> Tuple[dict, dict]:
        """Builds table and field lookups.

        Arguments:
            database_id {str} -- Metabase database ID.

        Returns:
            dict -- Dictionary of tables indexed by name.
            dict -- Dictionary of fields indexed by name, indexed by table name.
        """

        if schemas_to_exclude is None:
            schemas_to_exclude = []

        table_lookup = {}
        field_lookup = {}

        metadata = self.api(
            "get",
            f"/api/database/{database_id}/metadata",
            params=dict(include_hidden=True),
        )
        for table in metadata.get("tables", []):
            table_schema = table.get("schema")
            table_schema = table_schema.upper() if table_schema else "PUBLIC"
            table_name = table["name"].upper()

            if schemas_to_exclude:
                schemas_to_exclude = {
                    exclusion.upper() for exclusion in schemas_to_exclude
                }

                if table_schema in schemas_to_exclude:
                    logging.debug(
                        "Ignoring Metabase table %s in schema %s. It belongs to excluded schemas %s",
                        table_name,
                        table_schema,
                        schemas_to_exclude,
                    )
                    continue

            lookup_key = f"{table_schema}.{table_name}"
            table_lookup[lookup_key] = table
            table_field_lookup = {}

            for field in table.get("fields", []):
                field_name = field["name"].upper()
                table_field_lookup[field_name] = field

            field_lookup[lookup_key] = table_field_lookup

        return table_lookup, field_lookup

    def extract_exposures(
        self,
        models: List[MetabaseModel],
        output_path: str = ".",
        output_name: str = "metabase_exposures",
        include_personal_collections: bool = True,
        collection_excludes: Iterable = None,
    ) -> Mapping:

        _RESOURCE_VERSION = 2

        class DbtDumper(yaml.Dumper):
            def increase_indent(self, flow=False, indentless=False):
                indentless = False
                return super(DbtDumper, self).increase_indent(flow, indentless)

        if collection_excludes is None:
            collection_excludes = []

        refable_models = {node.name: node.ref for node in models}

        self.collections = self.api("get", "/api/collection")
        self.tables = self.api("get", "/api/table")
        self.table_map = {table["id"]: table["name"] for table in self.tables}

        documented_exposures = []
        parsed_exposures = []

        for collection in self.collections:

            # Exclude collections by name
            if collection["name"] in collection_excludes:
                continue

            # Optionally exclude personal collections
            if not include_personal_collections and collection.get("personal_owner_id"):
                continue

            # Iter through collection
            logging.info("Exploring collection %s", collection["name"])
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
                logging.info("Introspecting exposure: %s", exposure_name)

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

                # Extract creator info
                if "creator" in exposure:
                    creator_email = exposure["creator"]["email"]
                    creator_name = exposure["creator"]["common_name"]
                elif "creator_id" in exposure:
                    creator = self.api("get", f"/api/user/{exposure['creator_id']}")
                    creator_email = creator["email"]
                    creator_name = creator["common_name"]

                # No spaces allowed in model names in dbt docs DAG / No duplicate model names
                exposure_name = exposure_name.replace(" ", "_")
                enumer = 1
                while exposure_name in documented_exposures:
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
                        description=exposure.get("description"),
                        native_query=native_query,
                    )
                )

                documented_exposures.append(exposure_name)

        # Output dbt YAML
        with open(
            os.path.expanduser(os.path.join(output_path, f"{output_name}.yml")), "w"
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

    def _extract_card_exposures(self, card_id: int, model: Optional[Mapping] = None):
        if not model:
            model = self.api("get", f"/api/card/{card_id}")
        query = model.get("dataset_query", {})
        if query.get("type") == "query":
            source_table_id = query.get("query", {}).get(
                "source-table", model.get("table_id")
            )
            if source_table_id in self.table_map:
                if str(source_table_id).startswith("card__"):
                    self._extract_card_exposures(int(source_table_id.split("__")[-1]))
                else:
                    source_table = self.table_map[source_table_id]
                    logging.info(
                        "Model extracted from Metabase question: %s",
                        source_table,
                    )
                self.models_exposed.append(source_table)
                for query_join in query.get("query", {}).get("joins", []):
                    if isinstance(
                        query_join.get("source-table"), str
                    ) and query_join.get("source-table").startswith("card__"):
                        self._extract_card_exposures(
                            int(query_join.get("source-table").split("__")[-1])
                        )
                        continue
                    joined_table = self.table_map[query_join.get("source-table")]
                    logging.info(
                        "Model extracted from Metabase question join: %s",
                        joined_table,
                    )
                    self.models_exposed.append(joined_table)
        elif query.get("type") == "native":
            native_query = query.get("native").get("query")
            ctes = []
            for cte in re.findall(self.cte_parser, native_query):
                ctes.extend(cte)
            for exposure in re.findall(self.exposure_parser, native_query):
                clean_exposure = exposure.split(".")[-1].strip('"')
                if clean_exposure in ctes:
                    continue
                logging.info(
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
            native_query = "#### Query \n\n```\n{}\n```\n\n".format(
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
            "#### Metadata \n\n"
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
            "url": f"{self.protocol}://{self.host}/{exposure_type}/{exposure_id}",
            "maturity": "medium",
            "owner": {
                "name": creator_name,
                "email": creator_email,
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
        authenticated: bool = True,
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

        headers: MutableMapping = {}
        if "headers" not in kwargs:
            kwargs["headers"] = headers
        else:
            headers = kwargs["headers"].copy()

        if authenticated:
            headers["X-Metabase-Session"] = self.session_id

        response = requests.request(
            method, f"{self.protocol}://{self.host}{path}", verify=self.verify, **kwargs
        )

        if critical:
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                if "password" in kwargs["json"]:
                    logging.error("HTTP request failed. Response: %s", response.text)
                else:
                    logging.error(
                        "HTTP request failed. Payload: %s. Response: %s",
                        kwargs["json"],
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
