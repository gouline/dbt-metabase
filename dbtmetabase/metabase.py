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
from .parsers.metrics import MetabaseMetricCompiler

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
            table_schema = table.get("schema", "public").upper()
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
        output_path: str = "./",
        output_name: str = "metabase_exposures",
        include_personal_collections: bool = True,
        exclude_collections: Iterable = None,
    ):

        if exclude_collections is None:
            exclude_collections = []

        refable_models = {node.name: node.ref for node in models}

        self.collections = self.api("get", "/api/collection")
        self.tables = self.api("get", "/api/table")
        self.table_map = {table["id"]: table["name"] for table in self.tables}

        duplicate_name_check = []
        captured_exposures = []

        for collection in self.collections:
            if collection["name"] in exclude_collections:
                continue
            if (
                not include_personal_collections
                and collection.get("personal_owner_id") is not None
            ):
                continue
            logging.info("Exploring collection %s", collection["name"])
            for item in self.api("get", f"/api/collection/{collection['id']}/items"):
                self.models_exposed = []
                self.native_query = ""
                if item["model"] == "card":
                    model = self.api("get", f"/api/{item['model']}/{item['id']}")
                    name = model.get("name", "Indeterminate Card")
                    logging.info("Introspecting card: %s", name)
                    description = model.get("description")
                    creator_email = model.get("creator", {}).get("email")
                    creator_name = model.get("creator", {}).get("common_name")
                    created_at = model.get("created_at")
                    header = "### Visualization: {}\n\n".format(
                        model.get("display", "Unknown").title()
                    )
                    self._extract_card_exposures(model["id"], model)
                elif item["model"] == "dashboard":
                    dashboard = self.api("get", f"/api/{item['model']}/{item['id']}")
                    if "ordered_cards" not in dashboard:
                        continue
                    name = dashboard.get("name", "Indeterminate Dashboard")
                    logging.info("Introspecting dashboard: %s", name)
                    description = dashboard.get("description")
                    creator = self.api("get", f"/api/user/{dashboard['creator_id']}")
                    creator_email = creator["email"]
                    creator_name = creator["common_name"]
                    created_at = dashboard.get("created_at")
                    header = "### Dashboard Cards: {}\n\n".format(
                        str(len(dashboard["ordered_cards"]))
                    )
                    for dashboard_model in dashboard["ordered_cards"]:
                        dashboard_model_ref = dashboard_model.get("card", {})
                        if not "id" in dashboard_model_ref:
                            continue
                        self._extract_card_exposures(dashboard_model_ref["id"])
                else:
                    continue

                # No spaces allowed in model names in dbt docs DAG / No duplicate model names
                name = name.replace(" ", "_")
                enumer = 1
                while name in duplicate_name_check:
                    name = f"{name}_{enumer}"
                    enumer += 1
                duplicate_name_check.append(name)

                # Construct Exposure
                description = (
                    header
                    + (
                        "{}\n\n".format(description.strip())
                        if description
                        else "No description provided in Metabase\n\n"
                    )
                    + (
                        "#### Query\n\n```\n{}\n```\n\n".format(
                            "\n".join(
                                line
                                for line in self.native_query.strip().split("\n")
                                if line.strip() != ""
                            )
                        )
                        if self.native_query
                        else ""
                    )
                    + "#### Metadata\n\n"
                    + ("Metabase Id: __{}__\n\n".format(item["id"]))
                    + ("Created On: __{}__".format(created_at))
                )
                captured_exposure = {
                    "name": name,
                    "description": description,
                    "type": "analysis" if item["model"] == "card" else "dashboard",
                    "url": f"{self.protocol}://{self.host}/{item['model']}/{item['id']}",
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
                captured_exposures.append(captured_exposure)

        # Output dbt YAML
        with open(
            os.path.expanduser(os.path.join(output_path, f"{output_name}.yml")), "w"
        ) as docs:
            yaml.dump(
                {"version": 2, "exposures": captured_exposures},
                docs,
                allow_unicode=True,
                sort_keys=False,
            )
        return {"version": 2, "exposures": captured_exposures}

    def _extract_card_exposures(self, card_id: int, model: Optional[Mapping] = None):
        if not model:
            model = self.api("get", f"/api/card/{card_id}")
        query = model.get("dataset_query", {})
        if query.get("type") == "query":
            source_table_id = query.get("query", {}).get(
                "source-table", model.get("table_id")
            )
            if source_table_id in self.table_map:
                if isinstance(source_table_id, str) and source_table_id.startswith(
                    "card__"
                ):
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

    def sync_metrics(
        self,
        database: str,
        models: List[MetabaseModel],
        aliases: Optional[Mapping] = None,
        revision_header: str = "Metric has been updated. ",
    ):

        if aliases is None:
            aliases = {}

        metabase_metrics = self.api("get", "/api/metric")
        database_id = self.find_database_id(database)

        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return

        table_lookup, field_lookup = self.build_metadata_lookups(database_id)

        metabase_compiler = MetabaseMetricCompiler(field_lookup)

        for model in models:

            if "metabase.metrics" not in model.meta:
                continue

            schema_name = model.schema.upper()
            model_name = model.name.upper()

            lookup_key = f"{schema_name}.{aliases.get(model_name, model_name)}"
            metabase_compiler.current_target = lookup_key
            logging.info("Syncing metrics for %s", lookup_key)

            api_table = table_lookup.get(lookup_key)

            if not api_table:
                logging.error("Table %s does not exist in Metabase", lookup_key)
                continue

            table_id = api_table["id"]

            for metric in model.meta["metabase.metrics"]:
                if "name" not in metric or "metric" not in metric:
                    logging.warning(
                        "Invalid metric %s in model %s", metric["name"], lookup_key
                    )
                    continue
                logging.debug("Constructing metric from %s", metric)
                metric_name = metric["name"]
                metric_compiled = metabase_compiler.transpile_expression(
                    metric["metric"]
                )
                logging.debug("Metric %s compiled to %s", metric_name, metric_compiled)
                metric_description = metric.get(
                    "description", "No description provided"
                )
                compiled = {
                    "name": metric_name,
                    "description": metric_description,
                    "table_id": table_id,
                    "definition": {
                        "source-table": table_id,
                        "aggregation": [
                            [
                                "aggregation-options",
                                metric_compiled,
                                {"display-name": metric_name},
                            ]
                        ],
                    },
                }
                metric_filter = metric.get("filter")
                if metric_filter:
                    metric_filter = metabase_compiler.transpile_expression(
                        metric_filter
                    )
                    compiled["definition"]["filter"] = metric_filter
                this_metric = None
                for existing_metric in metabase_metrics:
                    if (
                        metric_name == existing_metric["name"]
                        and table_id == existing_metric["table_id"]
                    ):
                        if this_metric is not None:
                            logging.error("Duplicate metric in model %s", lookup_key)
                        logging.info(
                            "Existing metric %s found for %s", metric_name, lookup_key
                        )
                        this_metric = existing_metric
                if this_metric:
                    # Revise
                    agglomerate_changes = ""
                    # Check Name, Description, Table Id, and Definition
                    if this_metric["name"] != compiled["name"]:
                        agglomerate_changes += f'Name changed from {this_metric["name"]} to {compiled["name"]}. '
                    if this_metric["description"] != compiled["description"]:
                        agglomerate_changes += f'Description changed from {this_metric["description"]} to {compiled["description"]}. '
                    if this_metric["table_id"] != compiled["table_id"]:
                        agglomerate_changes += f'Table Id changed from {this_metric["table_id"]} to {compiled["table_id"]}. '
                    if this_metric["definition"] != compiled["definition"]:
                        agglomerate_changes += (
                            f'Formula definiton updated to {metric["metric"]}'
                        )
                    if agglomerate_changes:
                        compiled["revision_message"] = (
                            revision_header + agglomerate_changes
                        )
                        output_metric = self.api(
                            "put", f"/api/metric/{this_metric['id']}", json=compiled
                        )
                        logging.info("Metric %s updated!", metric_name)
                        logging.debug(output_metric)
                    else:
                        logging.info("No changes to %s", metric_name)
                else:
                    # Create
                    output_metric = self.api("post", "/api/metric/", json=compiled)
                    logging.info("Metric %s created!", metric_name)
                    logging.debug(output_metric)

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
