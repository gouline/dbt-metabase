from __future__ import annotations

import logging
import time
from abc import ABCMeta, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from ._format import NullValue, safe_name
from .interface import Filter, MetabaseRuntimeError
from .manifest import DEFAULT_SCHEMA, Column, Group, Model

logger = logging.getLogger(__name__)


class ModelsExporterMixin(metaclass=ABCMeta):
    """Abstraction for exporting models."""

    __SYNC_PERIOD = 5

    DEFAULT_SYNC_TIMEOUT = 30

    @abstractmethod
    def read_models(self) -> Iterable[Model]:
        pass

    @abstractmethod
    def metabase_api(self, method: str, path: str, **kwargs) -> Mapping:
        pass

    def export_models(
        self,
        metabase_database: str,
        database_filter: Optional[Filter] = None,
        schema_filter: Optional[Filter] = None,
        model_filter: Optional[Filter] = None,
        skip_sources: bool = False,
        sync_timeout: int = DEFAULT_SYNC_TIMEOUT,
        append_tags: bool = False,
        docs_url: Optional[str] = None,
    ):
        """Exports dbt models to Metabase database schema.

        Args:
            metabase_database (str): Target database in Metabase.
            database_filter (Optional[Filter], optional): Filter dbt manifest by database. Defaults to None.
            schema_filter (Optional[Filter], optional): Filter dbt manifest by schema. Defaults to None.
            model_filter (Optional[Filter], optional): Filter dbt manifest by model. Defaults to None.
            skip_sources (bool, optional): Exclude dbt sources from export. Defaults to False.
            sync_timeout (int, optional): Number of seconds to wait until Metabase schema matches the dbt project. To skip synchronization, set timeout to 0. Defaults to 30.
            append_tags (bool, optional): Append dbt tags to table descriptions. Defaults to False.
            docs_url (Optional[str], optional): URL for dbt docs hosting, to append model links to table descriptions. Defaults to None.
        """

        ctx = _Context()
        success = True

        database_id = None
        for api_database in self.metabase_api("get", "/api/database"):
            if api_database["name"].upper() == metabase_database.upper():
                database_id = api_database["id"]
                break
        if not database_id:
            raise MetabaseRuntimeError(f"Database {metabase_database} not found")

        models = self.__filtered_models(
            models=self.read_models(),
            database_filter=database_filter,
            schema_filter=schema_filter,
            model_filter=model_filter,
            skip_sources=skip_sources,
        )

        self.metabase_api("post", f"/api/database/{database_id}/sync_schema")

        deadline = int(time.time()) + sync_timeout
        synced = False
        while not synced:
            time.sleep(self.__SYNC_PERIOD)

            tables = self.__get_tables(database_id)

            synced = True
            for model in models:
                schema_name = model.schema.upper()
                model_name = model.name.upper()
                table_key = f"{schema_name}.{model_name}"

                table = tables.get(table_key)
                if not table:
                    logger.warning(
                        "Model %s not found in %s schema", table_key, schema_name
                    )
                    synced = False
                    continue

                for column in model.columns:
                    column_name = column.name.upper()

                    field = table.get("fields", {}).get(column_name)
                    if not field:
                        logger.warning(
                            "Column %s not found in %s model", column_name, table_key
                        )
                        synced = False
                        continue

            ctx.tables = tables

            if int(time.time()) > deadline:
                break

        if not synced and sync_timeout:
            raise MetabaseRuntimeError("Unable to sync models between dbt and Metabase")

        for model in models:
            success &= self.__export_model(ctx, model, append_tags, docs_url)

        for update in ctx.updates.values():
            self.metabase_api(
                method="put",
                path=f"/api/{update['kind']}/{update['id']}",
                json=update["body"],
            )
            logger.info(
                "API %s/%s updated successfully: %s",
                update["kind"],
                update["id"],
                ", ".join(update.get("body", {}).keys()),
            )

        if not success:
            raise MetabaseRuntimeError(
                "Model export encountered non-critical errors, check output"
            )

    def __export_model(
        self,
        ctx: _Context,
        model: Model,
        append_tags: bool,
        docs_url: Optional[str],
    ) -> bool:
        """Exports one dbt model to Metabase database schema."""

        success = True

        schema_name = model.schema.upper()
        model_name = model.name.upper()
        table_key = f"{schema_name}.{model_name}"

        api_table = ctx.tables.get(table_key)
        if not api_table:
            logger.error("Table %s does not exist in Metabase", table_key)
            return False

        # Empty strings not accepted by Metabase
        model_display_name = model.display_name or None
        model_description = model.format_description(append_tags, docs_url) or None
        model_points_of_interest = model.points_of_interest or None
        model_caveats = model.caveats or None
        model_visibility = model.visibility_type or None

        body_table = {}

        # Update if specified, otherwise reset one that had been set
        api_display_name = api_table.get("display_name")
        if api_display_name != model_display_name and (
            model_display_name
            or safe_name(api_display_name) != safe_name(api_table.get("name"))
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
            ctx.queue_update(entity=api_table, delta=body_table)
            logger.info("Table %s will be updated", table_key)
        else:
            logger.info("Table %s is up-to-date", table_key)

        for column in model.columns:
            success &= self.__export_column(ctx, schema_name, model_name, column)

        return success

    def __export_column(
        self,
        ctx: _Context,
        schema_name: str,
        model_name: str,
        column: Column,
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

        api_field = ctx.tables.get(table_key, {}).get("fields", {}).get(column_name)
        if not api_field:
            logger.error(
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
                logger.info(
                    "Skipping FK resolution for %s table, %s field not resolved during dbt parsing",
                    table_key,
                    target_field,
                )

            else:
                logger.debug(
                    "Looking for field %s in table %s",
                    target_field,
                    target_table,
                )

                fk_target_field = (
                    ctx.tables.get(target_table, {}).get("fields", {}).get(target_field)
                )
                if fk_target_field:
                    fk_target_field_id = fk_target_field.get("id")
                    if fk_target_field.get(semantic_type_key) != "type/PK":
                        logger.info(
                            "API field/%s will become PK (for %s column FK)",
                            fk_target_field_id,
                            column_name,
                        )
                        body_fk_target_field = {
                            semantic_type_key: "type/PK",
                        }
                        ctx.queue_update(
                            entity=fk_target_field, delta=body_fk_target_field
                        )
                    else:
                        logger.info(
                            "API field/%s is already PK (for %s column FK)",
                            fk_target_field_id,
                            column_name,
                        )
                else:
                    logger.error(
                        "Unable to find PK for %s.%s column FK",
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
        api_display_name = api_field.get("display_name")
        if api_display_name != column_display_name and (
            column_display_name
            or safe_name(api_display_name) != safe_name(api_field.get("name"))
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
        api_semantic_type = api_field.get(semantic_type_key)
        if (column.semantic_type and api_semantic_type != column.semantic_type) or (
            column.semantic_type is NullValue and api_semantic_type
        ):
            body_field[semantic_type_key] = column.semantic_type or None

        if body_field:
            ctx.queue_update(entity=api_field, delta=body_field)
            logger.info("Field %s.%s will be updated", model_name, column_name)
        else:
            logger.info("Field %s.%s is up-to-date", model_name, column_name)

        return success

    def __get_tables(self, database_id: str) -> Mapping[str, MutableMapping]:
        tables = {}

        metadata = self.metabase_api(
            method="get",
            path=f"/api/database/{database_id}/metadata",
            params={"include_hidden": True},
        )

        bigquery_schema = metadata.get("details", {}).get("dataset-id")

        for table in metadata.get("tables", []):
            # table[schema] is null for bigquery datasets
            table["schema"] = (
                table.get("schema") or bigquery_schema or DEFAULT_SCHEMA
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

    def __filtered_models(
        self,
        models: Iterable[Model],
        database_filter: Optional[Filter],
        schema_filter: Optional[Filter],
        model_filter: Optional[Filter],
        skip_sources: bool,
    ) -> Iterable[Model]:
        def selected(m: Model) -> bool:
            return (
                (not skip_sources or m.group != Group.sources)
                and (not database_filter or database_filter.selected(m.database))
                and (not schema_filter or schema_filter.selected(m.schema))
                and (not model_filter or model_filter.selected(m.name))
            )

        return list(filter(selected, models))


class _Context:
    def __init__(self):
        self.tables: Mapping[str, MutableMapping] = {}
        self.updates: MutableMapping[str, MutableMapping[str, Any]] = {}

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
