import subprocess
import re
from typing import List, Mapping, MutableMapping, Optional, Tuple

import json

from ..logger.logging import logger
from ..models.metabase import (
    METABASE_COLUMN_META_FIELDS,
    METABASE_MODEL_DEFAULT_SCHEMA,
    METABASE_MODEL_META_FIELDS,
    MetabaseColumn,
    MetabaseModel,
    ModelType,
)
from .dbt import DbtReader


class DbtFolderReader(DbtReader):
    """
    Reader for dbt project configuration.
    """

    def read_models(
        self,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        """Reads dbt models in Metabase-friendly format.

        Keyword Arguments:
            include_tags {bool} -- Append dbt model tags to dbt model descriptions. (default: {True})
            docs_url {Optional[str]} -- Append dbt docs url to dbt model description

        Returns:
            list -- List of dbt models in Metabase-friendly format.
        """

        mb_models: List[MetabaseModel] = []
        schema = self.schema or METABASE_MODEL_DEFAULT_SCHEMA
        resource_type_to_model_type = {
            "model": ModelType.nodes,
            "source": ModelType.sources,
        }

        for dbt_table in self._get_dbt_tables():
            table_name = dbt_table.get("name")
            table_schema = dbt_table.get("schema")
            table_resource_type = dbt_table.get("resource_type")

            logger().info("Processing dbt table: %s", table_name)

            if not self.model_selected(table_name):
                logger().info(
                    "Skipping %s not included in includes or excluded by excludes",
                    table_name,
                )
                continue

            if (
                table_resource_type == "source"
                and table_schema.lower() != schema.lower()
            ):
                logger().debug(
                    "Skipping schema %s not in target schema %s",
                    table_schema,
                    schema,
                )
                continue

            mb_models.append(
                self._read_model(
                    model=dbt_table,
                    source=dbt_table.get("source_name"),
                    schema=schema,
                    model_type=resource_type_to_model_type[table_resource_type],
                    include_tags=include_tags,
                )
            )

        return mb_models, self.alias_mapping

    def _get_dbt_tables(self) -> List[str]:
        get_dbt_tables_cli_args = [
            "dbt",
            "ls",
            "--resource-types",
            "model",
            "source",
            "--output",
            "json",
            "--output-keys",
            "resource_type",
            "name",
            "alias",
            "identifier",
            "schema",
            "description",
            "columns",
            "tags",
            "source_name",
        ]
        try:
            dbt_table_json_lines = (
                subprocess.run(
                    get_dbt_tables_cli_args,
                    cwd=self.path,
                    capture_output=True,
                    check=True,
                )
                .stdout.decode("utf-8")
                .splitlines()
            )
        except subprocess.CalledProcessError as e:
            logger().error(
                "Error running dbt ls command. Please make sure dbt is installed and configured correctly: %s",
                e.stdout.decode("utf-8"),
            )
            raise e
        dbt_table_jsons = []
        for dbt_table in dbt_table_json_lines:
            try:
                dbt_table_json = json.loads(dbt_table)
                if type(dbt_table_json) == dict:
                    dbt_table_jsons.append(dbt_table_json)
            # This could happen because dbt sends some other stuff to stdout ("Running dbt=1.7.0", etc.).
            except json.decoder.JSONDecodeError:
                continue
        return dbt_table_jsons

    def _read_model(
        self,
        model: dict,
        schema: str,
        source: Optional[str] = None,
        model_type: ModelType = ModelType.nodes,
        include_tags: bool = True,
    ) -> MetabaseModel:
        """Reads one dbt model in Metabase-friendly format.

        Arguments:
            model {dict} -- One dbt model to read.
            schema {str} -- Schema as passed doen from CLI args or parsed from `source`
            source {str, optional} -- Name of the source if source
            model_type {str} -- The type of the node which can be one of either nodes or sources
            include_tags: {bool} -- Flag to append tags to description of model

        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        metabase_columns: List[MetabaseColumn] = []

        columns = model.get("columns", {}).values()
        for column in columns:
            metabase_columns.append(self._read_column(column, schema))

        description = model.get("description", "")
        if include_tags:
            tags = model.get("tags", [])
            if tags:
                tags = ", ".join(tags)
                if description:
                    description += "\n\n"
                description += f"Tags: {tags}"

        # Resolved name is what the name will be in the database
        resolved_name = model.get("alias", model.get("identifier"))
        dbt_name = None
        if not resolved_name:
            resolved_name = model["name"]
        else:
            dbt_name = model["name"]

        return MetabaseModel(
            name=resolved_name,
            schema=schema,
            description=description,
            columns=metabase_columns,
            model_type=model_type,
            source=source,
            dbt_name=dbt_name,
            **self.read_meta_fields(model, METABASE_MODEL_META_FIELDS),
        )

    def _read_column(self, column: Mapping, schema: str) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.
            schema {str} -- Schema as passed down from CLI args or parsed from `source`

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        column_name = column.get("name", "").upper().strip('"')
        column_description = column.get("description")

        metabase_column = MetabaseColumn(
            name=column_name,
            description=column_description,
            **self.read_meta_fields(column, METABASE_COLUMN_META_FIELDS),
        )

        fk_target_table = None
        fk_target_field = None

        for test in column.get("tests") or []:
            if isinstance(test, dict):
                if "relationships" in test:
                    relationships = test["relationships"]
                    fk_target_table = self.parse_ref(relationships["to"])
                    if not fk_target_table:
                        logger().warning(
                            "Could not resolve foreign key target table for column %s",
                            metabase_column.name,
                        )
                        continue
                    fk_target_field = relationships["field"]

        self.set_column_foreign_key(
            column=column,
            metabase_column=metabase_column,
            table=fk_target_table,
            field=fk_target_field,
            schema=schema,
        )

        return metabase_column

    @staticmethod
    def parse_ref(text: str) -> Optional[str]:
        """Parses dbt ref() or source() statement.

        Arguments:
            text {str} -- Full statement in dbt YAML.

        Returns:
            str -- Name of the reference.
        """

        # We are catching the rightmost argument of either source or ref which is ultimately the table name
        matches = re.findall(r"['\"]([\w\_\-\ ]+)['\"][ ]*\)$", text.strip())
        if matches:
            logger().debug("%s -> %s", text, matches[0])
            return matches[0]
        return None
