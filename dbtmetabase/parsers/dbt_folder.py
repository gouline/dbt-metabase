import re
from pathlib import Path
from typing import List, Mapping, MutableMapping, Optional, Tuple

import yaml

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

        for path in (Path(self.path) / "models").rglob("*.yml"):
            with open(path, "r", encoding="utf-8") as stream:
                schema_file = yaml.safe_load(stream)
                if not schema_file:
                    logger().warning("Skipping empty or invalid YAML: %s", path)
                    continue

                for model in schema_file.get("models", []):
                    model_name = model.get("alias", model["name"]).upper()

                    # Refs will still use file name -- this alias mapping is good for getting the right name in the database
                    if "alias" in model:
                        self.alias_mapping[model_name] = model["name"].upper()

                    logger().info("Processing model: %s", path)

                    if not self.model_selected(model_name):
                        logger().debug(
                            "Skipping %s not included in includes or excluded by excludes",
                            model_name,
                        )
                        continue

                    mb_models.append(
                        self._read_model(
                            model=model,
                            schema=schema,
                            model_type=ModelType.nodes,
                            include_tags=include_tags,
                        )
                    )

                for source in schema_file.get("sources", []):
                    source_schema_name = source.get("schema", source["name"]).upper()

                    if "{{" in source_schema_name and "}}" in source_schema_name:
                        logger().warning(
                            "dbt folder reader cannot resolve Jinja expressions, defaulting to current schema"
                        )
                        source_schema_name = schema

                    elif source_schema_name != schema:
                        logger().debug(
                            "Skipping schema %s not in target schema %s",
                            source_schema_name,
                            schema,
                        )
                        continue

                    for model in source.get("tables", []):
                        model_name = model.get("identifier", model["name"]).upper()

                        # These will be used to resolve our regex parsed source() references
                        if "identifier" in model:
                            self.alias_mapping[model_name] = model["name"].upper()

                        logger().info(
                            "Processing source: %s -- table: %s", path, model_name
                        )

                        if not self.model_selected(model_name):
                            logger().debug(
                                "Skipping %s not included in includes or excluded by excludes",
                                model_name,
                            )
                            continue

                        mb_models.append(
                            self._read_model(
                                model=model,
                                source=source["name"],
                                model_type=ModelType.sources,
                                schema=source_schema_name,
                                include_tags=include_tags,
                            )
                        )

        return mb_models, self.alias_mapping

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

        for column in model.get("columns", []):
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
