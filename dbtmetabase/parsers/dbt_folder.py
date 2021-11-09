import os
import re
import yaml
from pathlib import Path
from typing import List, Iterable, Mapping, MutableMapping, Optional, Tuple

from ..models.config import DbtConfig
from ..models.metabase import METABASE_META_FIELDS, ModelType
from ..models.metabase import MetabaseModel, MetabaseColumn
from ..logger.logging import logger


class DbtFolderReader:
    """
    Reader for dbt project configuration.
    """

    def __init__(self, project_path: str):
        """Constructor.

        Arguments:
            project_path {str} -- Path to dbt project root.
        """

        self.project_path = os.path.expanduser(project_path)
        self.alias_mapping: MutableMapping = {}

    def read_models(
        self,
        dbt_config: DbtConfig,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        """Reads dbt models in Metabase-friendly format.

        Keyword Arguments:
            dbt_config {Dbt} -- Dbt object
            include_tags {bool} -- Append dbt model tags to dbt model descriptions. (default: {True})
            docs_url {Optional[str]} -- Append dbt docs url to dbt model description

        Returns:
            list -- List of dbt models in Metabase-friendly format.
        """

        database = dbt_config.database
        schema = dbt_config.schema
        schema_excludes = dbt_config.schema_excludes
        includes = dbt_config.includes
        excludes = dbt_config.excludes

        if schema_excludes is None:
            schema_excludes = []
        if includes is None:
            includes = []
        if excludes is None:
            excludes = []
        if schema is None:
            schema = "public"

        # Args that allow API interface for both readers to be interchangeable while passing CI
        del database, docs_url

        mb_models: List[MetabaseModel] = []

        for path in (Path(self.project_path) / "models").rglob("*.yml"):
            with open(path, "r", encoding="utf-8") as stream:
                schema_file = yaml.safe_load(stream)
                if schema_file is None:
                    logger().warning("Skipping empty or invalid YAML: %s", path)
                    continue
                for model in schema_file.get("models", []):
                    name = model.get("alias", model["name"])
                    # Refs will still use file name -- this alias mapping is good for getting the right name in the database
                    if "alias" in model:
                        self.alias_mapping[name] = model["name"]
                    logger().info("\nProcessing model: %s", path)
                    if (not includes or name in includes) and (name not in excludes):
                        mb_models.append(
                            self._read_model(
                                model=model,
                                schema=schema.upper(),
                                model_type=ModelType.nodes,
                                include_tags=include_tags,
                            )
                        )
                        logger().debug(mb_models[-1].ref)
                for source in schema_file.get("sources", []):
                    source_schema_name = source.get("schema", source["name"])
                    if "{{" in source_schema_name and "}}" in source_schema_name:
                        logger().warning(
                            "dbt Folder Reader cannot resolve jinja expressions- use the Manifest Reader instead."
                        )
                        source_schema_name = schema
                    if source_schema_name.upper() != schema.upper():
                        continue
                    for model in source.get("tables", []):
                        name = model.get("identifier", model["name"])
                        # These will be used to resolve our regex parsed source() references
                        if "identifier" in model:
                            self.alias_mapping[name] = model["name"]
                        logger().info(
                            "\nProcessing source: %s -- table: %s", path, name
                        )
                        if (not includes or name in includes) and (
                            name not in excludes
                        ):
                            mb_models.append(
                                self._read_model(
                                    model=model,
                                    source=source["name"],
                                    model_type=ModelType.sources,
                                    schema=source_schema_name.upper(),
                                    include_tags=include_tags,
                                )
                            )
                            logger().debug(mb_models[-1].ref)

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
        meta = model.get("meta", {})
        points_of_interest = meta.get("metabase.points_of_interest")
        caveats = meta.get("metabase.caveats")

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
            points_of_interest=points_of_interest,
            caveats=caveats,
            columns=metabase_columns,
            model_type=model_type,
            source=source,
            dbt_name=dbt_name,
        )

    def _read_column(self, column: Mapping, schema: str) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.
            schema {str} -- Schema as passed doen from CLI args or parsed from `source`

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        column_name = column.get("name", "").upper().strip('"')
        column_description = column.get("description")
        metabase_column = MetabaseColumn(
            name=column_name,
            description=column_description,
        )

        tests: Optional[Iterable] = column.get("tests", [])
        if tests is None:
            tests = []

        for test in tests:
            if isinstance(test, dict):
                if "relationships" in test:
                    relationships = test["relationships"]
                    parsed_table_ref = self.parse_ref(relationships["to"])
                    if not parsed_table_ref:
                        logger().warning(
                            "Could not resolve foreign key target table for column %s",
                            metabase_column.name,
                        )
                        continue

                    parsed_ref = ".".join(
                        map(
                            lambda s: s.strip('"'),
                            column.get("meta", {})
                            .get("metabase.foreign_key_target_table", "")
                            .split("."),
                        )
                    )
                    if not parsed_ref or "." not in parsed_ref:
                        parsed_ref = f"{schema}.{parsed_table_ref}"

                    metabase_column.semantic_type = "type/FK"
                    metabase_column.fk_target_table = parsed_ref.upper()
                    metabase_column.fk_target_field = (
                        str(relationships["field"]).upper().strip('"')
                    )
                    logger().debug(
                        "Relation from %s to %s.%s",
                        column.get("name", "").upper().strip('"'),
                        metabase_column.fk_target_table,
                        metabase_column.fk_target_field,
                    )

        if "meta" in column:
            meta = column.get("meta", [])
            for field in METABASE_META_FIELDS:
                if f"metabase.{field}" in meta:
                    setattr(metabase_column, field, meta[f"metabase.{field}"])

        return metabase_column

    @staticmethod
    def parse_ref(text: str) -> Optional[str]:
        """Parses dbt ref() or source() statement.

        Arguments:
            text {str} -- Full statement in dbt YAML.

        Returns:
            str -- Name of the reference.
        """

        # matches = re.findall(r"ref\(['\"]([\w\_\-\ ]+)['\"]\)", text)
        # We are catching the rightmost argument of either source or ref which is ultimately the table name
        matches = re.findall(r"['\"]([\w\_\-\ ]+)['\"][ ]*\)$", text.strip())
        if matches:
            logger().debug("%s -> %s", text, matches[0])
            return matches[0]
        return None
