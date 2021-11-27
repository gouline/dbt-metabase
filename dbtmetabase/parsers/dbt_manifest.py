import json
import os
from typing import List, Tuple, Mapping, Optional, MutableMapping

from ..models.config import DbtConfig
from ..models.metabase import METABASE_META_FIELDS, ModelType
from ..models.metabase import MetabaseModel, MetabaseColumn
from ..logger.logging import logger


class DbtManifestReader:
    """
    Reader for dbt manifest artifact.
    """

    def __init__(self, project_path: str):
        """Constructor.

        Arguments:
            manifest_path {str} -- Path to dbt manifest.json.
        """

        self.manifest_path = os.path.expanduser(project_path)
        self.manifest: Mapping = {}
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

        path = self.manifest_path
        mb_models: List[MetabaseModel] = []

        with open(path, "r", encoding="utf-8") as manifest_file:
            self.manifest = json.load(manifest_file)

        for _, node in self.manifest["nodes"].items():
            model_name = node["name"].upper()

            if node["config"]["materialized"] == "ephemeral":
                logger().debug(
                    "Skipping ephemeral model %s not manifested in database", model_name
                )
                continue

            if node["database"].upper() != database.upper():
                # Skip model not associated with target database
                logger().debug(
                    "Skipping %s not in target database %s", model_name, database
                )
                continue

            if node["resource_type"] != "model":
                # Target only model nodes
                logger().debug("Skipping %s not of resource type model", model_name)
                continue

            if schema and node["schema"].upper() != schema.upper():
                # Skip any models not in target schema
                logger().debug(
                    "Skipping %s in schema %s not in target schema %s",
                    model_name,
                    node["schema"],
                    schema,
                )
                continue

            if schema_excludes and node["schema"].upper() in schema_excludes:
                # Skip any model in a schema marked for exclusion
                logger().debug(
                    "Skipping %s in schema %s marked for exclusion",
                    model_name,
                    node["schema"],
                )
                continue

            if (includes and model_name not in includes) or (model_name in excludes):
                # Process only intersect of includes and excludes
                logger().debug(
                    "Skipping %s not included in includes or excluded by excludes",
                    model_name,
                )
                continue

            mb_models.append(
                self._read_model(
                    node,
                    include_tags=include_tags,
                    docs_url=docs_url,
                    model_type=ModelType.nodes,
                    source=None,
                )
            )

        for _, node in self.manifest["sources"].items():
            model_name = node.get("identifier", node.get("name")).upper()

            if node["database"].upper() != database.upper():
                # Skip model not associated with target database
                logger().debug(
                    "Skipping %s not in target database %s", model_name, database
                )
                continue

            if node["resource_type"] != "source":
                # Target only source nodes
                logger().debug("Skipping %s not of resource type source", model_name)
                continue

            if schema and node["schema"].upper() != schema.upper():
                # Skip any models not in target schema
                logger().debug(
                    "Skipping %s in schema %s not in target schema %s",
                    model_name,
                    node["schema"],
                    schema,
                )
                continue

            if schema_excludes and node["schema"].upper() in schema_excludes:
                # Skip any model in a schema marked for exclusion
                logger().debug(
                    "Skipping %s in schema %s marked for exclusion",
                    model_name,
                    node["schema"],
                )
                continue

            if (includes and model_name not in includes) or (model_name in excludes):
                # Process only intersect of includes and excludes
                logger().debug(
                    "Skipping %s not included in includes or excluded by excludes",
                    model_name,
                )
                continue

            mb_models.append(
                self._read_model(
                    node,
                    include_tags=include_tags,
                    docs_url=docs_url,
                    model_type=ModelType.sources,
                    source=node["source_name"],
                )
            )

        return mb_models, self.alias_mapping

    def _read_model(
        self,
        model: dict,
        source: Optional[str] = None,
        model_type: ModelType = ModelType.nodes,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> MetabaseModel:
        """Reads one dbt model in Metabase-friendly format.

        Arguments:
            model {dict} -- One dbt model to read.
            source {str, optional} -- Name of the source if source
            model_type {str} -- The type of the node which can be one of either nodes or sources
            include_tags: {bool} -- Flag to append tags to description of model

        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        metabase_column: List[MetabaseColumn] = []

        children = self.manifest["child_map"][model["unique_id"]]
        relationship_tests = {}

        for child_id in children:
            child = {}
            if self.manifest[model_type]:
                child = self.manifest[model_type].get(child_id, {})
            # Only proceed if we are seeing an explicitly declared relationship test
            if (
                child.get("resource_type") == "test"
                and child.get("test_metadata", {}).get("name") == "relationships"
            ):
                # To get the name of the foreign table, we could use child['test_metadata']['kwargs']['to'], which
                # would return the ref() written in the test, but if the model has an alias, that's not enough.
                # It is better to use child['depends_on']['nodes'] and exclude the current model

                depends_on_id = list(
                    set(child["depends_on"][model_type]) - {model["unique_id"]}
                )[0]

                foreign_key_model = self.manifest[model_type].get(depends_on_id, {})
                fk_target_table_alias = foreign_key_model.get(
                    "alias",
                    foreign_key_model.get("identifier", foreign_key_model.get("name")),
                )

                if not fk_target_table_alias:
                    logger().debug(
                        "Could not resolve depends on model id %s to a model in manifest",
                        depends_on_id,
                    )
                    continue

                fk_target_schema = self.manifest[model_type][depends_on_id].get(
                    "schema", "public"
                )
                fk_target_field = child["test_metadata"]["kwargs"]["field"].strip('"')

                relationship_tests[child["column_name"]] = {
                    "fk_target_table": f"{fk_target_schema}.{fk_target_table_alias}",
                    "fk_target_field": fk_target_field,
                }

        for _, column in model.get("columns", {}).items():
            metabase_column.append(
                self._read_column(
                    column=column,
                    relationship=relationship_tests.get(column["name"]),
                )
            )

        description = model.get("description", "")
        meta = model.get("meta", {})
        points_of_interest = meta.get("metabase.points_of_interest")
        caveats = meta.get("metabase.caveats")

        if include_tags:
            tags = model.get("tags", [])
            if tags:
                tags = ", ".join(tags)
                if description != "":
                    description += "\n\n"
                description += f"Tags: {tags}"

        unique_id = model["unique_id"]
        if docs_url:
            full_path = f"{docs_url}/#!/model/{unique_id}"
            if description != "":
                description += "\n\n"
            description += f"dbt docs link: {full_path}"

        resolved_name = model.get("alias", model.get("identifier"))
        dbt_name = None
        if not resolved_name:
            resolved_name = model["name"]
        else:
            dbt_name = model["name"]

        return MetabaseModel(
            name=resolved_name,
            schema=model["schema"].upper(),
            description=description,
            points_of_interest=points_of_interest,
            caveats=caveats,
            columns=metabase_column,
            model_type=model_type,
            unique_id=unique_id,
            source=source,
            dbt_name=dbt_name,
        )

    def _read_column(
        self,
        column: Mapping,
        relationship: Optional[Mapping],
    ) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.
            relationship {Mapping, optional} -- Mapping of columns to their foreign key relationships

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        column_name = column.get("name", "").upper().strip('"')
        column_description = column.get("description")
        metabase_column = MetabaseColumn(
            name=column_name,
            description=column_description,
        )

        if relationship:
            metabase_column.semantic_type = "type/FK"
            metabase_column.fk_target_table = relationship["fk_target_table"].upper()
            metabase_column.fk_target_field = relationship["fk_target_field"].upper()
            logger().debug(
                "Relation from %s to %s.%s",
                column.get("name", "").upper().strip('"'),
                metabase_column.fk_target_table,
                metabase_column.fk_target_field,
            )

        if column["meta"]:
            meta = column.get("meta", [])
            for field in METABASE_META_FIELDS:
                if f"metabase.{field}" in meta:
                    setattr(metabase_column, field, meta[f"metabase.{field}"])

        return metabase_column
