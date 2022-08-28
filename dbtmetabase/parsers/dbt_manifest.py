import json
from typing import List, Mapping, MutableMapping, Optional, Tuple

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


class DbtManifestReader(DbtReader):
    """Reader for dbt manifest artifact."""

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

        manifest = {}

        mb_models: List[MetabaseModel] = []

        with open(self.path, "r", encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)

        for _, node in manifest["nodes"].items():
            model_name = node["name"].upper()
            model_schema = node["schema"].upper()
            model_database = node["database"].upper()

            if node["resource_type"] != "model":
                logger().debug("Skipping %s not of resource type model", model_name)
                continue

            if node["config"]["materialized"] == "ephemeral":
                logger().debug(
                    "Skipping ephemeral model %s not manifested in database", model_name
                )
                continue

            if model_database != self.database:
                logger().debug(
                    "Skipping %s in database %s, not in target database %s",
                    model_name,
                    model_database,
                    self.database,
                )
                continue

            if self.schema and model_schema != self.schema:
                logger().debug(
                    "Skipping %s in schema %s not in target schema %s",
                    model_name,
                    model_schema,
                    self.schema,
                )
                continue

            if model_schema in self.schema_excludes:
                logger().debug(
                    "Skipping %s in schema %s marked for exclusion",
                    model_name,
                    model_schema,
                )
                continue

            if not self.model_selected(model_name):
                logger().debug(
                    "Skipping %s not included in includes or excluded by excludes",
                    model_name,
                )
                continue

            mb_models.append(
                self._read_model(
                    manifest,
                    node,
                    include_tags=include_tags,
                    docs_url=docs_url,
                    model_type=ModelType.nodes,
                    source=None,
                )
            )

        for _, node in manifest["sources"].items():
            source_name = node.get("identifier", node.get("name")).upper()
            source_schema = node["schema"].upper()
            source_database = node["database"].upper()

            if node["resource_type"] != "source":
                logger().debug("Skipping %s not of resource type source", source_name)
                continue

            if source_database != self.database:
                logger().debug(
                    "Skipping %s not in target database %s", source_name, self.database
                )
                continue

            if self.schema and source_schema != self.schema:
                logger().debug(
                    "Skipping %s in schema %s not in target schema %s",
                    source_name,
                    source_schema,
                    self.schema,
                )
                continue

            if source_schema in self.schema_excludes:
                logger().debug(
                    "Skipping %s in schema %s marked for exclusion",
                    source_name,
                    source_schema,
                )
                continue

            if not self.model_selected(source_name):
                logger().debug(
                    "Skipping %s not included in includes or excluded by excludes",
                    source_name,
                )
                continue

            mb_models.append(
                self._read_model(
                    manifest,
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
        manifest: Mapping,
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

        metabase_columns: List[MetabaseColumn] = []

        schema = model["schema"].upper()
        unique_id = model["unique_id"]

        children = manifest["child_map"][unique_id]
        relationship_tests = {}

        for child_id in children:
            child = {}
            if manifest[model_type]:
                child = manifest[model_type].get(child_id, {})
            # Only proceed if we are seeing an explicitly declared relationship test
            if (
                child.get("resource_type") == "test"
                and child.get("test_metadata", {}).get("name") == "relationships"
            ):
                # To get the name of the foreign table, we could use child['test_metadata']['kwargs']['to'], which
                # would return the ref() written in the test, but if the model has an alias, that's not enough.
                # It is better to use child['depends_on']['nodes'] and exclude the current model

                # From experience, nodes contains at most two tables: the referenced model and the current model.
                # Note, sometimes only the referenced model is returned.
                depends_on_nodes = list(child["depends_on"][model_type])
                if len(depends_on_nodes) > 2:
                    logger().warning(
                        "Expected at most two nodes, got %d {} nodes, skipping %s {}",
                        len(depends_on_nodes),
                        unique_id,
                    )
                    continue

                # Remove the current model from the list. Note, remove() only removes the first occurrence. This ensures
                # the logic also works for self referencing models.
                if len(depends_on_nodes) == 2 and unique_id in depends_on_nodes:
                    depends_on_nodes.remove(unique_id)

                if len(depends_on_nodes) != 1:
                    logger().warning(
                        "Expected single node after filtering, got %d nodes, skipping %s",
                        len(depends_on_nodes),
                        unique_id,
                    )
                    continue

                depends_on_id = depends_on_nodes[0]

                foreign_key_model = manifest[model_type].get(depends_on_id, {})
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

                fk_target_schema = manifest[model_type][depends_on_id].get(
                    "schema", METABASE_MODEL_DEFAULT_SCHEMA
                )
                fk_target_field = child["test_metadata"]["kwargs"]["field"].strip('"')

                relationship_tests[child["column_name"]] = {
                    "fk_target_table": f"{fk_target_schema}.{fk_target_table_alias}",
                    "fk_target_field": fk_target_field,
                }

        for _, column in model.get("columns", {}).items():
            metabase_columns.append(
                self._read_column(
                    column=column,
                    schema=schema,
                    relationship=relationship_tests.get(column["name"]),
                )
            )

        description = model.get("description", "")

        if include_tags:
            tags = model.get("tags", [])
            if tags:
                tags = ", ".join(tags)
                if description != "":
                    description += "\n\n"
                description += f"Tags: {tags}"

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
            schema=schema,
            description=description,
            columns=metabase_columns,
            model_type=model_type,
            unique_id=unique_id,
            source=source,
            dbt_name=dbt_name,
            **self.read_meta_fields(model, METABASE_MODEL_META_FIELDS),
        )

    def _read_column(
        self,
        column: Mapping,
        schema: str,
        relationship: Optional[Mapping],
    ) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.
            schema {str} -- Schema as passed down from CLI args or parsed from `source`
            relationship {Mapping, optional} -- Mapping of columns to their foreign key relationships

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

        self.set_column_foreign_key(
            column=column,
            metabase_column=metabase_column,
            table=relationship["fk_target_table"] if relationship else None,
            field=relationship["fk_target_field"] if relationship else None,
            schema=schema,
        )

        return metabase_column
