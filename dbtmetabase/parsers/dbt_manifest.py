import json
import os
from typing import List, Iterable, Mapping, Optional, MutableMapping
import logging

from dbtmetabase.models.metabase import METABASE_META_FIELDS
from dbtmetabase.models.metabase import MetabaseModel, MetabaseColumn


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
        self.catch_aliases: MutableMapping = {}

    def read_models(
        self,
        database: str,
        schema: str,
        schema_excludes: Iterable = None,
        includes: Iterable = None,
        excludes: Iterable = None,
        include_tags: bool = True,
        docs_url: str = None,
    ) -> List[MetabaseModel]:

        if schema_excludes is None:
            schema_excludes = []
        if includes is None:
            includes = []
        if excludes is None:
            excludes = []

        path = self.manifest_path
        mb_models: List[MetabaseModel] = []

        with open(path, "r") as manifest_file:
            self.manifest = json.load(manifest_file)

        for _, node in self.manifest["nodes"].items():
            model_name = node["name"].upper()

            if node["config"]["materialized"] == "ephemeral":
                logging.info(
                    "Skipping ephemeral model %s not manifested in database", model_name
                )
                continue

            if node["database"].upper() != database.upper():
                # Skip model not associated with target database
                logging.debug(
                    "Skipping %s not in target database %s", model_name, database
                )
                continue

            if node["resource_type"] != "model":
                # Target only model nodes
                logging.debug("Skipping %s not of resource type model", model_name)
                continue

            if schema and node["schema"].upper() != schema.upper():
                # Skip any models not in target schema
                logging.debug(
                    "Skipping %s in schema %s not in target schema %s",
                    model_name,
                    node["schema"],
                    schema,
                )
                continue

            if schema_excludes and node["schema"].upper() in schema_excludes:
                # Skip any model in a schema marked for exclusion
                logging.debug(
                    "Skipping %s in schema %s marked for exclusion",
                    model_name,
                    node["schema"],
                )
                continue

            if (includes and model_name not in includes) or (model_name in excludes):
                # Process only intersect of includes and excludes
                logging.debug(
                    "Skipping %s not included in includes or excluded by excludes",
                    model_name,
                )
                continue

            mb_models.append(
                self._read_model(node, include_tags=include_tags, docs_url=docs_url)
            )

        for _, node in self.manifest["sources"].items():
            model_name = node.get("identifier", node.get("name")).upper()

            if node["database"].upper() != database.upper():
                # Skip model not associated with target database
                logging.debug(
                    "Skipping %s not in target database %s", model_name, database
                )
                continue

            if node["resource_type"] != "source":
                # Target only source nodes
                logging.debug("Skipping %s not of resource type source", model_name)
                continue

            if schema and node["schema"].upper() != schema.upper():
                # Skip any models not in target schema
                logging.debug(
                    "Skipping %s in schema %s not in target schema %s",
                    model_name,
                    node["schema"],
                    schema,
                )
                continue

            if schema_excludes and node["schema"].upper() in schema_excludes:
                # Skip any model in a schema marked for exclusion
                logging.debug(
                    "Skipping %s in schema %s marked for exclusion",
                    model_name,
                    node["schema"],
                )
                continue

            if (includes and model_name not in includes) or (model_name in excludes):
                # Process only intersect of includes and excludes
                logging.debug(
                    "Skipping %s not included in includes or excluded by excludes",
                    model_name,
                )
                continue

            mb_models.append(
                self._read_model(
                    node,
                    include_tags=include_tags,
                    docs_url=docs_url,
                    manifest_key="sources",
                )
            )

        return mb_models

    def _read_model(
        self,
        model: dict,
        include_tags: bool = True,
        docs_url: str = None,
        manifest_key: str = "nodes",
    ) -> MetabaseModel:
        """Reads one dbt model in Metabase-friendly format.

        Arguments:
            model {dict} -- One dbt model to read.

        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        mb_columns: List[MetabaseColumn] = []

        children = self.manifest["child_map"][model["unique_id"]]
        relationship_tests = {}

        for child_id in children:
            child = {}
            if self.manifest[manifest_key]:
                child = self.manifest[manifest_key].get(child_id, {})
            if (
                child.get("resource_type") == "test"
                and child.get("test_metadata", {}).get("name") == "relationships"
            ):
                # Only proceed if we are seeing an explicitly declared relationship test

                # To get the name of the foreign table, we could use child['test_metadata']['kwargs']['to'], which
                # would return the ref() written in the test, but if the model as an alias, that's not enough.
                # It is better to use child['depends_on']['nodes'] and exclude the current model

                depends_on_id = list(
                    set(child["depends_on"][manifest_key]) - {model["unique_id"]}
                )[0]

                fk_target_table_alias = self.manifest[manifest_key][depends_on_id][
                    "alias"
                ]
                fk_target_schema = self.manifest[manifest_key][depends_on_id].get(
                    "schema", "public"
                )
                fk_target_field = child["test_metadata"]["kwargs"]["field"].strip('"')

                relationship_tests[child["column_name"]] = {
                    "fk_target_table": f"{fk_target_schema}.{fk_target_table_alias}",
                    "fk_target_field": fk_target_field,
                }

        for _, column in model.get("columns", {}).items():
            mb_columns.append(
                self._read_column(column, relationship_tests.get(column["name"]))
            )

        description = model.get("description", "")

        if include_tags:
            tags = model.get("tags")
            if tags:
                tags = ", ".join(tags)
                if description != "":
                    description += "\n\n"
                description += f"Tags: {tags}"

        if docs_url:
            full_path = f"{docs_url}/#!/model/{model['unique_id']}"
            if description != "":
                description += "\n\n"
            description += f"dbt docs link: {full_path}"

        return MetabaseModel(
            name=model.get("alias", model.get("identifier", model.get("name"))).upper(),
            schema=model["schema"].upper(),
            description=description,
            columns=mb_columns,
        )

    def _read_column(
        self, column: Mapping, relationship: Optional[Mapping]
    ) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        description = column.get("description", "")

        mb_column = MetabaseColumn(
            name=column.get("name", "").upper().strip('"'), description=description
        )

        if relationship:
            mb_column.semantic_type = "type/FK"
            mb_column.fk_target_table = relationship["fk_target_table"].upper()
            mb_column.fk_target_field = relationship["fk_target_field"].upper()
            logging.debug(
                "Relation from %s to %s.%s",
                column.get("name", "").upper().strip('"'),
                mb_column.fk_target_table,
                mb_column.fk_target_field,
            )

        if column["meta"]:
            meta = column.get("meta", [])
            for field in METABASE_META_FIELDS:
                if f"metabase.{field}" in meta:
                    setattr(mb_column, field, meta[f"metabase.{field}"])

        return mb_column
