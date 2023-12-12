import dataclasses
import json
import re
from enum import Enum
from pathlib import Path
from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import yaml

from .logger.logging import logger

# Allowed metabase.* fields
_METABASE_COMMON_META_FIELDS = [
    "display_name",
    "visibility_type",
]
# Must be covered by MetabaseColumn attributes
METABASE_COLUMN_META_FIELDS = _METABASE_COMMON_META_FIELDS + [
    "semantic_type",
    "has_field_values",
    "coercion_strategy",
    "number_style",
]
# Must be covered by MetabaseModel attributes
METABASE_MODEL_META_FIELDS = _METABASE_COMMON_META_FIELDS + [
    "points_of_interest",
    "caveats",
]

# Default model schema (only schema in BigQuery)
METABASE_MODEL_DEFAULT_SCHEMA = "PUBLIC"


class DbtArgumentError(ValueError):
    """Invalid dbt reader argument supplied."""


class ModelType(str, Enum):
    nodes = "nodes"
    sources = "sources"


@dataclasses.dataclass
class MetabaseColumn:
    name: str
    description: Optional[str] = None

    display_name: Optional[str] = None
    visibility_type: Optional[str] = None
    semantic_type: Optional[str] = None
    has_field_values: Optional[str] = None
    coercion_strategy: Optional[str] = None
    number_style: Optional[str] = None

    fk_target_table: Optional[str] = None
    fk_target_field: Optional[str] = None

    meta_fields: MutableMapping = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class MetabaseModel:
    name: str
    schema: str
    description: str = ""

    display_name: Optional[str] = None
    visibility_type: Optional[str] = None
    points_of_interest: Optional[str] = None
    caveats: Optional[str] = None

    model_type: ModelType = ModelType.nodes
    dbt_name: Optional[str] = None
    source: Optional[str] = None
    unique_id: Optional[str] = None

    @property
    def ref(self) -> Optional[str]:
        if self.model_type == ModelType.nodes:
            return f"ref('{self.name}')"
        elif self.model_type == ModelType.sources:
            return f"source('{self.source}', '{self.name if self.dbt_name is None else self.dbt_name}')"
        return None

    columns: Sequence[MetabaseColumn] = dataclasses.field(default_factory=list)


class _NullValue(str):
    """Explicitly null field value."""

    def __eq__(self, other: object) -> bool:
        return other is None


NullValue = _NullValue()


class DbtReader:
    """Reader for dbt projects."""

    def __init__(
        self,
        manifest_path: Optional[str] = None,
        project_path: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        schema_excludes: Optional[Iterable] = None,
        includes: Optional[Iterable] = None,
        excludes: Optional[Iterable] = None,
    ):
        """Reader for dbt projects.

        Manifest mode:
        - Reads compiled manifest.json file (located in target/).
        - Requires database and schema (schema_excludes optional).
        - Recommended option, because it supports all dynamic dbt features.
        - Requires you to run `dbt compile` before reading.

        Project reader:
        - Reads uncompiled schema.yml files in your project.
        - Not recommended, only when running `dbt compile` is not an option.
        - Does not support schema selection or any other dynamic dbt features.

        Args:
            manifest_path (str, optional): Path to dbt manifest.json. Not supported with project_path. Defaults to None.
            project_path (str, optional): Path to dbt project. Not supported with manifest_path. Defaults to None.
            database (str, optional): Target database name specified in dbt models. Not supported with project_path. Default to None.
            schema (str, optional): Target schema. Not supported with project_path. Defaults to None.
            schema_excludes (Iterable, optional): Target schemas to exclude. Not supported with project_path Defaults to None.
            includes (Iterable, optional): Model names to limit selection. Defaults to None.
            excludes (Iterable, optional): Model names to exclude from selection. Defaults to None.
        """

        self.manifest_path: Optional[Path] = None
        self.project_path: Optional[Path] = None
        self.database: Optional[str] = None
        self.schema: Optional[str] = None

        if manifest_path and not project_path:
            self.manifest_path = Path(manifest_path).expanduser()

            if database and schema:
                self.database = database.upper()
                self.schema = schema.upper()
                self.schema_excludes = [x.upper() for x in schema_excludes or []]
            else:
                raise DbtArgumentError(
                    "Arguments database and schema is required with manifest_path"
                )
        elif project_path and not manifest_path:
            self.project_path = Path(project_path).expanduser()

            if not (database or schema or schema_excludes):
                self.database = None
                self.schema = None
                self.schema_excludes = []
            else:
                raise DbtArgumentError(
                    "Arguments database, schema and schema_exclude not allowed with project_path"
                )
        else:
            raise DbtArgumentError("Argument manifest_path OR project_path is required")

        self.includes = [x.upper() for x in includes or []]
        self.excludes = [x.upper() for x in excludes or []]
        self.alias_mapping: MutableMapping[str, str] = {}

    def read_models(
        self,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        if self.manifest_path:
            return self._read_manifest_models(include_tags, docs_url)
        else:
            return self._read_project_models(include_tags)

    def _read_manifest_models(
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

        assert self.manifest_path
        with open(self.manifest_path, "r", encoding="utf-8") as manifest_file:
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
                self._read_manifest_model(
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
                self._read_manifest_model(
                    manifest,
                    node,
                    include_tags=include_tags,
                    docs_url=docs_url,
                    model_type=ModelType.sources,
                    source=node["source_name"],
                )
            )

        return mb_models, self.alias_mapping

    def _read_manifest_model(
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

                # Skip the incoming relationship tests, in which the fk_target_table is the model currently being read.
                # Otherwise, the primary key of the current model would be (incorrectly) determined to be a foreign key.
                is_incoming_relationship_test = depends_on_nodes[1] != unique_id
                if len(depends_on_nodes) == 2 and is_incoming_relationship_test:
                    logger().debug(
                        "Skip this incoming relationship test, concerning nodes %s.",
                        depends_on_nodes,
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
                self._read_manifest_column(
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

    def _read_manifest_column(
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

    def _read_project_models(
        self,
        include_tags: bool = True,
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

        assert self.project_path
        for path in (self.project_path / "models").rglob("*.yml"):
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
                        self._read_project_model(
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
                            self._read_project_model(
                                model=model,
                                source=source["name"],
                                model_type=ModelType.sources,
                                schema=source_schema_name,
                                include_tags=include_tags,
                            )
                        )

        return mb_models, self.alias_mapping

    def _read_project_model(
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
            metabase_columns.append(self._read_project_column(column, schema))

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

    def _read_project_column(self, column: Mapping, schema: str) -> MetabaseColumn:
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

    def model_selected(self, name: str) -> bool:
        """Checks whether model passes inclusion/exclusion criteria.

        Args:
            name (str): Model name.

        Returns:
            bool: True if included, false otherwise.
        """
        n = name.upper()
        return n not in self.excludes and (not self.includes or n in self.includes)

    def set_column_foreign_key(
        self,
        column: Mapping,
        metabase_column: MetabaseColumn,
        table: Optional[str],
        field: Optional[str],
        schema: Optional[str],
    ):
        """Sets foreign key target on a column.

        Args:
            column (Mapping): Schema column definition.
            metabase_column (MetabaseColumn): Metabase column definition.
            table (str): Foreign key target table.
            field (str): Foreign key target field.
            schema (str): Current schema name.
        """
        # Meta fields take precedence
        meta = column.get("meta", {})
        table = meta.get("metabase.fk_target_table", table)
        field = meta.get("metabase.fk_target_field", field)

        if not table or not field:
            if table or field:
                logger().warning(
                    "Foreign key requires table and field for column %s",
                    metabase_column.name,
                )
            return

        table_path = table.split(".")
        if len(table_path) == 1 and schema:
            table_path.insert(0, schema)

        metabase_column.semantic_type = "type/FK"
        metabase_column.fk_target_table = ".".join(
            [x.strip('"').upper() for x in table_path]
        )
        metabase_column.fk_target_field = field.strip('"').upper()
        logger().debug(
            "Relation from %s to %s.%s",
            metabase_column.name,
            metabase_column.fk_target_table,
            metabase_column.fk_target_field,
        )

    @staticmethod
    def read_meta_fields(obj: Mapping, fields: List) -> Mapping:
        """Reads meta fields from a schem object.

        Args:
            obj (Mapping): Schema object.
            fields (List): List of fields to read.

        Returns:
            Mapping: Field values.
        """

        vals = {}
        meta = obj.get("meta", [])
        for field in fields:
            if f"metabase.{field}" in meta:
                value = meta[f"metabase.{field}"]
                vals[field] = value if value is not None else NullValue
        return vals

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
