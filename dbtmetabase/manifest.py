from __future__ import annotations

import dataclasses
import json
import logging
from enum import Enum
from pathlib import Path
from typing import (
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Union,
)

from ._format import scan_fields

_logger = logging.getLogger(__name__)

# Namespace for meta fields, e.g. metabase.field
_META_NS = "metabase"
# Allowed namespace fields
_COMMON_META_FIELDS = [
    "display_name",
    "visibility_type",
]
# Must be covered by Column attributes
_COLUMN_META_FIELDS = _COMMON_META_FIELDS + [
    "semantic_type",
    "has_field_values",
    "coercion_strategy",
    "number_style",
]
# Must be covered by Model attributes
_MODEL_META_FIELDS = _COMMON_META_FIELDS + [
    "points_of_interest",
    "caveats",
]

# Default model schema (only schema in BigQuery)
DEFAULT_SCHEMA = "PUBLIC"


class Manifest:
    """dbt manifest reader."""

    def __init__(self, path: Union[str, Path]):
        """Reader for compiled dbt manifest.json file.

        Args:
            path (Union[str, Path]): Path to dbt manifest.json (usually under target/).
        """

        self.path = Path(path).expanduser()

    def read_models(self) -> Iterable[Model]:
        """Reads dbt models in Metabase-friendly format.

        Returns:
            Iterable[Model]: List of dbt models in Metabase-friendly format.
        """

        with open(self.path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        models: MutableSequence[Model] = []

        for node in manifest["nodes"].values():
            if node["resource_type"] != "model":
                continue

            name = node["name"].upper()
            if node["config"]["materialized"] == "ephemeral":
                _logger.debug("Skipping ephemeral model %s", name)
                continue

            models.append(self._read_model(manifest, node, Group.nodes))

        for node in manifest["sources"].values():
            if node["resource_type"] != "source":
                continue

            models.append(
                self._read_model(manifest, node, Group.sources, node["source_name"])
            )

        return models

    def _read_model(
        self,
        manifest: Mapping,
        model: Mapping,
        group: Group,
        source: Optional[str] = None,
    ) -> Model:
        database = model["database"].upper()
        schema = model["schema"].upper()
        unique_id = model["unique_id"]

        relationships = self._read_relationships(manifest, group, unique_id)

        metabase_columns = [
            self._read_column(column, schema, relationships.get(column["name"]))
            for column in model.get("columns", {}).values()
        ]

        return Model(
            database=database,
            schema=schema,
            group=group,
            name=model.get("alias", model.get("identifier", model["name"])),
            description=model.get("description"),
            columns=metabase_columns,
            unique_id=unique_id,
            source=source,
            tags=model.get("tags", []),
            **scan_fields(
                model.get("meta", {}),
                fields=_MODEL_META_FIELDS,
                ns=_META_NS,
            ),
        )

    def _read_column(
        self,
        column: Mapping,
        schema: str,
        relationship: Optional[Mapping],
    ) -> Column:
        metabase_column = Column(
            name=column.get("name", "").upper().strip('"'),
            description=column.get("description"),
            **scan_fields(
                column.get("meta", {}),
                fields=_COLUMN_META_FIELDS,
                ns=_META_NS,
            ),
        )

        self._set_column_fk(
            column=column,
            metabase_column=metabase_column,
            table=relationship["fk_target_table"] if relationship else None,
            field=relationship["fk_target_field"] if relationship else None,
            schema=schema,
        )

        return metabase_column

    def _read_relationships(
        self,
        manifest: Mapping,
        group: Group,
        unique_id: str,
    ) -> Mapping[str, Mapping[str, str]]:
        relationships = {}

        for child_id in manifest["child_map"][unique_id]:
            child = {}
            if manifest[group]:
                child = manifest[group].get(child_id, {})

            if (
                child.get("resource_type") == "test"
                and child.get("test_metadata", {}).get("name") == "relationships"
            ):
                # To get the name of the foreign table, we could use child[test_metadata][kwargs][to], which
                # would return the ref() written in the test, but if the model has an alias, that's not enough.
                # Using child[depends_on][nodes] and excluding the current model is better.

                # Nodes contain at most two tables: referenced model and current model (optional).
                depends_on_nodes = list(child["depends_on"][group])
                if len(depends_on_nodes) > 2:
                    _logger.warning(
                        "Expected at most two nodes, got %d {} nodes, skipping %s {}",
                        len(depends_on_nodes),
                        unique_id,
                    )
                    continue

                # Skip the incoming relationship tests, in which the fk_target_table is the model currently being read.
                # Otherwise, the primary key of the current model would be (incorrectly) determined to be FK.
                is_incoming_relationship_test = depends_on_nodes[1] != unique_id
                if len(depends_on_nodes) == 2 and is_incoming_relationship_test:
                    _logger.debug(
                        "Skip this incoming relationship test, concerning nodes %s",
                        depends_on_nodes,
                    )
                    continue

                # Remove the current model from the list, ensuring it works for self-referencing models.
                if len(depends_on_nodes) == 2 and unique_id in depends_on_nodes:
                    depends_on_nodes.remove(unique_id)

                if len(depends_on_nodes) != 1:
                    _logger.warning(
                        "Expected single node after filtering, got %d instead, skipping %s",
                        len(depends_on_nodes),
                        unique_id,
                    )
                    continue

                depends_on_id = depends_on_nodes[0]

                fk_model = manifest[group].get(depends_on_id, {})
                fk_target_table_alias = fk_model.get(
                    "alias", fk_model.get("identifier", fk_model.get("name"))
                )

                if not fk_target_table_alias:
                    _logger.debug(
                        "Cannot resolve depends on model %s to a model in manifest",
                        depends_on_id,
                    )
                    continue

                fk_target_schema = manifest[group][depends_on_id].get(
                    "schema", DEFAULT_SCHEMA
                )
                fk_target_field = child["test_metadata"]["kwargs"]["field"].strip('"')

                relationships[child["column_name"]] = {
                    "fk_target_table": f"{fk_target_schema}.{fk_target_table_alias}",
                    "fk_target_field": fk_target_field,
                }

        return relationships

    def _set_column_fk(
        self,
        column: Mapping,
        metabase_column: Column,
        table: Optional[str],
        field: Optional[str],
        schema: Optional[str],
    ):
        """Sets foreign key target on a column.

        Args:
            column (Mapping): Schema column definition.
            metabase_column (Column): Metabase column definition.
            table (str): Foreign key target table.
            field (str): Foreign key target field.
            schema (str): Current schema name.
        """
        # Meta fields take precedence
        meta = column.get("meta", {})
        table = meta.get(f"{_META_NS}.fk_target_table", table)
        field = meta.get(f"{_META_NS}.fk_target_field", field)

        if not table or not field:
            if table or field:
                _logger.warning(
                    "FK requires table and field for column %s",
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
        _logger.debug(
            "Relation from %s to %s.%s",
            metabase_column.name,
            metabase_column.fk_target_table,
            metabase_column.fk_target_field,
        )


class Group(str, Enum):
    nodes = "nodes"
    sources = "sources"


@dataclasses.dataclass
class Column:
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
class Model:
    database: str
    schema: str
    group: Group

    name: str
    description: Optional[str] = None
    display_name: Optional[str] = None
    visibility_type: Optional[str] = None
    points_of_interest: Optional[str] = None
    caveats: Optional[str] = None

    unique_id: Optional[str] = None
    source: Optional[str] = None
    tags: Optional[Sequence[str]] = None

    columns: Sequence[Column] = dataclasses.field(default_factory=list)

    @property
    def ref(self) -> Optional[str]:
        if self.group == Group.nodes:
            return f"ref('{self.name}')"
        elif self.group == Group.sources:
            return f"source('{self.source}', '{self.name}')"
        return None

    def format_description(
        self,
        append_tags: bool = False,
        docs_url: Optional[str] = None,
    ) -> str:
        """Formats description from available information.

        Args:
            append_tags (bool, optional): True to include dbt model tags. Defaults to False.
            docs_url (Optional[str], optional): Provide docs base URL to include links. Defaults to None.

        Returns:
            str: Formatted description.
        """

        sections = []

        if self.description:
            sections.append(self.description)

        if append_tags and self.tags:
            sections.append(f"Tags: {', '.join(self.tags)}")

        if docs_url:
            sections.append(
                f"dbt docs: {docs_url.rstrip('/')}/#!/model/{self.unique_id}"
            )

        return "\n\n".join(sections)
