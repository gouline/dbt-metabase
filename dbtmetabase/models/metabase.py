from dataclasses import dataclass, field
from enum import Enum
from typing import MutableMapping, Optional, Sequence, List

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


class ModelType(str, Enum):
    nodes = "nodes"
    sources = "sources"


@dataclass
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

    meta_fields: MutableMapping = field(default_factory=dict)


@dataclass
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

    columns: Sequence[MetabaseColumn] = field(default_factory=list)


@dataclass
class MetabaseCard:
    visualization_settings: MutableMapping = field(default_factory=dict)
    parameters: Optional[List[MutableMapping]] = None
    description: Optional[str] = None
    collection_position: Optional[int] = None
    result_metadata: Optional[List[MutableMapping]] = None
    collection_id: Optional[int] = None
    cache_ttl: Optional[int] = None
    parameter_mappings: Optional[List[MutableMapping]] = None
    display: Optional[str] = None

    exposure_name: Optional[str] = None
    unique_id: Optional[str] = None
    depends_on: Optional[MutableMapping] = None
    compiled_code: Optional[str] = None

    @property
    def id(self):
        return int(self.exposure_name.split("-")[0].replace("MB", "").strip())

    @property
    def name(self):
        return self.exposure_name.split("-")[1].strip()

    @property
    def analysis_id(self):
        nodes = self.depends_on.get("nodes", [])
        assert (
            len(nodes) == 1
        ), "Metabase card should only depend on one analysis model."
        return nodes[0]

    def json(self, database_id: int):
        return {
            "name": self.name,
            "description": self.description,
            "display": "table",
            "visualization_settings": {},
            "dataset_query": {
                "type": "native",
                "native": {
                    "query": self.compiled_code,
                    "template_tags": {},
                },
                "database": database_id,
            },
        }


class _NullValue(str):
    """Explicitly null field value."""

    def __eq__(self, other: object) -> bool:
        return other is None


NullValue = _NullValue()
