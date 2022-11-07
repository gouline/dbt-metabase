from dataclasses import dataclass, field
from enum import Enum
from typing import MutableMapping, Optional, Sequence

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


class _NullValue(str):
    """Explicitly null field value."""

    def __eq__(self, other: object) -> bool:
        return other is None


NullValue = _NullValue()
