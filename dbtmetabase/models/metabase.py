from dataclasses import dataclass, field
from enum import Enum

from typing import Sequence, Optional, MutableMapping

# Allowed metabase.* fields
# Should be covered by attributes in the MetabaseColumn class
METABASE_META_FIELDS = ["special_type", "semantic_type", "visibility_type"]


class ModelType(str, Enum):
    nodes = "nodes"
    sources = "sources"


@dataclass
class MetabaseColumn:
    name: str
    description: Optional[str] = None

    meta_fields: MutableMapping = field(default_factory=dict)

    semantic_type: Optional[str] = None
    visibility_type: Optional[str] = None

    fk_target_table: Optional[str] = None
    fk_target_field: Optional[str] = None


@dataclass
class MetabaseModel:
    name: str
    schema: str
    description: str = ""
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
