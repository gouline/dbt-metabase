from dataclasses import dataclass, field
from typing import Sequence, Optional, MutableMapping

# Allowed metabase.* fields
# Should be covered by attributes in the MetabaseColumn class
METABASE_META_FIELDS = ["special_type", "semantic_type", "visibility_type"]


@dataclass
class MetabaseColumn:
    name: str
    description: str = ""

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

    columns: Sequence[MetabaseColumn] = field(default_factory=list)
