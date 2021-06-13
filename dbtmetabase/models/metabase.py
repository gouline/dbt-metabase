from dataclasses import dataclass, field
from typing import Sequence, Optional, MutableMapping

# Allowed metabase.* fields
# Should be covered by attributes in the MetabaseColumn class
METABASE_META_FIELDS = ["special_type", "semantic_type", "visibility_type"]


class NonEmptyString:
    def __init__(self):
        self.name = None

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, objtype=None):
        return obj.__dict__[self.name]

    def __set__(self, obj, value):
        if value == "":
            value = None

        obj.__dict__[self.name] = value


@dataclass
class MetabaseColumn:
    name: str
    description: NonEmptyString = NonEmptyString()

    meta_fields: MutableMapping = field(default_factory=dict)

    semantic_type: Optional[str] = None
    visibility_type: Optional[str] = None

    fk_target_table: Optional[str] = None
    fk_target_field: Optional[str] = None


@dataclass
class MetabaseModel:
    name: str
    schema: str
    description: NonEmptyString = NonEmptyString()

    columns: Sequence[MetabaseColumn] = field(default_factory=list)
