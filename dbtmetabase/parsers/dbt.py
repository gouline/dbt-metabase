from abc import ABCMeta, abstractmethod
from os.path import expanduser
from typing import Optional, Mapping, MutableMapping, Iterable, Tuple, List

from ..models.metabase import METABASE_META_FIELDS, MetabaseModel, NullValue


class DbtReader(metaclass=ABCMeta):
    """Base dbt reader."""

    def __init__(
        self,
        path: str,
        database: str,
        schema: Optional[str],
        schema_excludes: Optional[Iterable],
        includes: Optional[Iterable],
        excludes: Optional[Iterable],
    ):
        """Constructor.

        Args:
            path (str): Path to dbt target.
            database (str): Target database name as specified in dbt models to be actioned.
            path (Optional[str]): Path to dbt project. If specified with manifest_path, then the manifest is prioritized.
            schema (Optional[str]): Target schema. Should be passed if using folder parser.
            schema_excludes (Optional[Iterable]): Target schemas to exclude. Ignored in folder parser.
            includes (Optional[Iterable]): Model names to limit processing to.
            excludes (Optional[Iterable]): Model names to exclude.
        """

        self.path = expanduser(path)
        self.database = database.upper() if schema else None
        self.schema = schema.upper() if schema else "PUBLIC"
        self.schema_excludes = [x.upper() for x in schema_excludes or []]
        self.includes = [x.upper() for x in includes or []]
        self.excludes = [x.upper() for x in excludes or []]
        self.alias_mapping: MutableMapping = {}

    @abstractmethod
    def read_models(
        self,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        pass

    def model_selected(self, name: str) -> bool:
        """Checks whether model passes inclusion/exclusion criteria.

        Args:
            name (str): Model name.

        Returns:
            bool: True if included, false otherwise.
        """
        n = name.upper()
        return n not in self.excludes and (not self.includes or n in self.includes)

    @staticmethod
    def read_meta_fields(obj: Mapping) -> Mapping:
        """Reads meta fields from a schem object.

        Args:
            obj (Mapping): Schema object.

        Returns:
            Mapping: Field values.
        """

        vals = {}
        meta = obj.get("meta", [])
        for field in METABASE_META_FIELDS:
            if f"metabase.{field}" in meta:
                value = meta[f"metabase.{field}"]
                vals[field] = value if value is not None else NullValue
        return vals
