from abc import ABCMeta, abstractmethod
from os.path import expanduser
from typing import Optional, MutableMapping, Iterable, Tuple, List

from ..models.metabase import MetabaseModel


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
        self.database = database
        self.schema = schema
        self.schema_excludes = schema_excludes
        self.includes = includes
        self.excludes = excludes
        self.alias_mapping: MutableMapping = {}

    @abstractmethod
    def read_models(
        self,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        pass
