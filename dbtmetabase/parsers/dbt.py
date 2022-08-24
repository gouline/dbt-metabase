from abc import ABCMeta, abstractmethod
from os.path import expanduser
from typing import Iterable, List, Mapping, MutableMapping, Optional, Tuple

from ..logger.logging import logger
from ..models.metabase import MetabaseColumn, MetabaseModel, NullValue


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
        self.database = database.upper() if database else None
        self.schema = schema.upper() if schema else None
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
