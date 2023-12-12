import logging
from os.path import expandvars
from typing import Iterable, List, MutableMapping, Optional, Tuple, Union

from ..exceptions import NoDbtPathSupplied, NoDbtSchemaSupplied
from ..parsers.dbt import DbtReader
from ..parsers.dbt_folder import DbtFolderReader
from ..parsers.dbt_manifest import DbtManifestReader
from .metabase import MetabaseModel


class DbtInterface:
    """Interface for interacting with dbt and preparing a validated parser object."""

    _parser: Optional[Union[DbtManifestReader, DbtFolderReader]] = None

    def __init__(
        self,
        database: str,
        manifest_path: Optional[str] = None,
        path: Optional[str] = None,
        schema: Optional[str] = None,
        schema_excludes: Optional[Iterable] = None,
        includes: Optional[Iterable] = None,
        excludes: Optional[Iterable] = None,
    ):
        """Constructor.

        Args:
            database (str): Target database name as specified in dbt models to be actioned.
            manifest_path (Optional[str], optional): Path to dbt manifest.json file (typically located in the /target/ directory of the dbt project). Defaults to None.
            path (Optional[str], optional): Path to dbt project. If specified with manifest_path, then the manifest is prioritized. Defaults to None.
            schema (Optional[str], optional): Target schema. Should be passed if using folder parser. Defaults to None.
            schema_excludes (Optional[Iterable], optional): Target schemas to exclude. Ignored in folder parser. Defaults to None.
            includes (Optional[Iterable], optional): Model names to limit processing to. Defaults to None.
            excludes (Optional[Iterable], optional): Model names to exclude. Defaults to None.
        """

        if schema_excludes is None:
            schema_excludes = []
        if includes is None:
            includes = []
        if excludes is None:
            excludes = []

        # dbt Reader
        self.database = database
        self.manifest_path = manifest_path
        self.path = path
        # dbt Target Models
        self.schema = schema
        self._schema_excludes = schema_excludes
        self.includes = includes
        self.excludes = excludes

        self.validate_config()

    @property
    def parser(self) -> DbtReader:
        if self._parser is None:
            self.prepare_dbt_parser()
            assert self._parser
        return self._parser

    @property
    def schema_excludes(self) -> Iterable:
        return self._schema_excludes

    @schema_excludes.setter
    def schema_excludes(self, value: Iterable):
        self._schema_excludes = list({schema.upper() for schema in value})

    def validate_config(self):
        """Validates a dbt config object

        Raises:
            NoDbtPathSupplied: If no path for either manifest or project is supplied, this error is raised
            NoDbtSchemaSupplied: If no schema is supplied while using the folder parser, this error is raised
        """
        # Check 1 Verify Path
        if not (self.path or self.manifest_path):
            raise NoDbtPathSupplied(
                "One of either dbt_path or dbt_manifest_path is required."
            )
        # Check 2 Notify User if Both Paths Are Supplied
        if self.path and self.manifest_path:
            logging.warning(
                "Both dbt path and manifest path were supplied. Prioritizing manifest parser"
            )
        # Check 3 Validation for Folder Parser
        if self.path and not self.schema:
            raise NoDbtSchemaSupplied(
                "Must supply a schema if using YAML parser, it is used to resolve foreign key relations and which Metabase models to propagate documentation to"
            )
        # ... Add checks to interface as needed

    def prepare_dbt_parser(self):
        """Resolve dbt reader being either YAML or manifest.json based."""

        if self._parser is not None:
            # Already prepared
            return

        kwargs = {
            "database": self.database,
            "schema": self.schema,
            "schema_excludes": self.schema_excludes,
            "includes": self.includes,
            "excludes": self.excludes,
        }
        self._parser: DbtReader
        if self.manifest_path:
            self._parser = DbtManifestReader(expandvars(self.manifest_path), **kwargs)
        elif self.path:
            self._parser = DbtFolderReader(expandvars(self.path), **kwargs)
        else:
            raise NoDbtPathSupplied("Either path or path is required.")

    def read_models(
        self,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        return self.parser.read_models(include_tags, docs_url)
