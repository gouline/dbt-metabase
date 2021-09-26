import logging
import os.path
from typing import Optional, Iterable, Union, List

from .metabase import MetabaseModel
from ..metabase import MetabaseClient
from .exceptions import (
    NoDbtPathSupplied,
    NoDbtSchemaSupplied,
    MetabaseClientNotInstantiated,
    DbtParserNotInstantiated,
    MetabaseUnableToSync,
)
from ..parsers.dbt_folder import DbtFolderReader
from ..parsers.dbt_manifest import DbtManifestReader


class Metabase:
    def __init__(
        self,
        database: str,
        host: str,
        user: str,
        password: str,
        use_http: bool = False,
        verify: Optional[Union[str, bool]] = True,
        sync: bool = True,
        sync_timeout: Optional[int] = None,
    ):
        # Metabase Client
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        # Metabase additional connection opts
        self.use_http = use_http
        self.verify = verify
        # Metabase Sync
        self.sync = sync
        self.sync_timeout = sync_timeout
        self._client: Optional["MetabaseClient"] = None

    @property
    def client(self) -> "MetabaseClient":
        if self._client is None:
            raise MetabaseClientNotInstantiated(
                "Metabase client is not yet instantiated. Call `prepare_metabase_client` method first"
            )
        return self._client

    def prepare_metabase_client(self, dbt_models: Optional[List[MetabaseModel]] = None):
        """Prepares the metabase client which can then after be accessed via the `client` property

        Args:
            dbt_models (Optional[List[MetabaseModel]]): Used if sync is enabled to verify all dbt models passed exist in Metabase

        Raises:
            MetabaseUnableToSync: This error is raised if sync is enabled and a timeout is explicitly set in the `Metabase` object config
        """
        if dbt_models is None:
            dbt_models = []

        self._client = MetabaseClient(
            host=self.host,
            user=self.user,
            password=self.password,
            use_http=self.use_http,
            verify=self.verify,
        )

        # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
        if self.sync:
            if self.sync_timeout is not None and not self._client.sync_and_wait(
                self.database,
                dbt_models,
                self.sync_timeout,
            ):
                logging.critical("Sync timeout reached, models still not compatible")
                raise MetabaseUnableToSync(
                    "Unable to align models between dbt target models and Metabase"
                )


class Dbt:
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
        self._parser: Optional[Union[DbtManifestReader, DbtFolderReader]] = None
        self.validate_config()
        self.prepare_dbt_parser()

    @property
    def schema_excludes(self) -> Iterable:
        return self._schema_excludes

    @schema_excludes.setter
    def schema_excludes(self, value: Iterable) -> None:
        self._schema_excludes = list({schema.upper() for schema in value})

    @property
    def parser(self) -> Union[DbtManifestReader, DbtFolderReader]:
        if self._parser is None:
            raise DbtParserNotInstantiated(
                "dbt reader is not yet instantiated. Call `prepare_dbt_parser` method first"
            )
        return self._parser

    def validate_config(self) -> None:
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

    def prepare_dbt_parser(self) -> None:
        """Resolve dbt reader being either YAML or manifest.json based which can then after be accessed via the `parser` property"""
        if self.manifest_path:
            self._parser = DbtManifestReader(os.path.expandvars(self.manifest_path))
        elif self.path:
            self._parser = DbtFolderReader(os.path.expandvars(self.path))
        else:
            raise NoDbtPathSupplied(
                "One of either dbt_path or dbt_manifest_path is required."
            )
