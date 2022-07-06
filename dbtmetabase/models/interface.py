import logging
from os.path import expandvars
from typing import Optional, Union, List, Tuple, MutableMapping, Iterable

from .metabase import MetabaseModel
from .exceptions import NoDbtPathSupplied, NoDbtSchemaSupplied
from ..parsers.dbt import DbtReader
from ..parsers.dbt_folder import DbtFolderReader
from ..parsers.dbt_manifest import DbtManifestReader
from ..metabase import MetabaseClient


class MetabaseInterface:
    """Interface for interacting with Metabase and preparing a client object."""

    _client: Optional[MetabaseClient] = None

    def __init__(
        self,
        database: str,
        host: str,
        user: str,
        password: str,
        session_id: Optional[str] = None,
        use_http: bool = False,
        verify: Optional[Union[str, bool]] = True,
        sync: bool = True,
        sync_timeout: Optional[int] = None,
        exclude_sources: bool = False,
    ):
        """Constructor.

        Args:
            database (str): Target database name as set in Metabase (typically aliased).
            host (str): Metabase hostname.
            user (str): Metabase username.
            password (str): Metabase password.
            session_id (Optional[str], optional): Session ID. Defaults to None.
            use_http (bool, optional): Use HTTP to connect to Metabase.. Defaults to False.
            verify (Optional[Union[str, bool]], optional): Path to custom certificate bundle to be used by Metabase client. Defaults to True.
            sync (bool, optional): Attempt to synchronize Metabase schema with local models. Defaults to True.
            sync_timeout (Optional[int], optional): Synchronization timeout (in secs). Defaults to None.
            exclude_sources (bool, optional): Exclude exporting sources. Defaults to False.
        """

        # Metabase Client
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.session_id = session_id
        # Metabase additional connection opts
        self.use_http = use_http
        self.verify = verify
        # Metabase Sync
        self.sync = sync
        self.sync_timeout = sync_timeout
        self.exclude_sources = exclude_sources

    @property
    def client(self) -> MetabaseClient:
        if self._client is None:
            self.prepare_metabase_client()
            assert self._client
        return self._client

    def prepare_metabase_client(self, dbt_models: Optional[List[MetabaseModel]] = None):
        """Prepares the metabase client which can then after be accessed via the `client` property

        Args:
            dbt_models (Optional[List[MetabaseModel]]): Used if sync is enabled to verify all dbt models passed exist in Metabase

        Raises:
            MetabaseUnableToSync: This error is raised if sync is enabled and a timeout is explicitly set in the `Metabase` object config
        """

        if self._client is not None:
            # Already prepared
            return

        if dbt_models is None:
            dbt_models = []

        self._client = MetabaseClient(
            host=self.host,
            user=self.user,
            password=self.password,
            use_http=self.use_http,
            verify=self.verify,
            session_id=self.session_id,
            exclude_sources=self.exclude_sources,
        )

        # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
        if self.sync:
            self._client.sync_and_wait(
                self.database,
                dbt_models,
                self.sync_timeout,
            )


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
