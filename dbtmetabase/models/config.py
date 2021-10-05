from typing import Optional, Iterable, Union


class MetabaseConfig:
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


class DbtConfig:
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

    @property
    def schema_excludes(self) -> Iterable:
        return self._schema_excludes

    @schema_excludes.setter
    def schema_excludes(self, value: Iterable) -> None:
        self._schema_excludes = list({schema.upper() for schema in value})
