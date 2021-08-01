from dataclasses import dataclass, field
from typing import Optional, Iterable, Union


@dataclass
class MetabaseConfig:
    # Metabase Client
    database: str
    host: str
    user: str
    password: str
    # Metabase additional connection opts
    use_http: bool = False
    verify: Union[str, bool] = True
    # Metabase Sync
    sync_skip: bool = False
    sync_timeout: Optional[int] = None


@dataclass
class DbtConfig:
    # dbt Reader
    database: str
    manifest_path: Optional[str] = None
    path: Optional[str] = None
    # dbt Target Models
    schema: Optional[str] = None
    schema_excludes: Iterable = field(default_factory=list)
    includes: Iterable = field(default_factory=list)
    excludes: Iterable = field(default_factory=list)
