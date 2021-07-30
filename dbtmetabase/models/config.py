from dataclasses import dataclass, field
from typing import Optional, Iterable, Union


@dataclass
class MetabaseConfig:
    # Metabase Client
    metabase_database: str
    metabase_host: str
    metabase_user: str
    metabase_password: str
    # Metabase additional connection opts
    metabase_use_http: bool = False
    metabase_verify: Union[str, bool] = True
    # Metabase Sync
    metabase_sync_skip: bool = False
    metabase_sync_timeout: Optional[int] = None


@dataclass
class DbtConfig:
    # dbt Reader
    dbt_database: str
    dbt_manifest_path: Optional[str] = None
    dbt_path: Optional[str] = None
    # dbt Target Models
    dbt_schema: Optional[str] = None
    dbt_schema_excludes: Iterable = field(default_factory=list)
    dbt_includes: Iterable = field(default_factory=list)
    dbt_excludes: Iterable = field(default_factory=list)
