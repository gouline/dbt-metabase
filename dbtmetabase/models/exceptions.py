class NoDbtPathSupplied(Exception):
    """Thrown when no argument for dbt path has been supplied"""


class NoDbtSchemaSupplied(Exception):
    """Thrown when using folder parser without supplying a schema"""


class MetabaseClientNotInstantiated(Exception):
    """Thrown when trying to access metabase client from interface prior to instantiation via class method"""


class MetabaseUnableToSync(Exception):
    """Thrown when Metabase cannot sync / align models with dbt model"""


class DbtParserNotInstantiated(Exception):
    """Thrown when trying to access dbt reader from interface prior to instantiation via class method"""
