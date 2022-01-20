class NoDbtPathSupplied(Exception):
    """Thrown when no argument for dbt path has been supplied"""


class NoDbtSchemaSupplied(Exception):
    """Thrown when using folder parser without supplying a schema"""


class MetabaseUnableToSync(Exception):
    """Thrown when Metabase cannot sync / align models with dbt model"""
