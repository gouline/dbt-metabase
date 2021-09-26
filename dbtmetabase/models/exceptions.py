class NoDbtPathSupplied(Exception):
    """Thrown when no argument for dbt path has been supplied"""

    pass


class NoDbtSchemaSupplied(Exception):
    """Thrown when using folder parser without supplying a schema"""

    pass


class MetabaseClientNotInstantiated(Exception):
    """Thrown when trying to access metabase client from interface prior to instantiation via class method"""

    pass


class MetabaseUnableToSync(Exception):
    """Thrown when Metabase cannot sync / align models with dbt model"""

    pass


class DbtParserNotInstantiated(Exception):
    """Thrown when trying to access dbt reader from interface prior to instantiation via class method"""

    pass
