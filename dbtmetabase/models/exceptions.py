class NoDbtPathSupplied(Exception):
    """Thrown when no argument for dbt path has been supplied"""


class NoDbtSchemaSupplied(Exception):
    """Thrown when using folder parser without supplying a schema"""


class NoMetabaseCredentialsSupplied(Exception):
    """Thrown when credentials or session id not supplied"""


class MetabaseUnableToSync(Exception):
    """Thrown when Metabase cannot sync / align models with dbt model"""


class MetabaseRuntimeError(Exception):
    """Thrown when Metabase execution failed."""


class MetabaseCertificateImplementationError(Exception):
    """Thrown when cert argument and pkcs12_data argument are both defined"""


class ExtraLibraryInstallationError(Exception):
    """Thrown when an extra library is not installed but imported"""
