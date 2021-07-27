import logging
import importlib.metadata


def get_version() -> str:
    """Checks _version.py or build metadata for package version.

    Returns:
        str: Version string.
    """

    try:
        from ._version import version

        return version
    except ModuleNotFoundError:
        logging.debug("No _version.py found")

    try:
        return importlib.metadata.version("dbt-metabase")
    except importlib.metadata.PackageNotFoundError:
        logging.warning("No version found in metadata")

    return "0.0.0-UNKONWN"
