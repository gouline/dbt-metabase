import logging


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

    # importlib is only available on Python 3.6+
    # pylint also needs to be ignored
    try:
        # pylint: disable=import-error,no-name-in-module,no-member
        import importlib.metadata

        try:
            return importlib.metadata.version("dbt-metabase")
        except importlib.metadata.PackageNotFoundError:
            logging.warning("No version found in metadata")
    except ModuleNotFoundError:
        logging.warning(
            "metadata package not available to retrieve the package version"
        )

    return "0.0.0-UNKONWN"
