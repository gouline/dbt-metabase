import logging
import sys
import os
import argparse

from .metabase import MetabaseClient
from .parsers.dbt_folder import DbtFolderReader
from .parsers.dbt_manifest import DbtManifestReader
from .models.config import MetabaseConfig, DbtConfig
from .utils import get_version

from typing import Iterable, List, Union, Optional

__version__ = get_version()


def models(
    metabase_config: MetabaseConfig,
    dbt_config: DbtConfig,
    dbt_include_tags: bool = True,
    dbt_docs_url: Optional[str] = None,
):
    """Exports models from dbt to Metabase.

    Args:
        metabase_config (str): Source database name.
        dbt_config (str): Target Metabase database name. Database in Metabase is aliased.
        dbt_include_tags (bool, optional): Append the dbt tags to the end of the table description. Defaults to True.
        dbt_docs_url (str, optional): URL to your dbt docs hosted catalog, a link will be appended to the model description (only works for manifest parsing). Defaults to None.
        dbt_includes (Iterable, optional): Model names to limit processing to. Defaults to None.
        dbt_excludes (Iterable, optional): Model names to exclude. Defaults to None.
    """

    # Assertions
    if dbt_config.path and dbt_config.manifest_path:
        logging.warning("Prioritizing manifest path arg")
        dbt_config.path = None
    if dbt_config.path and not dbt_config.schema:
        logging.error(
            "Must supply a schema if using YAML parser, it is used to resolve foreign key relations and which Metabase models to propagate documentation to"
        )
    if dbt_config.path:
        if dbt_config.database:
            logging.info(
                "Argument --database %s is unused in dbt_project yml parser. Use manifest parser instead.",
                dbt_config.database,
            )
        if dbt_docs_url:
            logging.info(
                "Argument --dbt_docs_url %s is unused in dbt_project yml parser. Use manifest parser instead.",
                dbt_docs_url,
            )

    # Instantiate Metabase client
    mbc = MetabaseClient(
        host=metabase_config.host,
        user=metabase_config.user,
        password=metabase_config.password,
        use_http=metabase_config.use_http,
        verify=metabase_config.verify,
    )
    reader: Union[DbtFolderReader, DbtManifestReader]

    # Resolve dbt reader being either YAML or manifest.json based
    if dbt_config.manifest_path:
        reader = DbtManifestReader(os.path.expandvars(dbt_config.manifest_path))
    elif dbt_config.path:
        reader = DbtFolderReader(os.path.expandvars(dbt_config.path))

    if dbt_config.schema_excludes:
        dbt_config.schema_excludes = {
            _schema.upper() for _schema in dbt_config.schema_excludes
        }

    # Process dbt stuff
    dbt_models = reader.read_models(
        database=dbt_config.database,
        schema=dbt_config.schema,
        schema_excludes=dbt_config.schema_excludes,
        includes=dbt_config.includes,
        excludes=dbt_config.excludes,
        include_tags=dbt_include_tags,
        docs_url=dbt_docs_url,
    )

    # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
    if not metabase_config.sync_skip:
        if metabase_config.sync_timeout is not None and not mbc.sync_and_wait(
            metabase_config.database,
            dbt_models,
            metabase_config.sync_timeout,
        ):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    # Process Metabase stuff
    mbc.export_models(
        database=metabase_config.database,
        models=dbt_models,
        aliases=reader.catch_aliases,
    )


def exposures(
    metabase_config: MetabaseConfig,
    dbt_config: DbtConfig,
    output_path: str,
    output_name: str,
    include_personal_collections: bool = False,
    collection_excludes: Optional[Iterable] = None,
):
    """Extracts and imports exposures from Metabase to dbt.

    Args:
        metabase_config (str): Source database name.
        dbt_config (str): Target Metabase database name. Database in Metabase is aliased.
        output_path (str): Append the dbt tags to the end of the table description. Defaults to True.
        output_name (str): URL to your dbt docs hosted catalog, a link will be appended to the model description (only works for manifest parsing). Defaults to None.
        include_personal_collections (bool, optional): Model names to limit processing to. Defaults to None.
        collection_excludes (Iterable, optional): Model names to exclude. Defaults to None.
    """

    # Assertions
    if dbt_config.path and dbt_config.manifest_path:
        logging.warning("Prioritizing manifest path arg")
        dbt_config.path = None
    if dbt_config.path and not dbt_config.schema:
        logging.error(
            "Must supply a schema if using YAML parser, it is used to resolve foreign key relations and which Metabase models to propagate documentation to"
        )

    # Instantiate Metabase client
    mbc = MetabaseClient(
        host=metabase_config.host,
        user=metabase_config.user,
        password=metabase_config.password,
        use_http=metabase_config.use_http,
        verify=metabase_config.verify,
    )
    reader: Union[DbtFolderReader, DbtManifestReader]

    # Resolve dbt reader being either YAML or manifest.json based
    if dbt_config.manifest_path:
        reader = DbtManifestReader(os.path.expandvars(dbt_config.manifest_path))
    elif dbt_config.path:
        reader = DbtFolderReader(os.path.expandvars(dbt_config.path))

    if dbt_config.schema_excludes:
        dbt_config.schema_excludes = {
            _schema.upper() for _schema in dbt_config.schema_excludes
        }

    # Process dbt stuff
    dbt_models = reader.read_models(
        database=dbt_config.database,
        schema=dbt_config.schema,
        schema_excludes=dbt_config.schema_excludes,
        includes=dbt_config.includes,
        excludes=dbt_config.excludes,
    )

    # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
    if not metabase_config.sync_skip:
        if metabase_config.sync_timeout is not None and not mbc.sync_and_wait(
            metabase_config.database,
            dbt_models,
            metabase_config.sync_timeout,
        ):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    # Process Metabase stuff
    mbc.extract_exposures(
        models=dbt_models,
        output_path=output_path,
        output_name=output_name,
        include_personal_collections=include_personal_collections,
        collection_excludes=collection_excludes,
    )


def main(args: List = None):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(
        prog="PROG", description="Model synchronization from dbt to Metabase."
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument("command", choices=["export"], help="command to execute")

    # Commands
    parser.add_argument(
        "command", choices=["models", "exposures"], help="Command to execute"
    )

    parser_dbt = parser.add_argument_group("dbt Parser")
    parser_metabase = parser.add_argument_group("Metabase Client")
    parser_models = parser.add_argument_group("`models` optional arguments")
    parser_exposures = parser.add_argument_group("`exposures` optional arguments")

    # dbt arguments
    parser_dbt.add_argument(
        "--dbt_database",
        metavar="DB",
        required=True,
        help="Target database name as specified in dbt",
    )
    group = parser_dbt.add_mutually_exclusive_group()
    group.add_argument(
        "--dbt_path",
        help="Path to dbt project. Cannot be specified with --dbt_manifest_path",
    )
    group.add_argument(
        "--dbt_manifest_path",
        help="Path to dbt manifest.json (typically located in the /target/ directory of the dbt project directory). Cannot be specified with --dbt_path",
    )
    parser_dbt.add_argument(
        "--dbt_schema",
        help="Target schema. Should be passed if using folder parser",
    )
    parser_dbt.add_argument(
        "--dbt_schema_excludes",
        nargs="*",
        default=[],
        help="Target schemas to exclude. Ignored in folder parser",
    )
    parser_dbt.add_argument(
        "--dbt_includes",
        metavar="MODELS",
        nargs="*",
        default=[],
        help="Model names to limit processing to",
    )
    parser_dbt.add_argument(
        "--dbt_excludes",
        metavar="MODELS",
        nargs="*",
        default=[],
        help="Model names to exclude",
    )

    # Metabase arguments
    parser_metabase.add_argument(
        "--metabase_database",
        metavar="DB",
        required=True,
        help="Target database name as set in Metabase (typically aliased)",
    )
    parser_metabase.add_argument(
        "--metabase_host", metavar="HOST", required=True, help="Metabase hostname"
    )
    parser_metabase.add_argument(
        "--metabase_user", metavar="USER", required=True, help="Metabase username"
    )
    parser_metabase.add_argument(
        "--metabase_password", metavar="PASS", required=True, help="Metabase password"
    )
    parser_metabase.add_argument(
        "--metabase_use_http",
        action="store_true",
        help="use HTTP to connect to Metabase instead of HTTPS",
    )
    parser_metabase.add_argument(
        "--metabase_verify",
        metavar="CERT",
        help="Path to certificate bundle used by Metabase client",
    )
    parser_metabase.add_argument(
        "--metabase_sync_skip",
        action="store_true",
        help="Skip synchronizing Metabase database before export",
    )
    parser_metabase.add_argument(
        "--metabase_sync_timeout",
        metavar="SECS",
        type=int,
        help="Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success",
    )

    # Models specific args
    parser_models.add_argument(
        "--dbt_docs_url",
        metavar="URL",
        help="Pass in URL to dbt docs site. Appends dbt docs URL for each model to Metabase table description (default None)",
    )
    parser_models.add_argument(
        "--dbt_include_tags",
        action="store_true",
        default=False,
        help="Append tags to Table descriptions in Metabase (default False)",
    )

    # Exposures specific args
    parser_exposures.add_argument(
        "--output_path",
        default="./",
        help="Path where generated YAML will be outputted (default local dir)",
    )
    parser_exposures.add_argument(
        "--output_name",
        default="metabase_exposures",
        help="Used in Exposure extractor, name of generated YAML file (default metabase_exposures)",
    )
    parser_exposures.add_argument(
        "--include_personal_collections",
        action="store_true",
        default=False,
        help="Include personal collections in exposure extraction (default False)",
    )
    parser_exposures.add_argument(
        "--collection_excludes",
        nargs="*",
        default=[],
        help="Exclude a list of collections from exposure parsing (default [])",
    )

    # Common/misc arguments
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Verbose output",
    )

    parsed = parser.parse_args(args=args)

    if parsed.verbose:
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)

    # These args drive loading the Metabase client and dbt models and are prerequisites to any functionality of dbt-metabase
    metabase_config = MetabaseConfig(
        host=parsed.metabase_host,
        user=parsed.metabase_user,
        password=parsed.metabase_password,
        use_http=parsed.metabase_use_http,
        verify=parsed.metabase_verify,
        database=parsed.metabase_database,
        sync_skip=parsed.metabase_sync_skip,
        sync_timeout=parsed.metabase_sync_timeout,
    )
    dbt_config = DbtConfig(
        path=parsed.dbt_path,
        manifest_path=parsed.dbt_manifest_path,
        database=parsed.dbt_database,
        schema=parsed.dbt_schema,
        schema_excludes=parsed.dbt_schema_excludes,
        includes=parsed.dbt_includes,
        excludes=parsed.dbt_excludes,
    )

    if parsed.command == "models":
        models(
            metabase_config,
            dbt_config,
            dbt_docs_url=parsed.dbt_docs_url,
            dbt_include_tags=parsed.dbt_include_tags,
        )
    elif parsed.command == "exposures":
        exposures(
            metabase_config,
            dbt_config,
            output_path=parsed.output_path,
            output_name=parsed.output_name,
            include_personal_collections=parsed.include_personal_collections,
            collection_excludes=parsed.collection_excludes,
        )
    else:
        logging.error("Invalid command. Must be one of either 'models' or 'exposures'.")
