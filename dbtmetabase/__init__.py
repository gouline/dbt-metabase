import logging
import sys
import os
import argparse

from .metabase import MetabaseClient
from .parsers.dbt_folder import DbtFolderReader
from .parsers.dbt_manifest import DbtManifestReader
from .models.config import MetabaseConfig, dbtConfig

from typing import Iterable, List, Union, Optional

from ._version import version as __version__


def models(
    metabase_config: MetabaseConfig,
    dbt_config: dbtConfig,
    include_tags: bool = True,
    dbt_docs_url: Optional[str] = None,
):
    """Exports models from dbt to Metabase.

    Args:
        metabase_config (str): Source database name.
        dbt_config (str): Target Metabase database name. Database in Metabase is aliased.
        include_tags (bool, optional): Append the dbt tags to the end of the table description. Defaults to True.
        dbt_docs_url (str, optional): URL to your dbt docs hosted catalog, a link will be appended to the model description (only works for manifest parsing). Defaults to None.
        includes (Iterable, optional): Model names to limit processing to. Defaults to None.
        excludes (Iterable, optional): Model names to exclude. Defaults to None.
    """

    # Assertions
    if dbt_config.dbt_path and dbt_config.dbt_manifest_path:
        logging.warning("Prioritizing manifest path arg")
        dbt_config.dbt_path = None
    if dbt_config.dbt_path and not dbt_config.schema:
        logging.error(
            "Must supply a schema if using YAML parser, it is used to resolve foreign key relations and which Metabase models to propagate documentation to"
        )

    # Instantiate Metabase client
    mbc = MetabaseClient(
        host=metabase_config.metabase_host,
        user=metabase_config.metabase_user,
        password=metabase_config.metabase_password,
        use_http=metabase_config.metabase_use_http,
        verify=metabase_config.metabase_verify,
    )
    reader: Union[DbtFolderReader, DbtManifestReader]

    # Resolve dbt reader being either YAML or manifest.json based
    if dbt_config.dbt_manifest_path:
        reader = DbtManifestReader(os.path.expandvars(dbt_config.dbt_manifest_path))
    elif dbt_config.dbt_path:
        reader = DbtFolderReader(os.path.expandvars(dbt_config.dbt_path))

    if dbt_config.schema_excludes:
        dbt_config.schema_excludes = {
            schema.upper() for schema in dbt_config.schema_excludes
        }

    # Process dbt stuff
    dbt_models = reader.read_models(
        database=dbt_config.dbt_database,
        schema=dbt_config.schema,
        schema_excludes=dbt_config.schema_excludes,
        includes=dbt_config.includes,
        excludes=dbt_config.excludes,
        include_tags=include_tags,
        dbt_docs_url=dbt_docs_url,
    )

    # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
    if not metabase_config.metabase_sync_skip:
        if metabase_config.metabase_sync_timeout is not None and not mbc.sync_and_wait(
            metabase_config.metabase_database,
            dbt_models,
            metabase_config.metabase_sync_timeout,
        ):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    # Process Metabase stuff
    mbc.export_models(
        database=metabase_config.metabase_database,
        models=dbt_models,
        aliases=reader.catch_aliases,
    )


def exposures(
    metabase_config: MetabaseConfig,
    dbt_config: dbtConfig,
    output_path: str,
    output_name: str,
    include_personal_collections: bool = False,
    exclude_collections: Optional[Iterable] = None,
):
    """Extracts and imports exposures from Metabase to dbt.

    Args:
        metabase_config (str): Source database name.
        dbt_config (str): Target Metabase database name. Database in Metabase is aliased.
        output_path (str): Append the dbt tags to the end of the table description. Defaults to True.
        output_name (str): URL to your dbt docs hosted catalog, a link will be appended to the model description (only works for manifest parsing). Defaults to None.
        include_personal_collections (bool, optional): Model names to limit processing to. Defaults to None.
        exclude_collections (Iterable, optional): Model names to exclude. Defaults to None.
    """

    # Assertions
    if dbt_config.dbt_path and dbt_config.dbt_manifest_path:
        logging.warning("Prioritizing manifest path arg")
        dbt_config.dbt_path = None
    if dbt_config.dbt_path and not dbt_config.schema:
        logging.error(
            "Must supply a schema if using YAML parser, it is used to resolve foreign key relations and which Metabase models to propagate documentation to"
        )

    # Instantiate Metabase client
    mbc = MetabaseClient(
        host=metabase_config.metabase_host,
        user=metabase_config.metabase_user,
        password=metabase_config.metabase_password,
        use_http=metabase_config.metabase_use_http,
        verify=metabase_config.metabase_verify,
    )
    reader: Union[DbtFolderReader, DbtManifestReader]

    # Resolve dbt reader being either YAML or manifest.json based
    if dbt_config.dbt_manifest_path:
        reader = DbtManifestReader(os.path.expandvars(dbt_config.dbt_manifest_path))
    elif dbt_config.dbt_path:
        reader = DbtFolderReader(os.path.expandvars(dbt_config.dbt_path))

    if dbt_config.schema_excludes:
        dbt_config.schema_excludes = {
            schema.upper() for schema in dbt_config.schema_excludes
        }

    # Process dbt stuff
    dbt_models = reader.read_models(
        database=dbt_config.dbt_database,
        schema=dbt_config.schema,
        schema_excludes=dbt_config.schema_excludes,
        includes=dbt_config.includes,
        excludes=dbt_config.excludes,
    )

    # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
    if not metabase_config.metabase_sync_skip:
        if metabase_config.metabase_sync_timeout is not None and not mbc.sync_and_wait(
            metabase_config.metabase_database,
            dbt_models,
            metabase_config.metabase_sync_timeout,
        ):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    # Process Metabase stuff
    mbc.extract_exposures(
        models=dbt_models,
        output_path=output_path,
        output_name=output_name,
        include_personal_collections=include_personal_collections,
        exclude_collections=exclude_collections,
    )


def main(args: List = None):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(
        prog="PROG", description="Model synchronization from dbt to Metabase."
    )

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
        "--schema_excludes",
        nargs="*",
        default=[],
        help="Target schemas to exclude. Cannot be specified with --schema. Will sync all schemas not excluded",
    )
    parser_dbt.add_argument(
        "--includes",
        metavar="MODELS",
        nargs="*",
        default=[],
        help="Model names to limit processing to",
    )
    parser_dbt.add_argument(
        "--excludes",
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
        "--include_tags",
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
        dest="exclude_collections",
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
        metabase_host=parsed.metabase_host,
        metabase_user=parsed.metabase_user,
        metabase_password=parsed.metabase_password,
        metabase_use_http=parsed.metabase_use_http,
        metabase_verify=parsed.metabase_verify,
        metabase_database=parsed.metabase_database,
        metabase_sync_skip=parsed.metabase_sync_skip,
        metabase_sync_timeout=parsed.metabase_sync_timeout,
    )
    dbt_config = dbtConfig(
        dbt_path=parsed.dbt_path,
        dbt_manifest_path=parsed.dbt_manifest_path,
        dbt_database=parsed.dbt_database,
        schema_excludes=parsed.schema_excludes,
        includes=parsed.includes,
        excludes=parsed.excludes,
    )

    if parsed.command == "models":
        models(
            metabase_config,
            dbt_config,
            dbt_docs_url=parsed.dbt_docs_url,
            include_tags=parsed.include_tags,
        )
    elif parsed.command == "exposures":
        exposures(
            metabase_config,
            dbt_config,
            output_path=parsed.output_path,
            output_name=parsed.output_name,
            include_personal_collections=parsed.include_personal_collections,
            exclude_collections=parsed.exclude_collections,
        )
    else:
        logging.error("Invalid command. Must be one of either 'export' or 'exposures'.")
