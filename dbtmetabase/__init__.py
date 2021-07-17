import logging
import sys
import os
import re
import argparse

from .metabase import MetabaseClient
from .parsers.dbt_folder import DbtFolderReader
from .parsers.dbt_manifest import DbtManifestReader

from typing import Mapping, Iterable, List, Union, Literal

__version__ = "0.9.0"


def execute(
    # Metabase Client
    metabase_database: str,
    metabase_host: str,
    metabase_user: str,
    metabase_password: str,
    # dbt Reader
    dbt_database: str,
    dbt_manifest_path: str = "",
    dbt_path: str = "",
    # Invocation Command
    command: Literal["export_models", "extract_exposures"] = "export_models",
    # dbt Target Models
    schema: str = "public",
    schema_excludes: Iterable = None,
    # Metabase additional connection opts
    metabase_use_http: bool = False,
    metabase_verify: Union[str, bool] = True,
    # Metabase Sync
    metabase_sync_skip: bool = False,
    metabase_sync_timeout: int = None,
    # Documentation Propagation Opts
    include_tags: bool = True,
    dbt_docs_url: str = None,
    includes: Iterable = None,
    excludes: Iterable = None,
    # Exposure Parsing Opts
    output_path: str = None,
    output_name: str = None,
    include_personal_collections: bool = True,
    exclude_collections: Iterable = None,
):
    """Exports models from dbt to Metabase.

    Args:
        dbt_database (str): Source database name.
        metabase_database (str): Target Metabase database name. Database in Metabase is aliased.
        metabase_host (str): Metabase hostname.
        metabase_user (str): Metabase username.
        metabase_password (str): Metabase password.
        dbt_manifest_path (str, optional): Path to dbt project manifest.json [Primary]. Defaults to "".
        dbt_path (str, optional): Path to dbt project. [Alternative]. Defaults to "".
        dbt_docs_url (str, optional): URL to your dbt docs hosted catalog, a link will be appended to the model description (only works for manifest parsing). Defaults to None.
        metabase_use_http (bool, optional): Use HTTP to connect to Metabase instead of the default HTTPS. Defaults to False.
        metabase_verify (Union[str, bool], optional): Supply path to certificate or disable verification. Defaults to True.
        metabase_sync_skip (bool, optional): Skip synchronizing Metabase database before export. Defaults to False.
        metabase_sync_timeout (int, optional): Metabase synchronization timeout in seconds. Defaults to None.
        schema (str, optional): Target schema name. Defaults to "public".
        schema_excludes (Iterable, optional): Alternative to target schema, specify schema exclusions (only works for manifest parsing). Defaults to None.
        includes (Iterable, optional): Model names to limit processing to. Defaults to None.
        excludes (Iterable, optional): Model names to exclude. Defaults to None.
        include_tags (bool, optional): Append the dbt tags to the end of the table description. Defaults to True.
    """

    if schema_excludes is None:
        schema_excludes = []
    if includes is None:
        includes = []
    if excludes is None:
        excludes = []

    # Assertions
    assert bool(dbt_path) != bool(
        dbt_manifest_path
    ), "Bad arguments. dbt_path and dbt_manifest_path cannot be provide at the same time. One option must be specified."
    if dbt_path:
        assert (
            schema and not schema_excludes
        ), "Must target a single schema if using yaml parser, multiple schemas not supported."
    assert command in [
        "export_models",
        "extract_exposures",
    ], f"Invalid command {command}, must be one of `export_models`, `extract_exposures`"

    # Instantiate Metabase client
    mbc = MetabaseClient(
        host=metabase_host,
        user=metabase_user,
        password=metabase_password,
        use_http=metabase_use_http,
        verify=metabase_verify,
    )
    reader: Union[DbtFolderReader, DbtManifestReader]

    # Resolve dbt reader being either YAML or manifest.json based
    if dbt_path:
        reader = DbtFolderReader(os.path.expandvars(dbt_path))
    else:
        reader = DbtManifestReader(os.path.expandvars(dbt_manifest_path))

    if schema_excludes:
        schema_excludes = {schema.upper() for schema in schema_excludes}

    # Process dbt stuff
    models = reader.read_models(
        database=dbt_database,
        schema=schema,
        schema_excludes=schema_excludes,
        includes=includes,
        excludes=excludes,
        include_tags=include_tags,
        dbt_docs_url=dbt_docs_url,
    )

    # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
    if not metabase_sync_skip:
        if metabase_sync_timeout is not None and not mbc.sync_and_wait(
            metabase_database, models, metabase_sync_timeout
        ):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    # Process Metabase stuff
    getattr(mbc, command)(
        database=metabase_database,
        aliases=reader.catch_aliases,
        schema_excludes=schema_excludes,
        output_path=output_path,
        output_name=output_name,
        models=models,
        include_personal_collections=include_personal_collections,
        exclude_collections=exclude_collections,
    )


def main(args: List = None):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(
        description="Model synchronization from dbt to Metabase."
    )

    # Commands
    parser.add_argument(
        "command", choices=["export", "exposures"], help="command to execute"
    )

    # dbt arguments
    parser.add_argument(
        "--dbt_database",
        metavar="DB",
        required=True,
        help="Target database name as specified in dbt",
    )
    parser.add_argument(
        "--dbt_path",
        help="Path to dbt project. Cannot be specified with --dbt_manifest_path",
    )
    parser.add_argument(
        "--dbt_manifest_path",
        help="Path to dbt manifest.json (typically located in the /target/ directory of the dbt project directory). Cannot be specified with --dbt_path",
    )
    parser.add_argument(
        "--dbt_docs_url",
        metavar="URL",
        help="Pass in URL to dbt docs site. Appends dbt docs URL for each model to Metabase table description",
    )

    # Metabase arguments
    parser.add_argument(
        "--metabase_database",
        metavar="DB",
        required=True,
        help="Target database name as set in Metabase (typically aliased)",
    )
    parser.add_argument(
        "--metabase_host", metavar="HOST", required=True, help="Metabase hostname"
    )
    parser.add_argument(
        "--metabase_user", metavar="USER", required=True, help="Metabase username"
    )
    parser.add_argument(
        "--metabase_password", metavar="PASS", required=True, help="Metabase password"
    )
    parser.add_argument(
        "--metabase_use_http",
        action="store_true",
        help="use HTTP to connect to Metabase instead of HTTPS",
    )
    parser.add_argument(
        "--metabase_verify",
        metavar="CERT",
        help="Path to certificate bundle used by Metabase client",
    )
    parser.add_argument(
        "--metabase_sync_skip",
        action="store_true",
        help="Skip synchronizing Metabase database before export",
    )
    parser.add_argument(
        "--metabase_sync_timeout",
        metavar="SECS",
        type=int,
        help="Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success",
    )

    # Common/misc arguments
    parser.add_argument(
        "--schema_excludes",
        help="Target schemas to exclude. Cannot be specified with --schema. Will sync all schemas not excluded",
    )
    parser.add_argument(
        "--includes",
        metavar="MODELS",
        nargs="*",
        default=[],
        help="Model names to limit processing to",
    )
    parser.add_argument(
        "--excludes",
        metavar="MODELS",
        nargs="*",
        default=[],
        help="Model names to exclude",
    )
    parser.add_argument(
        "--include_tags",
        action="store_true",
        default=False,
        help="Append tags to Table descriptions in Metabase",
    )
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
    primary_args = {
        "dbt_path": parsed.dbt_path,
        "dbt_manifest_path": parsed.dbt_manifest_path,
        "dbt_database": parsed.dbt_database,
        "metabase_host": parsed.metabase_host,
        "metabase_user": parsed.metabase_user,
        "metabase_password": parsed.metabase_password,
        "metabase_use_http": parsed.metabase_use_http,
        "metabase_verify": parsed.metabase_verify,
        "metabase_database": parsed.metabase_database,
        "schema_excludes": parsed.schema_excludes,
        "metabase_sync_skip": parsed.metabase_sync_skip,
        "metabase_sync_timeout": parsed.metabase_sync_timeout,
        "includes": parsed.includes,
        "excludes": parsed.excludes,
    }

    if parsed.command == "export":
        execute(
            **primary_args,
            include_tags=parsed.include_tags,
            dbt_docs_url=parsed.dbt_docs_url,
            command="export_models",
        )
    elif parsed.command == "exposures":
        execute(
            **primary_args,
            output_path=parsed.output_path,
            output_name=parsed.output_name,
            include_personal_collections=parsed.include_personal_collections,
            exclude_collections=parsed.exclude_collections,
            command="extract_exposures",
        )
    else:
        logging.warning(
            "Invalid command. Must be one of either 'export' or 'exposures'."
        )
