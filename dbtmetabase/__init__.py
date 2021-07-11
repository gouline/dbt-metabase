import logging
import sys
import os

from .metabase import MetabaseClient
from .parsers.dbt_folder import DbtFolderReader
from .parsers.dbt_manifest import DbtManifestReader

from typing import Mapping, Iterable, List, Union

__version__ = "0.8.0"


def export(
    mb_host: str,
    mb_user: str,
    mb_password: str,
    database: str,
    dbt_database: str,
    dbt_manifest_path: str = "",
    dbt_path: str = "",
    schema: str = "public",
    schemas_excludes: Iterable = None,
    mb_use_http: bool = False,
    mb_verify: Union[str, bool] = True,
    mb_sync_skip: bool = False,
    mb_sync_timeout: int = None,
    includes: Iterable = None,
    excludes: Iterable = None,
    include_tags: bool = True,
    dbt_docs_url: str = None,
):
    """Exports models from dbt to Metabase.

    Arguments:
        mb_host {str} -- Metabase hostname.
        mb_user {str} -- Metabase username.
        mb_password {str} -- Metabase password.
        database {str} -- Target Metabase database name. Database in Metabase is aliased.
        dbt_database {str} -- Source database name.
        dbt_manifest_path {str} -- Path to dbt project manifest.json [Primary]
        dbt_path {str} -- Path to dbt project. [Alternative]

    Keyword Arguments:
        schema {str} -- Target schema name. (default: {"public"})
        schemas_excludes -- Alternative to target schema, specify schema exclusions. Only works for manifest parsing. (default: {None})
        mb_use_http {bool} -- Use HTTP to connect to Metabase instead of the default HTTPS. (default: {False})
        mb_verify {str} -- Supply path to certificate or disable verification. (default: {None})
        mb_sync_skip {bool} -- Skip synchronizing Metabase database before export. (default: {False})
        mb_sync_timeout {int} -- Metabase synchronization timeout in seconds. (default: {30})
        includes {list} -- Model names to limit processing to. (default: {None})
        excludes {list} -- Model names to exclude. (default: {None})
        include_tags {bool} -- Append the dbt tags to the end of the table description. (default: {True})
        dbt_docs_url {str} -- URL to your dbt docs hosted catalog. A link will be appended to the model description. Only works for manifest parsing. (default: {None})
    """

    if schemas_excludes is None:
        schemas_excludes = []
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
            schema and not schemas_excludes
        ), "Must target a single schema if using yaml parser, multiple schemas not supported."
    assert bool(schema) != bool(
        schemas_excludes
    ), "Bad arguments. schema and schema_excludes cannot be provide at the same time. One option must be specified."

    # Instantiate Metabase client
    mbc = MetabaseClient(mb_host, mb_user, mb_password, mb_use_http, verify=mb_verify)
    reader: Union[DbtFolderReader, DbtManifestReader]

    # Resolve dbt reader being either YAML or manifest.json based
    if dbt_path:
        reader = DbtFolderReader(os.path.expandvars(dbt_path))
    else:
        reader = DbtManifestReader(os.path.expandvars(dbt_manifest_path))

    if schemas_excludes:
        schemas_excludes = {schema.upper() for schema in schemas_excludes}

    # Process dbt stuff
    models = reader.read_models(
        database=dbt_database,
        schema=schema,
        schemas_excludes=schemas_excludes,
        includes=includes,
        excludes=excludes,
        include_tags=include_tags,
        dbt_docs_url=dbt_docs_url,
    )

    # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
    if not mb_sync_skip:
        if mb_sync_timeout is not None and not mbc.sync_and_wait(
            database, schema, models, mb_sync_timeout
        ):
            logging.critical("Sync timeout reached, models still not compatible")
            return

    # Process Metabase stuff
    mbc.export_models(database, schema, models, reader.catch_aliases)


def main(args: List = None):
    import argparse

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(
        description="Model synchronization from dbt to Metabase."
    )
    parser.add_argument("command", choices=["export"], help="command to execute")
    parser.add_argument(
        "--dbt_path",
        help="Path to dbt project. Cannot be specified with --dbt_manifest_path",
    )
    parser.add_argument(
        "--dbt_manifest_path",
        help="Path to dbt manifest.json typically located in the /target/ directory of the dbt project directory. Cannot be specified with --dbt_path",
    )
    parser.add_argument(
        "--mb_host", metavar="HOST", required=True, help="Metabase hostname"
    )
    parser.add_argument(
        "--mb_user", metavar="USER", required=True, help="Metabase username"
    )
    parser.add_argument(
        "--mb_password", metavar="PASS", required=True, help="Metabase password"
    )
    parser.add_argument(
        "--mb_http",
        dest="mb_http",
        action="store_true",
        help="use HTTP to connect to Metabase instead of HTTPS",
    )
    parser.add_argument(
        "--mb_verify",
        metavar="CERT",
        help="Path to certificate bundle used by Metabase client",
    )
    parser.add_argument(
        "--database",
        metavar="ALIAS",
        required=True,
        help="Target database name as set in Metabase (typically aliased)",
    )
    parser.add_argument(
        "--dbt_database",
        metavar="DB",
        required=True,
        help="Target database name as specified in dbt",
    )
    parser.add_argument(
        "--schema",
        metavar="SCHEMA",
        help="Target schema name. Cannot be specified with --schema_excludes",
    )
    parser.add_argument(
        "--schema_excludes",
        help="Target schemas to exclude. Cannot be specified with --schema. Will sync all schemas not excluded",
    )
    parser.add_argument(
        "--mb_sync_skip",
        dest="mb_sync_skip",
        action="store_true",
        help="Skip synchronizing Metabase database before export",
    )
    parser.add_argument(
        "--mb_sync_timeout",
        metavar="SECS",
        type=int,
        help="Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success",
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
        "--docs",
        metavar="DOCS URL",
        help="Pass in url to dbt docs site. Appends dbt docs url for each model to Metabase table description",
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

    if parsed.command == "export":
        export(
            dbt_path=parsed.dbt_path,
            dbt_manifest_path=parsed.dbt_manifest_path,
            dbt_database=parsed.dbt_database,
            mb_host=parsed.mb_host,
            mb_user=parsed.mb_user,
            mb_password=parsed.mb_password,
            mb_use_http=parsed.mb_use_http,
            mb_verify=parsed.mb_verify,
            database=parsed.database,
            schema=parsed.schema,
            schemas_excludes=parsed.schema_excludes,
            mb_sync_skip=parsed.mb_sync_skip,
            mb_sync_timeout=parsed.mb_sync_timeout,
            includes=parsed.includes,
            excludes=parsed.excludes,
            include_tags=parsed.include_tags,
            dbt_docs_url=parsed.docs,
        )
