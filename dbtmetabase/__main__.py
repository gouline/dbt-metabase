import functools
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Union

import click
import yaml
from rich.logging import RichHandler
from typing_extensions import cast

from .dbt import DbtReader
from .metabase import MetabaseClient

LOG_PATH = Path.home().absolute() / ".dbt-metabase" / "logs" / "dbtmetabase.log"

logger = logging.getLogger(__name__)


def _setup_logger(level: int = logging.INFO):
    """Basic logger configuration for the CLI.

    Args:
        level (int, optional): Logging level. Defaults to logging.INFO.
    """

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        filename=LOG_PATH,
        maxBytes=int(1e6),
        backupCount=3,
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
    )
    file_handler.setLevel(logging.WARNING)

    rich_handler = RichHandler(
        level=level,
        rich_tracebacks=True,
        markup=True,
        show_time=False,
    )

    logging.basicConfig(
        level=level,
        format="%(asctime)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
        handlers=[file_handler, rich_handler],
        force=True,
    )


def _comma_separated_list_callback(
    ctx: click.Context,
    param: click.Option,
    value: Union[str, List[str]],
) -> Optional[List[str]]:
    """Click callback for handling comma-separated lists."""

    if value is None:
        return None

    assert (
        param.type == click.UNPROCESSED or param.type.name == "list"
    ), "comma-separated list options must be of type UNPROCESSED or list"

    if ctx.get_parameter_source(str(param.name)) in (
        click.core.ParameterSource.DEFAULT,
        click.core.ParameterSource.DEFAULT_MAP,
    ) and isinstance(value, list):
        # Lists in defaults (config or option) should be lists
        return value

    elif isinstance(value, str):
        str_value = value
    if isinstance(value, list):
        # When type=list, string value will be a list of chars
        str_value = "".join(value)
    else:
        raise click.BadParameter("must be comma-separated list")

    return str_value.split(",")


@click.group()
@click.version_option(package_name="dbt-metabase")
@click.option(
    "--config-path",
    default="~/.dbt-metabase/config.yml",
    show_default=True,
    type=click.Path(),
    help="Path to config.yml file with default values.",
)
@click.pass_context
def cli(ctx: click.Context, config_path: str):
    group = cast(click.Group, ctx.command)

    config_path_expanded = Path(config_path).expanduser()
    if config_path_expanded.exists():
        with open(config_path_expanded, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f).get("config", {})
            # Propagate root configs to all commands
            ctx.default_map = {command: config for command in group.commands}


def _add_setup(func: Callable) -> Callable:
    """Add common options and create DbtReader and MetabaseClient."""

    @click.option(
        "--dbt-manifest-path",
        envvar="DBT_MANIFEST_PATH",
        show_envvar=True,
        required=True,
        type=click.Path(exists=True, dir_okay=False),
        help="Path to dbt manifest.json file under /target/ in the dbt project directory. Uses dbt manifest parsing (recommended).",
    )
    @click.option(
        "--dbt-database",
        metavar="DATABASE",
        envvar="DBT_DATABASE",
        show_envvar=True,
        required=True,
        type=click.STRING,
        help="Target database name in dbt models.",
    )
    @click.option(
        "--dbt-schema",
        metavar="SCHEMA",
        envvar="DBT_SCHEMA",
        show_envvar=True,
        help="Target dbt schema. Must be passed if using project reader.",
        type=click.STRING,
    )
    @click.option(
        "--dbt-schema-excludes",
        metavar="SCHEMAS",
        envvar="DBT_SCHEMA_EXCLUDES",
        show_envvar=True,
        type=click.UNPROCESSED,
        callback=_comma_separated_list_callback,
        help="Target dbt schemas to exclude. Ignored in project parser.",
    )
    @click.option(
        "--dbt-includes",
        metavar="MODELS",
        envvar="DBT_INCLUDES",
        show_envvar=True,
        type=click.UNPROCESSED,
        callback=_comma_separated_list_callback,
        help="Include specific dbt models names.",
    )
    @click.option(
        "--dbt-excludes",
        metavar="MODELS",
        envvar="DBT_EXCLUDES",
        show_envvar=True,
        type=click.UNPROCESSED,
        callback=_comma_separated_list_callback,
        help="Exclude specific dbt model names.",
    )
    @click.option(
        "--metabase-url",
        metavar="URL",
        envvar="MB_URL",
        show_envvar=True,
        required=True,
        type=click.STRING,
        help="Metabase URL, including protocol and excluding trailing slash.",
    )
    @click.option(
        "--metabase-username",
        metavar="USERNAME",
        envvar="METABASE_USERNAME",
        show_envvar=True,
        type=click.STRING,
        help="Metabase username.",
    )
    @click.option(
        "--metabase-password",
        metavar="PASSWORD",
        envvar="METABASE_PASSWORD",
        show_envvar=True,
        type=click.STRING,
        help="Metabase password.",
    )
    @click.option(
        "--metabase-session-id",
        metavar="TOKEN",
        envvar="METABASE_SESSION_ID",
        show_envvar=True,
        type=click.STRING,
        help="Metabase session ID.",
    )
    @click.option(
        "--metabase-verify/--metabase-verify-skip",
        "metabase_verify",
        envvar="METABASE_VERIFY",
        show_envvar=True,
        default=True,
        help="Verify the TLS certificate at the Metabase end.",
    )
    @click.option(
        "--metabase-cert",
        metavar="CERT",
        envvar="METABASE_CERT",
        show_envvar=True,
        type=click.Path(exists=True, dir_okay=False),
        help="Path to certificate bundle used to connect to Metabase.",
    )
    @click.option(
        "--metabase-timeout",
        metavar="SECS",
        envvar="METABASE_TIMEOUT",
        show_envvar=True,
        type=click.INT,
        default=15,
        show_default=True,
        help="Metabase API HTTP timeout in seconds.",
    )
    @click.option(
        "-v",
        "--verbose",
        is_flag=True,
        help="Enable verbose logging.",
    )
    @functools.wraps(func)
    def wrapper(
        metabase_url: str,
        metabase_username: str,
        metabase_password: str,
        dbt_database: str,
        dbt_manifest_path: str,
        dbt_schema: Optional[str],
        dbt_schema_excludes: Optional[Iterable],
        dbt_includes: Optional[Iterable],
        dbt_excludes: Optional[Iterable],
        metabase_session_id: Optional[str],
        metabase_verify: bool,
        metabase_cert: Optional[str],
        metabase_timeout: int,
        verbose: bool,
        **kwargs,
    ):
        _setup_logger(level=logging.DEBUG if verbose else logging.INFO)

        return func(
            dbt_reader=DbtReader(
                manifest_path=dbt_manifest_path,
                database=dbt_database,
                schema=dbt_schema,
                schema_excludes=dbt_schema_excludes,
                includes=dbt_includes,
                excludes=dbt_excludes,
            ),
            metabase_client=MetabaseClient(
                url=metabase_url,
                username=metabase_username,
                password=metabase_password,
                session_id=metabase_session_id,
                verify=metabase_verify,
                cert=metabase_cert,
                http_timeout=metabase_timeout,
            ),
            **kwargs,
        )

    return wrapper


@cli.command(help="Export dbt models to Metabase.")
@_add_setup
@click.option(
    "--dbt-docs-url",
    metavar="URL",
    envvar="DBT_DOCS_URL",
    show_envvar=True,
    type=click.STRING,
    help="URL for dbt docs to be appended to table descriptions in Metabase.",
)
@click.option(
    "--dbt-include-tags",
    envvar="DBT_INCLUDE_TAGS",
    show_envvar=True,
    is_flag=True,
    help="Append tags to table descriptions in Metabase.",
)
@click.option(
    "--metabase-database",
    metavar="DATABASE",
    envvar="METABASE_DATABASE",
    show_envvar=True,
    required=True,
    type=click.STRING,
    help="Target database name in Metabase.",
)
@click.option(
    "--metabase-sync-timeout",
    metavar="SECS",
    envvar="METABASE_SYNC_TIMEOUT",
    show_envvar=True,
    default=30,
    type=click.INT,
    help="Synchronization timeout in secs. When set, command fails on failed synchronization. Otherwise, command proceeds regardless. Only valid if sync is enabled.",
)
@click.option(
    "--metabase-exclude-sources",
    envvar="METABASE_EXCLUDE_SOURCES",
    show_envvar=True,
    is_flag=True,
    help="Skip exporting sources to Metabase.",
)
def models(
    dbt_docs_url: Optional[str],
    dbt_include_tags: bool,
    metabase_database: str,
    metabase_sync_timeout: int,
    metabase_exclude_sources: bool,
    dbt_reader: DbtReader,
    metabase_client: MetabaseClient,
):
    dbt_models = dbt_reader.read_models(
        include_tags=dbt_include_tags,
        docs_url=dbt_docs_url,
    )
    metabase_client.export_models(
        database=metabase_database,
        models=dbt_models,
        exclude_sources=metabase_exclude_sources,
        sync_timeout=metabase_sync_timeout,
    )


@cli.command(help="Export dbt exposures to Metabase.")
@_add_setup
@click.option(
    "--output-path",
    envvar="OUTPUT_PATH",
    show_envvar=True,
    type=click.Path(exists=True, file_okay=False),
    default=".",
    show_default=True,
    help="Output path for generated exposure YAML files.",
)
@click.option(
    "--output-grouping",
    envvar="OUTPUT_GROUPING",
    show_envvar=True,
    type=click.Choice(["collection", "type"]),
    help="Grouping for output YAML files",
)
@click.option(
    "--metabase-include-personal-collections",
    envvar="METABASE_INCLUDE_PERSONAL_COLLECTIONS",
    show_envvar=True,
    is_flag=True,
    help="Include personal collections when parsing exposures.",
)
@click.option(
    "--metabase-collection-includes",
    metavar="COLLECTIONS",
    envvar="METABASE_COLLECTION_INCLUDES",
    show_envvar=True,
    type=click.UNPROCESSED,
    callback=_comma_separated_list_callback,
    help="Metabase collection names to includes.",
)
@click.option(
    "--metabase-collection-excludes",
    metavar="COLLECTIONS",
    envvar="METABASE_COLLECTION_EXCLUDES",
    show_envvar=True,
    type=click.UNPROCESSED,
    callback=_comma_separated_list_callback,
    help="Metabase collection names to exclude.",
)
def exposures(
    output_path: str,
    output_grouping: Optional[str],
    metabase_include_personal_collections: bool,
    metabase_collection_includes: Optional[Iterable],
    metabase_collection_excludes: Optional[Iterable],
    dbt_reader: DbtReader,
    metabase_client: MetabaseClient,
):
    dbt_models = dbt_reader.read_models()
    metabase_client.extract_exposures(
        models=dbt_models,
        output_path=output_path,
        output_grouping=output_grouping,
        include_personal_collections=metabase_include_personal_collections,
        collection_includes=metabase_collection_includes,
        collection_excludes=metabase_collection_excludes,
    )


if __name__ == "__main__":
    # Executed when running locally via python3 -m dbtmetabase
    cli()  # pylint: disable=no-value-for-parameter
