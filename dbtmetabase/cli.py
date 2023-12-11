import functools
import logging
from pathlib import Path
from typing import Callable, Iterable, Optional

import click
import yaml
from typing_extensions import cast

from .logger import logging as package_logger
from .models.interface import DbtInterface, MetabaseInterface


# Source: https://stackoverflow.com/a/48394004/818393
class OptionEatAll(click.Option):
    def __init__(self, *args, **kwargs):
        self.save_other_options = kwargs.pop("save_other_options", True)
        nargs = kwargs.pop("nargs", -1)
        assert nargs == -1, "nargs, if set, must be -1 not {}".format(nargs)
        super().__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

    def add_to_parser(self, parser, ctx):
        def parser_process(value, state):
            # method to hook to the parser.process
            done = False
            value = [value]
            if self.save_other_options:
                # grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value.append(state.rargs.pop(0))
            else:
                # grab everything remaining
                value += state.rargs
                state.rargs[:] = []
            value = tuple(value)

            # call the actual process
            self._previous_parser_process(value, state)

        super().add_to_parser(parser, ctx)
        for name in self.opts:
            # pylint: disable=protected-access
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return


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

            ## TODO: there must be a better way, but it works for now
            ctx.default_map = {command: config for command in group.commands}


def common_options(func: Callable) -> Callable:
    """Common click options between commands."""

    @click.option(
        "--dbt-database",
        envvar="DBT_DATABASE",
        show_envvar=True,
        required=True,
        type=click.STRING,
        help="Target database name in dbt models.",
    )
    @click.option(
        "--dbt-manifest-path",
        envvar="DBT_MANIFEST_PATH",
        show_envvar=True,
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
        help="Path to dbt manifest.json file under /target/ in the dbt project directory. Uses dbt manifest parsing (recommended).",
    )
    @click.option(
        "--dbt-project-path",
        envvar="DBT_PROJECT_PATH",
        show_envvar=True,
        type=click.Path(exists=True, file_okay=False, dir_okay=True),
        help="Path to dbt project directory containing models. Uses dbt project parsing (not recommended).",
    )
    @click.option(
        "--dbt-schema",
        help="Target dbt schema. Must be passed if using project parser.",
        type=click.STRING,
    )
    @click.option(
        "--dbt-schema-excludes",
        metavar="SCHEMAS",
        type=list,
        cls=OptionEatAll,
        help="Target dbt schemas to exclude. Ignored in project parser.",
    )
    @click.option(
        "--dbt-includes",
        metavar="MODELS",
        type=list,
        cls=OptionEatAll,
        help="Include specific dbt models names.",
    )
    @click.option(
        "--dbt-excludes",
        metavar="MODELS",
        type=list,
        cls=OptionEatAll,
        help="Exclude specific dbt model names.",
    )
    @click.option(
        "--metabase-database",
        envvar="MB_DATABASE",
        show_envvar=True,
        required=True,
        type=click.STRING,
        help="Target database name in Metabase.",
    )
    @click.option(
        "--metabase-host",
        metavar="HOST",
        envvar="MB_HOST",
        show_envvar=True,
        required=True,
        type=click.STRING,
        help="Metabase hostname, excluding protocol.",
    )
    @click.option(
        "--metabase-user",
        metavar="USER",
        envvar="MB_USER",
        show_envvar=True,
        type=click.STRING,
        help="Metabase username.",
    )
    @click.option(
        "--metabase-password",
        metavar="PASS",
        envvar="MB_PASSWORD",
        show_envvar=True,
        type=click.STRING,
        help="Metabase password.",
    )
    @click.option(
        "--metabase-session-id",
        metavar="TOKEN",
        envvar="MB_SESSION_ID",
        show_envvar=True,
        type=click.STRING,
        help="Metabase session ID.",
    )
    @click.option(
        "--metabase-http/--metabase-https",
        "metabase_use_http",
        default=False,
        help="Force HTTP instead of HTTPS to connect to Metabase.",
    )
    @click.option(
        "--metabase-verify",
        metavar="CERT",
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
        help="Path to certificate bundle used to connect to Metabase.",
    )
    @click.option(
        "--metabase-sync/--metabase-sync-skip",
        "metabase_sync",
        default=True,
        show_default=True,
        help="Attempt to synchronize Metabase schema with local models.",
    )
    @click.option(
        "--metabase-sync-timeout",
        metavar="SECS",
        type=click.INT,
        help="Synchronization timeout in secs. When set, command fails on failed synchronization. Otherwise, command proceeds regardless. Only valid if sync is enabled.",
    )
    @click.option(
        "--metabase-http-timeout",
        type=int,
        default=15,
        show_default=True,
        envvar="MB_HTTP_TIMEOUT",
        show_envvar=True,
        help="Set the value for single requests timeout.",
    )
    @click.option(
        "-v",
        "--verbose",
        is_flag=True,
        help="Enable verbose logging.",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@cli.command(help="Export dbt models to Metabase.")
@common_options
@click.option(
    "--dbt-docs-url",
    metavar="URL",
    type=click.STRING,
    help="URL for dbt docs to be appended to table descriptions in Metabase.",
)
@click.option(
    "--dbt-include-tags",
    is_flag=True,
    help="Append tags to table descriptions in Metabase.",
)
@click.option(
    "--metabase-exclude-sources",
    is_flag=True,
    help="Skip exporting sources to Metabase.",
)
def models(
    metabase_host: str,
    metabase_user: str,
    metabase_password: str,
    metabase_database: str,
    dbt_database: str,
    dbt_path: Optional[str],
    dbt_manifest_path: Optional[str],
    dbt_schema: Optional[str],
    dbt_schema_excludes: Optional[Iterable],
    dbt_includes: Optional[Iterable],
    dbt_excludes: Optional[Iterable],
    metabase_session_id: Optional[str],
    metabase_use_http: bool,
    metabase_verify: Optional[str],
    metabase_sync: bool,
    metabase_sync_timeout: Optional[int],
    metabase_exclude_sources: bool,
    metabase_http_timeout: int,
    dbt_include_tags: bool,
    dbt_docs_url: Optional[str],
    verbose: bool,
):
    # Set global logging level if verbose
    if verbose:
        package_logger.LOGGING_LEVEL = logging.DEBUG

    # Instantiate dbt interface
    dbt = DbtInterface(
        path=dbt_path,
        manifest_path=dbt_manifest_path,
        database=dbt_database,
        schema=dbt_schema,
        schema_excludes=dbt_schema_excludes,
        includes=dbt_includes,
        excludes=dbt_excludes,
    )

    # Load models
    dbt_models, aliases = dbt.read_models(
        include_tags=dbt_include_tags,
        docs_url=dbt_docs_url,
    )

    # Instantiate Metabase interface
    metabase = MetabaseInterface(
        host=metabase_host,
        user=metabase_user,
        password=metabase_password,
        session_id=metabase_session_id,
        use_http=metabase_use_http,
        verify=metabase_verify,
        database=metabase_database,
        sync=metabase_sync,
        sync_timeout=metabase_sync_timeout,
        exclude_sources=metabase_exclude_sources,
        http_timeout=metabase_http_timeout,
    )

    # Load client
    metabase.prepare_metabase_client(dbt_models)

    # Execute model export
    metabase.client.export_models(
        database=metabase.database,
        models=dbt_models,
        aliases=aliases,
    )


@cli.command(help="Export dbt exposures to Metabase.")
@common_options
@click.option(
    "--output-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True),
    default=".",
    show_default=True,
    help="Output path for generated exposure YAML.",
)
@click.option(
    "--output-name",
    type=click.STRING,
    default="metabase_exposures.yml",
    show_default=True,
    help="File name for generated exposure YAML.",
)
@click.option(
    "--include-personal-collections",
    is_flag=True,
    help="Include personal collections when parsing exposures.",
)
@click.option(
    "--collection-excludes",
    cls=OptionEatAll,
    type=list,
    help="Metabase collection names to exclude.",
)
def exposures(
    metabase_host: str,
    metabase_user: str,
    metabase_password: str,
    metabase_database: str,
    dbt_database: str,
    dbt_path: Optional[str],
    dbt_manifest_path: Optional[str],
    dbt_schema: Optional[str],
    dbt_schema_excludes: Optional[Iterable],
    dbt_includes: Optional[Iterable],
    dbt_excludes: Optional[Iterable],
    metabase_session_id: Optional[str],
    metabase_use_http: bool,
    metabase_verify: Optional[str],
    metabase_sync: bool,
    metabase_sync_timeout: Optional[int],
    metabase_http_timeout: int,
    output_path: str,
    output_name: str,
    include_personal_collections: bool,
    collection_excludes: Optional[Iterable],
    verbose: bool,
):
    if verbose:
        package_logger.LOGGING_LEVEL = logging.DEBUG

    # Instantiate dbt interface
    dbt = DbtInterface(
        path=dbt_path,
        manifest_path=dbt_manifest_path,
        database=dbt_database,
        schema=dbt_schema,
        schema_excludes=dbt_schema_excludes,
        includes=dbt_includes,
        excludes=dbt_excludes,
    )

    # Load models
    dbt_models, _ = dbt.read_models()

    # Instantiate Metabase interface
    metabase = MetabaseInterface(
        host=metabase_host,
        user=metabase_user,
        password=metabase_password,
        session_id=metabase_session_id,
        use_http=metabase_use_http,
        verify=metabase_verify,
        database=metabase_database,
        sync=metabase_sync,
        sync_timeout=metabase_sync_timeout,
        http_timeout=metabase_http_timeout,
    )

    # Load client
    metabase.prepare_metabase_client(dbt_models)

    # Execute exposure extraction
    metabase.client.extract_exposures(
        models=dbt_models,
        output_path=output_path,
        output_name=output_name,
        include_personal_collections=include_personal_collections,
        collection_excludes=collection_excludes,
    )
