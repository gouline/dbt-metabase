import logging
import functools
from pathlib import Path
from typing import Iterable, Optional, Callable, Any
import os

import click
import yaml

from .logger import logging as package_logger
from .models.interface import MetabaseInterface, DbtInterface
from .utils import get_version, load_config

__all__ = ["MetabaseInterface", "DbtInterface"]
__version__ = get_version()

CONFIG = load_config()
ENV_VARS = [
    "DBT_DATABASE",
    "DBT_PATH",
    "DBT_MANIFEST_PATH",
    "MB_USER",
    "MB_PASSWORD",
    "MB_HOST",
    "MB_DATABASE",
    "MB_SESSION_TOKEN",
]


class MultiArg(click.Option):
    """This class lets us pass multiple arguments after an options, equivalent to nargs=*"""

    def __init__(self, *args, **kwargs):
        nargs = kwargs.pop("nargs", -1)
        assert nargs == -1, "nargs, if set, must be -1 not {}".format(nargs)
        super(MultiArg, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

    def add_to_parser(self, parser, ctx):
        def parser_process(value, state):
            # Method to hook to the parser.process
            done = False
            value = [value]
            # Grab everything up to the next option
            while state.rargs and not done:
                for prefix in self._eat_all_parser.prefixes:
                    if state.rargs[0].startswith(prefix):
                        done = True
                if not done:
                    value.append(state.rargs.pop(0))
            value = tuple(value)

            # Call the actual process
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


class ListParam(click.Tuple):
    def __init__(self) -> None:
        self.type = click.STRING
        super().__init__([])

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        len_value = len(value)
        types = [self.type] * len_value

        return list(ty(x, param, ctx) for ty, x in zip(types, value))


class OptionAcceptableFromConfig(click.Option):
    """This class override should be used on arguments that are marked `required=True` in order to give them
    more resilence to raising an error when the option exists in the users config.

    This also overrides default values for boolean CLI flags (e.g. --use_metabase_http/--use_metabase_https) in options when
    no CLI flag is passed, but a value is provided in the config file (e.g. metabase_use_http: True)."""

    def process_value(self, ctx: click.Context, value: Any) -> Any:
        if value is not None:
            value = self.type_cast_value(ctx, value)

        assert self.name, "none config option"

        if (
            isinstance(self.type, click.types.BoolParamType)
            and ctx.get_parameter_source(self.name)
            == click.core.ParameterSource.DEFAULT
            and self.name in CONFIG
        ):
            value = CONFIG[self.name]

        if self.required and self.value_is_missing(value):
            if self.name not in CONFIG:
                raise click.core.MissingParameter(ctx=ctx, param=self)
            value = CONFIG[self.name]

        if self.callback is not None:
            value = self.callback(ctx, self, value)

        return value


class CommandController(click.Command):
    """This class inherets from click.Command and supplies custom help text renderer to
    render our docstrings a little prettier as well as a hook in the invoke to load from a config file if it exists."""

    def invoke(self, ctx: click.Context):

        if CONFIG:
            for param, value in ctx.params.items():
                if value is None and param in CONFIG:
                    ctx.params[param] = CONFIG[param]

        return super().invoke(ctx)

    def get_help(self, ctx: click.Context):
        orig_wrap_test = click.formatting.wrap_text

        def wrap_text(
            text: str,
            width: int = 78,
            initial_indent: str = "",
            subsequent_indent: str = "",
            preserve_paragraphs: bool = False,
        ):
            del preserve_paragraphs
            return orig_wrap_test(
                text.replace("\n", "\n\n"),
                width,
                initial_indent=initial_indent,
                subsequent_indent=subsequent_indent,
                preserve_paragraphs=True,
            ).replace("\n\n", "\n")

        click.formatting.wrap_text = wrap_text
        return super().get_help(ctx)


def shared_opts(func: Callable) -> Callable:
    """Here we define the options shared across subcommands

    Args:
        func (Callable): Wraps a subcommand

    Returns:
        Callable: Subcommand with added options
    """

    @click.option(
        "--dbt_database",
        envvar="DBT_DATABASE",
        show_envvar=True,
        required=True,
        cls=OptionAcceptableFromConfig,
        help="Target database name as specified in dbt models to be actioned",
        type=click.STRING,
    )
    @click.option(
        "--dbt_path",
        envvar="DBT_PATH",
        show_envvar=True,
        help="Path to dbt project. If specified with --dbt_manifest_path, then the manifest is prioritized",
        type=click.Path(exists=True, file_okay=False, dir_okay=True),
    )
    @click.option(
        "--dbt_manifest_path",
        envvar="DBT_MANIFEST_PATH",
        show_envvar=True,
        help="Path to dbt manifest.json file (typically located in the /target/ directory of the dbt project)",
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
    )
    @click.option(
        "--dbt_schema",
        help="Target schema. Should be passed if using folder parser",
        type=click.STRING,
    )
    @click.option(
        "--dbt_schema_excludes",
        metavar="SCHEMAS",
        cls=MultiArg,
        type=list,
        help="Target schemas to exclude. Ignored in folder parser. Accepts multiple arguments after the flag",
    )
    @click.option(
        "--dbt_includes",
        metavar="MODELS",
        cls=MultiArg,
        type=list,
        help="Model names to limit processing to",
    )
    @click.option(
        "--dbt_excludes",
        metavar="MODELS",
        cls=MultiArg,
        type=list,
        help="Model names to exclude",
    )
    @click.option(
        "--metabase_database",
        envvar="MB_DATABASE",
        show_envvar=True,
        required=True,
        cls=OptionAcceptableFromConfig,
        type=click.STRING,
        help="Target database name as set in Metabase (typically aliased)",
    )
    @click.option(
        "--metabase_host",
        metavar="HOST",
        envvar="MB_HOST",
        show_envvar=True,
        required=True,
        cls=OptionAcceptableFromConfig,
        type=click.STRING,
        help="Metabase hostname",
    )
    @click.option(
        "--metabase_user",
        metavar="USER",
        envvar="MB_USER",
        show_envvar=True,
        required=True,
        cls=OptionAcceptableFromConfig,
        type=click.STRING,
        help="Metabase username",
    )
    @click.option(
        "--metabase_password",
        metavar="PASS",
        envvar="MB_PASSWORD",
        show_envvar=True,
        required=True,
        cls=OptionAcceptableFromConfig,
        type=click.STRING,
        help="Metabase password",
    )
    @click.option(
        "--metabase_session_id",
        metavar="TOKEN",
        envvar="MB_SESSION_ID",
        show_envvar=True,
        default=None,
        cls=OptionAcceptableFromConfig,
        type=click.STRING,
        help="Metabase session ID",
    )
    @click.option(
        "--metabase_http/--metabase_https",
        "metabase_use_http",
        default=False,
        cls=OptionAcceptableFromConfig,
        help="use HTTP or HTTPS to connect to Metabase. Default HTTPS",
    )
    @click.option(
        "--metabase_verify",
        metavar="CERT",
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
        help="Path to certificate bundle used by Metabase client",
    )
    @click.option(
        "--metabase_sync/--metabase_sync_skip",
        "metabase_sync",
        cls=OptionAcceptableFromConfig,
        default=True,
        help="Attempt to synchronize Metabase schema with local models. Default sync",
    )
    @click.option(
        "--metabase_sync_timeout",
        metavar="SECS",
        type=int,
        help="Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success. Only valid if sync is enabled",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group()
@click.version_option(__version__)
def cli():
    """Model synchronization from dbt to Metabase."""


@click.command(cls=CommandController)
def check_config():
    package_logger.logger().info(
        "Looking for configuration file in ~/.dbt-metabase :magnifying_glass_tilted_right:"
    )
    package_logger.logger().info(
        "...bootstrapping environmental variables :racing_car:"
    )
    any_found = False
    for env in ENV_VARS:
        if env in os.environ:
            package_logger.logger().info("Injecting valid env var: %s", env)
            param = env.lower().replace("mb_", "metabase_")
            CONFIG[param] = os.environ[env]
            any_found = True
    if not any_found:
        package_logger.logger().info("NO valid env vars found")

    if not CONFIG:
        package_logger.logger().info(
            "No configuration file or env vars found, run the `config` command to interactively generate one.",
        )
    else:
        package_logger.logger().info("Config rendered!")
        package_logger.logger().info(
            {k: (v if "pass" not in k else "****") for k, v in CONFIG.items()}
        )


@click.command(cls=CommandController)
def check_env():
    package_logger.logger().info("All valid env vars: %s", ENV_VARS)
    any_found = False
    for env in ENV_VARS:
        if env in os.environ:
            val = os.environ[env] if "pass" not in env.lower() else "****"
            package_logger.logger().info("Found value for %s --> %s", env, val)
            any_found = True
    if not any_found:
        package_logger.logger().info("None of the env vars found in environment")


@cli.command(cls=CommandController)
@click.option(
    "--inspect",
    is_flag=True,
    help="Introspect your dbt-metabase config.",
)
@click.option(
    "--resolve",
    is_flag=True,
    help="Introspect your dbt-metabase config automatically injecting env vars into the configuration overwriting config.yml defaults. Use this flag if you are using env vars and want to see the resolved runtime output.",
)
@click.option(
    "--env",
    is_flag=True,
    help="List all valid env vars for dbt-metabase. Env vars are evaluated before the config.yml and thus take precendence if used.",
)
@click.pass_context
def config(ctx, inspect: bool = False, resolve: bool = False, env: bool = False):
    """Interactively generate a config or validate an existing config.yml

    A config allows you to omit arguments which will be substituted with config defaults. This simplifies
    the execution of dbt-metabase to simply calling a command in most cases. Ex `dbt-metabase models`

    Execute the `config` command with no flags to enter an interactive session to create or update a config.yml.

    The config.yml should be located in ~/.dbt-metabase/
        Valid keys include any parameter seen in a dbt-metabase --help function
        Example: `dbt-metabase models --help`
    """
    if inspect:
        package_logger.logger().info(
            {k: (v if "pass" not in k else "****") for k, v in CONFIG.items()}
        )
    if resolve:
        ctx.invoke(check_config)
    if env:
        ctx.invoke(check_env)
    if inspect or resolve or env:
        ctx.exit()
    click.confirm(
        "Confirming you want to build or modify a dbt-metabase config file?", abort=True
    )
    package_logger.logger().info(
        "Preparing interactive configuration :rocket: (note defaults denoted by [...] are pulled from your existing config if found)"
    )
    config_path = Path.home() / ".dbt-metabase"
    config_path.mkdir(parents=True, exist_ok=True)
    config_file = {}
    conf_name = None
    if (config_path / "config.yml").exists():
        with open(config_path / "config.yml", "r", encoding="utf-8") as f:
            config_file = yaml.safe_load(f).get("config", {})
            conf_name = "config.yml"
    elif (config_path / "config.yaml").exists():
        with open(config_path / "config.yaml", "r", encoding="utf-8") as f:
            config_file = yaml.safe_load(f).get("config", {})
            conf_name = "config.yaml"
    else:
        # Default config name
        conf_name = "config.yml"
    if not config_file:
        package_logger.logger().info("Building config file! :hammer:")
    else:
        package_logger.logger().info("Modifying config file! :wrench:")
    config_file["dbt_database"] = click.prompt(
        "Please enter the name of your dbt Database",
        default=config_file.get("dbt_database"),
        show_default=True,
        type=click.STRING,
    )
    config_file["dbt_manifest_path"] = click.prompt(
        "Please enter the path to your dbt manifest.json \ntypically located in the /target directory of the dbt project",
        default=config_file.get("dbt_manifest_path"),
        show_default=True,
        type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    )
    if click.confirm(
        "Would you like to set some default schemas to exclude when no flags are provided?"
    ):
        config_file["dbt_schema_excludes"] = click.prompt(
            "Target schemas to exclude separated by commas",
            default=config_file.get("dbt_schema_excludes"),
            show_default=True,
            value_proc=lambda s: list(map(str.strip, s.split(","))),
            type=click.UNPROCESSED,
        )
    else:
        config_file.pop("dbt_schema_excludes", None)
    if click.confirm(
        "Would you like to set some default dbt models to exclude when no flags are provided?"
    ):
        config_file["dbt_excludes"] = click.prompt(
            "dbt model names to exclude separated by commas",
            default=config_file.get("dbt_excludes"),
            show_default=True,
            value_proc=lambda s: list(map(str.strip, s.split(","))),
            type=ListParam(),
        )
    else:
        config_file.pop("dbt_excludes", None)
    config_file["metabase_database"] = click.prompt(
        "Target database name as set in Metabase (typically aliased)",
        default=config_file.get("metabase_database"),
        show_default=True,
        type=click.STRING,
    )
    config_file["metabase_host"] = click.prompt(
        "Metabase hostname, this is the URL without the protocol (HTTP/S)",
        default=config_file.get("metabase_host"),
        show_default=True,
        type=click.STRING,
    )
    config_file["metabase_user"] = click.prompt(
        "Metabase username",
        default=config_file.get("metabase_user"),
        show_default=True,
        type=click.STRING,
    )
    config_file["metabase_password"] = click.prompt(
        "Metabase password [hidden]",
        default=config_file.get("metabase_password"),
        hide_input=True,
        show_default=False,
        type=click.STRING,
    )
    config_file["metabase_use_http"] = click.confirm(
        "Use HTTP instead of HTTPS to connect to Metabase, unless testing locally we should be saying no here",
        default=config_file.get("metabase_use_http", False),
        show_default=True,
    )
    if click.confirm("Would you like to set a custom certificate bundle to use?"):
        config_file["metabase_verify"] = click.prompt(
            "Path to certificate bundle used by Metabase client",
            default=config_file.get("metabase_verify"),
            show_default=True,
            type=click.Path(
                exists=True, file_okay=True, dir_okay=False, resolve_path=True
            ),
        )
    else:
        config_file.pop("metabase_verify", None)
    config_file["metabase_sync"] = click.confirm(
        "Would you like to allow Metabase schema syncs by default? Best to say yes as there is little downside",
        default=config_file.get("metabase_sync", True),
        show_default=True,
    )
    if config_file["metabase_sync"]:
        config_file["metabase_sync_timeout"] = click.prompt(
            "Synchronization timeout in seconds. If set, we will fail hard on synchronization failure; \nIf set to 0, we will proceed after attempting sync regardless of success",
            default=config_file.get("metabase_sync_timeout", 0),
            show_default=True,
            value_proc=lambda i: None if int(i) <= 0 else int(i),
            type=click.INT,
        )
    else:
        config_file.pop("metabase_sync_timeout", None)
    output_config = {"config": config_file}
    package_logger.logger().info(
        "Config constructed -- writing config to ~/.dbt-metabase"
    )
    package_logger.logger().info(
        {k: (v if "pass" not in k else "****") for k, v in config_file.items()}
    )
    with open(config_path / conf_name, "w", encoding="utf-8") as outfile:
        yaml.dump(
            output_config,
            outfile,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )


@cli.command(cls=CommandController)
@shared_opts
@click.option(
    "--dbt_docs_url",
    metavar="URL",
    type=click.STRING,
    help="Pass in URL to dbt docs site. Appends dbt docs URL for each model to Metabase table description (default None)",
)
@click.option(
    "--dbt_include_tags",
    is_flag=True,
    help="Flag to append tags to table descriptions in Metabase (default False)",
)
@click.option(
    "--metabase_exclude_sources",
    is_flag=True,
    help="Flag to skip exporting sources to Metabase (default False)",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Flag which signals verbose output",
)
def models(
    metabase_host: str,
    metabase_user: str,
    metabase_password: str,
    metabase_database: str,
    dbt_database: str,
    dbt_path: Optional[str] = None,
    dbt_manifest_path: Optional[str] = None,
    dbt_schema: Optional[str] = None,
    dbt_schema_excludes: Optional[Iterable] = None,
    dbt_includes: Optional[Iterable] = None,
    dbt_excludes: Optional[Iterable] = None,
    metabase_session_id: Optional[str] = None,
    metabase_use_http: bool = False,
    metabase_verify: Optional[str] = None,
    metabase_sync: bool = True,
    metabase_sync_timeout: Optional[int] = None,
    metabase_exclude_sources: bool = False,
    dbt_include_tags: bool = True,
    dbt_docs_url: Optional[str] = None,
    verbose: bool = False,
):
    """Exports model documentation and semantic types from dbt to Metabase.

    Args:
        metabase_host (str): Metabase hostname.
        metabase_user (str): Metabase username.
        metabase_password (str): Metabase password.
        metabase_database (str): Target database name as set in Metabase (typically aliased).
        metabase_session_id (Optional[str], optional): Session ID. Defaults to None.
        dbt_database (str):  Target database name as specified in dbt models to be actioned.
        dbt_path (Optional[str], optional): Path to dbt project. If specified with dbt_manifest_path, then the manifest is prioritized. Defaults to None.
        dbt_manifest_path (Optional[str], optional): Path to dbt manifest.json file (typically located in the /target/ directory of the dbt project). Defaults to None.
        dbt_schema (Optional[str], optional): Target schema. Should be passed if using folder parser. Defaults to None.
        dbt_schema_excludes (Optional[Iterable], optional): Target schemas to exclude. Ignored in folder parser. Defaults to None.
        dbt_includes (Optional[Iterable], optional): Model names to limit processing to. Defaults to None.
        dbt_excludes (Optional[Iterable], optional): Model names to exclude. Defaults to None.
        metabase_session_id (Optional[str], optional): Metabase session id. Defaults to none.
        metabase_use_http (bool, optional): Use HTTP to connect to Metabase. Defaults to False.
        metabase_verify (Optional[str], optional): Path to custom certificate bundle to be used by Metabase client. Defaults to None.
        metabase_sync (bool, optional): Attempt to synchronize Metabase schema with local models. Defaults to True.
        metabase_sync_timeout (Optional[int], optional): Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success. Only valid if sync is enabled. Defaults to None.
        metabase_exclude_sources (bool, optional): Flag to skip exporting sources to Metabase. Defaults to False.
        dbt_include_tags (bool, optional): Flag to append tags to table descriptions in Metabase. Defaults to True.
        dbt_docs_url (Optional[str], optional): Pass in URL to dbt docs site. Appends dbt docs URL for each model to Metabase table description. Defaults to None.
        verbose (bool, optional): Flag which signals verbose output. Defaults to False.
    """

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
    )

    # Load client
    metabase.prepare_metabase_client(dbt_models)

    # Execute model export
    metabase.client.export_models(
        database=metabase.database,
        models=dbt_models,
        aliases=aliases,
    )


@cli.command(cls=CommandController)
@shared_opts
@click.option(
    "--output_path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True),
    help="Output path for generated exposure yaml. Defaults to local dir.",
    default=".",
)
@click.option(
    "--output_name",
    type=click.STRING,
    help="Output name for generated exposure yaml. Defaults to metabase_exposures.yml",
)
@click.option(
    "--include_personal_collections",
    is_flag=True,
    help="Flag to include Personal Collections during exposure parsing",
)
@click.option(
    "--collection_excludes",
    cls=MultiArg,
    type=list,
    help="Metabase collection names to exclude",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Flag which signals verbose output",
)
def exposures(
    metabase_host: str,
    metabase_user: str,
    metabase_password: str,
    metabase_database: str,
    dbt_database: str,
    dbt_path: Optional[str] = None,
    dbt_manifest_path: Optional[str] = None,
    dbt_schema: Optional[str] = None,
    dbt_schema_excludes: Optional[Iterable] = None,
    dbt_includes: Optional[Iterable] = None,
    dbt_excludes: Optional[Iterable] = None,
    metabase_session_id: Optional[str] = None,
    metabase_use_http: bool = False,
    metabase_verify: Optional[str] = None,
    metabase_sync: bool = True,
    metabase_sync_timeout: Optional[int] = None,
    output_path: str = ".",
    output_name: str = "metabase_exposures.yml",
    include_personal_collections: bool = False,
    collection_excludes: Optional[Iterable] = None,
    verbose: bool = False,
) -> None:
    """Extracts and imports exposures from Metabase to dbt.

    Args:
        metabase_host (str): Metabase hostname.
        metabase_user (str): Metabase username.
        metabase_password (str): Metabase password.
        metabase_database (str): Target database name as set in Metabase (typically aliased).
        dbt_database (str): Target database name as specified in dbt models to be actioned.
        dbt_path (Optional[str], optional): Path to dbt project. If specified with dbt_manifest_path, then the manifest is prioritized. Defaults to None.
        dbt_manifest_path (Optional[str], optional): Path to dbt manifest.json file (typically located in the /target/ directory of the dbt project). Defaults to None.
        dbt_schema (Optional[str], optional): Target schema. Should be passed if using folder parser. Defaults to None.
        dbt_schema_excludes (Optional[Iterable], optional): Target schemas to exclude. Ignored in folder parser. Defaults to None.
        dbt_includes (Optional[Iterable], optional): Model names to limit processing to. Defaults to None.
        dbt_excludes (Optional[Iterable], optional): Model names to exclude. Defaults to None.
        metabase_session_id (Optional[str], optional): Metabase session id. Defaults to none.
        metabase_use_http (bool, optional): Use HTTP to connect to Metabase. Defaults to False.
        metabase_verify (Optional[str], optional): Path to custom certificate bundle to be used by Metabase client. Defaults to None.
        metabase_sync (bool, optional): Attempt to synchronize Metabase schema with local models. Defaults to True.
        metabase_sync_timeout (Optional[int], optional): Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success. Only valid if sync is enabled. Defaults to None.
        output_path (str): Output path for generated exposure yaml. Defaults to "." local dir.
        output_name (str): Output name for generated exposure yaml. Defaults to metabase_exposures.yml.
        include_personal_collections (bool, optional): Flag to include Personal Collections during exposure parsing. Defaults to False.
        collection_excludes (Iterable, optional): Collection names to exclude. Defaults to None.
        verbose (bool, optional): Flag which signals verbose output. Defaults to False.
    """

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


def main():
    # Valid kwarg
    cli(max_content_width=600)  # pylint: disable=unexpected-keyword-arg
