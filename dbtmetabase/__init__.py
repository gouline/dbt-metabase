import logging
import functools
from typing import Iterable, Optional, Callable

import click
import yaml

from .logger import logging as package_logger
from .models.interface import Metabase, Dbt
from .utils import get_version


__version__ = get_version()


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

        retval = super(MultiArg, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break

        return retval


class CommandController(click.Command):
    """This class inherets from click.Command and supplies custom help text render to
    render our docstrings a little prettier as well as an invoke hook to load from a config file if it exists
    or the argument is passed"""

    def invoke(self, ctx: click.Context):
        config_file = ctx.params.get("config")
        if config_file is not None:
            with open(config_file) as f:
                config_data = yaml.safe_load(f)
                for param, value in ctx.params.items():
                    if value is None and param in config_data:
                        ctx.params[param] = config_data[param]

        return super(CommandController, self).invoke(ctx)

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
        return super(CommandController, self).get_help(ctx)


def shared_opts(func: Callable) -> Callable:
    """Here we define the options shared across subcommands

    Args:
        func (Callable): Wraps a subcommand

    Returns:
        Callable: Subcommand with added options
    """

    @click.option(
        "--dbt_database",
        required=True,
        envvar="DBT_DATABASE",
        show_envvar=True,
        help="Target database name as specified in dbt models to be actioned",
        type=str,
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
        type=str,
    )
    @click.option(
        "--dbt_schema_excludes",
        metavar="SCHEMAS",
        cls=MultiArg,
        type=tuple,
        help="Target schemas to exclude. Ignored in folder parser. Accepts multiple arguments after the flag",
    )
    @click.option(
        "--dbt_includes",
        metavar="MODELS",
        cls=MultiArg,
        type=tuple,
        help="Model names to limit processing to",
    )
    @click.option(
        "--dbt_excludes",
        metavar="MODELS",
        cls=MultiArg,
        type=tuple,
        help="Model names to exclude",
    )
    @click.option(
        "--metabase_database",
        required=True,
        envvar="MB_DATABASE",
        show_envvar=True,
        type=str,
        help="Target database name as set in Metabase (typically aliased)",
    )
    @click.option(
        "--metabase_host",
        metavar="HOST",
        envvar="MB_HOST",
        show_envvar=True,
        required=True,
        type=str,
        help="Metabase hostname",
    )
    @click.option(
        "--metabase_user",
        metavar="USER",
        envvar="MB_USER",
        show_envvar=True,
        required=True,
        type=str,
        help="Metabase username",
    )
    @click.option(
        "--metabase_password",
        metavar="PASS",
        envvar="MB_PASSWORD",
        show_envvar=True,
        required=True,
        type=str,
        help="Metabase password",
    )
    @click.option(
        "--metabase_http/--metabase_https",
        "metabase_use_http",
        default=False,
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
    ...


@cli.command(context_settings=dict(max_content_width=600), cls=CommandController)
@shared_opts
@click.option(
    "--dbt_docs_url",
    metavar="URL",
    type=str,
    help="Pass in URL to dbt docs site. Appends dbt docs URL for each model to Metabase table description (default None)",
)
@click.option(
    "--dbt_include_tags",
    is_flag=True,
    help="Flag to append tags to table descriptions in Metabase (default False)",
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
    metabase_use_http: bool = False,
    metabase_verify: Optional[str] = None,
    metabase_sync: bool = True,
    metabase_sync_timeout: Optional[int] = None,
    dbt_include_tags: bool = True,
    dbt_docs_url: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """Exports model documentation and semantic types from dbt to Metabase.

    \f
    Args:
        metabase_host (str): Metabase hostname
        metabase_user (str): Metabase username
        metabase_password (str): Metabase password
        metabase_database (str): Target database name as set in Metabase (typically aliased)
        dbt_database (str):  Target database name as specified in dbt models to be actioned
        dbt_path (Optional[str], optional): Path to dbt project. If specified with dbt_manifest_path, then the manifest is prioritized. Defaults to None.
        dbt_manifest_path (Optional[str], optional): Path to dbt manifest.json file (typically located in the /target/ directory of the dbt project). Defaults to None.
        dbt_schema (Optional[str], optional): Target schema. Should be passed if using folder parser. Defaults to None.
        dbt_schema_excludes (Optional[Iterable], optional): Target schemas to exclude. Ignored in folder parser. Defaults to None.
        dbt_includes (Optional[Iterable], optional): Model names to limit processing to. Defaults to None.
        dbt_excludes (Optional[Iterable], optional): Model names to exclude. Defaults to None.
        metabase_use_http (bool, optional): Use HTTP to connect to Metabase. Defaults to False.
        metabase_verify (Optional[str], optional): Path to custom certificate bundle to be used by Metabase client. Defaults to None.
        metabase_sync (bool, optional): Attempt to synchronize Metabase schema with local models. Defaults to True.
        metabase_sync_timeout (Optional[int], optional): Synchronization timeout (in secs). If set, we will fail hard on synchronization failure; if not set, we will proceed after attempting sync regardless of success. Only valid if sync is enabled. Defaults to None.
        dbt_include_tags (bool, optional): Flag to append tags to table descriptions in Metabase. Defaults to True.
        dbt_docs_url (Optional[str], optional): Pass in URL to dbt docs site. Appends dbt docs URL for each model to Metabase table description. Defaults to None.
        verbose (bool, optional): Flag which signals verbose output. Defaults to False.
    """

    if verbose:
        package_logger.LOGGING_LEVEL = logging.DEBUG

    # Load models
    dbt_interface = Dbt(
        path=dbt_path,
        manifest_path=dbt_manifest_path,
        database=dbt_database,
        schema=dbt_schema,
        schema_excludes=dbt_schema_excludes,
        includes=dbt_includes,
        excludes=dbt_excludes,
    )
    dbt_models, alias_mapping = dbt_interface.parser.read_models(
        dbt_config=dbt_interface,
        include_tags=dbt_include_tags,
        docs_url=dbt_docs_url,
    )

    # Instantiate Metabase interface
    metabase_interface = Metabase(
        host=metabase_host,
        user=metabase_user,
        password=metabase_password,
        use_http=metabase_use_http,
        verify=metabase_verify,
        database=metabase_database,
        sync=metabase_sync,
        sync_timeout=metabase_sync_timeout,
    )

    # Process Metabase stuff
    metabase_interface.prepare_metabase_client(dbt_models)
    metabase_interface.client.export_models(
        database=metabase_interface.database,
        models=dbt_models,
        aliases=alias_mapping,
    )


@cli.command(context_settings=dict(max_content_width=600), cls=CommandController)
@shared_opts
@click.option(
    "--output_path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True),
    help="Output path for generated exposure yaml. Defaults to local dir.",
    default=".",
)
@click.option(
    "--output_name",
    type=str,
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

    \f
    Args:
        metabase_host (str): Metabase hostname
        metabase_user (str): Metabase username
        metabase_password (str): Metabase password
        metabase_database (str): Target database name as set in Metabase (typically aliased)
        dbt_database (str):  Target database name as specified in dbt models to be actioned
        dbt_path (Optional[str], optional): Path to dbt project. If specified with dbt_manifest_path, then the manifest is prioritized. Defaults to None.
        dbt_manifest_path (Optional[str], optional): Path to dbt manifest.json file (typically located in the /target/ directory of the dbt project). Defaults to None.
        dbt_schema (Optional[str], optional): Target schema. Should be passed if using folder parser. Defaults to None.
        dbt_schema_excludes (Optional[Iterable], optional): Target schemas to exclude. Ignored in folder parser. Defaults to None.
        dbt_includes (Optional[Iterable], optional): Model names to limit processing to. Defaults to None.
        dbt_excludes (Optional[Iterable], optional): Model names to exclude. Defaults to None.
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

    if verbose:
        package_logger.LOGGING_LEVEL = logging.DEBUG

    # Load models
    dbt_interface = Dbt(
        path=dbt_path,
        manifest_path=dbt_manifest_path,
        database=dbt_database,
        schema=dbt_schema,
        schema_excludes=dbt_schema_excludes,
        includes=dbt_includes,
        excludes=dbt_excludes,
    )

    dbt_models = dbt_interface.parser.read_models(dbt_config=dbt_interface)[0]

    # Instantiate Metabase interface
    metabase_interface = Metabase(
        host=metabase_host,
        user=metabase_user,
        password=metabase_password,
        use_http=metabase_use_http,
        verify=metabase_verify,
        database=metabase_database,
        sync=metabase_sync,
        sync_timeout=metabase_sync_timeout,
    )

    # Process Metabase stuff
    metabase_interface.prepare_metabase_client(dbt_models)
    metabase_interface.client.extract_exposures(
        models=dbt_models,
        output_path=output_path,
        output_name=output_name,
        include_personal_collections=include_personal_collections,
        collection_excludes=collection_excludes,
    )


def main():
    cli()
