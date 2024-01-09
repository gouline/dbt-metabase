from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableSequence, Optional, Union

import click
from rich.logging import RichHandler


class _NullValue(str):
    """Explicitly null field value."""

    def __eq__(self, other: object) -> bool:
        return other is None


NullValue = _NullValue()


def setup_logging(level: int, path: Optional[Path] = None):
    """Basic logger configuration for the CLI.

    Args:
        level (int): Logging level. Defaults to logging.INFO.
        path (Path): Path to file logs.
    """

    handlers: MutableSequence[logging.Handler] = []

    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            filename=path,
            maxBytes=int(1e6),
            backupCount=3,
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
        )
        file_handler.setLevel(logging.WARNING)
        handlers.append(file_handler)

    handlers.append(
        RichHandler(
            level=level,
            rich_tracebacks=True,
            markup=True,
            show_time=False,
        )
    )

    logging.basicConfig(
        level=level,
        format="%(asctime)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
        handlers=handlers,
        force=True,
    )


def click_list_option_kwargs() -> Mapping[str, Any]:
    """Click option that accepts comma-separated values.

    Built-in list only allows repeated flags, which is ugly for larger lists.

    Returns:
        Mapping[str, Any]: Mapping of kwargs (to be unpacked with **).
    """

    def callback(
        ctx: click.Context,
        param: click.Option,
        value: Union[str, List[str]],
    ) -> Optional[List[str]]:
        if value is None:
            return None

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

    return {
        "type": click.UNPROCESSED,
        "callback": callback,
    }


def safe_name(text: Optional[str]) -> str:
    """Sanitizes a human-readable "friendly" name to a safe string.

    For example, "Joe's Collection" becomes "joe_s_collection".

    Args:
        text (Optional[str]): Unsafe text with non-underscore symbols and spaces.

    Returns:
        str: Sanitized lowercase string with underscores.
    """
    return re.sub(r"[^\w]", "_", text or "").lower()


def safe_description(text: Optional[str]) -> str:
    """Sanitizes a human-readable long text, such as description.

    Args:
        text (Optional[str]): Unsafe long text with Jinja syntax.

    Returns:
        str: Sanitized string with escaped Jinja syntax.
    """
    return re.sub(r"{{(.*)}}", r"\1", text or "")


def scan_fields(t: Mapping, fields: Iterable[str], ns: str) -> Mapping:
    """Reads meta fields from a schem object.

    Args:
        t (Mapping): Target to scan for fields.
        fields (List): List of fields to accept.
        ns (str): Field namespace (separated by .).

    Returns:
        Mapping: Field values.
    """

    vals = {}
    for field in fields:
        if f"{ns}.{field}" in t:
            value = t[f"{ns}.{field}"]
            vals[field] = value if value is not None else NullValue
    return vals
