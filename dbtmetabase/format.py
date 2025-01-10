from __future__ import annotations

import fnmatch
import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, MutableSequence, Optional, Sequence, TextIO

import yaml
from rich.logging import RichHandler


class Filter:
    """Inclusion/exclusion filtering."""

    def __init__(
        self,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
    ):
        """Inclusion/exclusion filtering.

        Args:
            include (Optional[Sequence[str]], optional): Optional inclusions (i.e. include only these). Defaults to None.
            exclude (Optional[Sequence[str]], optional): Optional exclusion list (i.e. exclude these, even if in inclusion list). Defaults to None.
        """
        self.include = self._norm_arg(include)
        self.exclude = self._norm_arg(exclude)

    def match(self, item: Optional[str]) -> bool:
        item = self._norm_item(item) if item else ""

        for exclude in self.exclude:
            if fnmatch.fnmatch(item, exclude):
                return False

        if self.include:
            for include in self.include:
                if fnmatch.fnmatch(item, include):
                    return True
            return False

        return True

    @staticmethod
    def _norm_arg(arg: Optional[Sequence[str]]) -> Sequence[str]:
        if isinstance(arg, str):
            arg = [arg]
        return [Filter._norm_item(x) for x in arg or []]

    @staticmethod
    def _norm_item(x: str) -> str:
        return x.upper()


class _YAMLDumper(yaml.Dumper):
    """Custom YAML dumper for uniform formatting."""

    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, indentless=False)


class _NullValue(str):
    """Explicitly null field value."""

    def __eq__(self, other: object) -> bool:
        return other is None


NullValue = _NullValue()


def dump_yaml(data: Any, stream: TextIO):
    """Uniform way to dump object to YAML file.

    Args:
        data (Any): Payload.
        stream (TextIO): Text file handle.
    """
    yaml.dump(
        data,
        stream,
        Dumper=_YAMLDumper,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )


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
    return re.sub(r"{{(.*?)}}", r"(\1)", text or "")
