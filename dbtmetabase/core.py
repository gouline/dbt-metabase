from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Tuple, Union

from requests.adapters import HTTPAdapter

from ._exposures import ExposuresMixin
from ._models import ModelsMixin
from .manifest import Manifest
from .metabase import Metabase

_logger = logging.getLogger(__name__)


class DbtMetabase(ModelsMixin, ExposuresMixin):
    """dbt + Metabase integration."""

    DEFAULT_HTTP_TIMEOUT = 15

    def __init__(
        self,
        manifest_path: Union[str, Path],
        metabase_url: str,
        metabase_api_key: Optional[str] = None,
        metabase_username: Optional[str] = None,
        metabase_password: Optional[str] = None,
        metabase_session_id: Optional[str] = None,
        skip_verify: bool = False,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        http_timeout: int = DEFAULT_HTTP_TIMEOUT,
        http_headers: Optional[dict] = None,
        http_adapter: Optional[HTTPAdapter] = None,
    ):
        """dbt + Metabase integration.

        Args:
            manifest_path (Union[str,Path]): Path to dbt manifest.json, usually in target/ directory after compilation.
            metabase_url (str): Metabase URL, e.g. "https://metabase.example.com".
            metabase_api_key (Optional[str], optional): Metabase API key (required unless providing username/password or session ID). Defaults to None.
            metabase_username (Optional[str], optional): Metabase username (required unless providing API key or session ID). Defaults to None.
            metabase_password (Optional[str], optional): Metabase password (required unless providing API key or session ID). Defaults to None.
            metabase_session_id (Optional[str], optional): Metabase session ID (deprecated and will be removed in future). Defaults to None.
            skip_verify (bool, optional): Skip TLS certificate verification (not recommended). Defaults to False.
            cert (Optional[Union[str, Tuple[str, str]]], optional): Path to a custom certificate. Defaults to None.
            http_timeout (int, optional): HTTP request timeout in secs. Defaults to 15.
            http_headers (Optional[dict], optional): Additional HTTP headers. Defaults to None.
            http_adapter (Optional[HTTPAdapter], optional): Custom requests HTTP adapter. Defaults to None.
        """

        self._manifest = Manifest(
            path=manifest_path,
        )
        self._metabase = Metabase(
            url=metabase_url,
            api_key=metabase_api_key,
            username=metabase_username,
            password=metabase_password,
            session_id=metabase_session_id,
            skip_verify=skip_verify,
            cert=cert,
            http_timeout=http_timeout,
            http_headers=http_headers,
            http_adapter=http_adapter,
        )

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    @property
    def metabase(self) -> Metabase:
        return self._metabase
