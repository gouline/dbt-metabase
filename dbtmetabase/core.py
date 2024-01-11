from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter, Retry

from .core_exposures import ExposuresExtractorMixin
from .core_models import ModelsExporterMixin
from .interface import MetabaseArgumentError
from .manifest import Manifest, Model

_logger = logging.getLogger(__name__)


class DbtMetabase(ModelsExporterMixin, ExposuresExtractorMixin):
    """dbt + Metabase integration."""

    DEFAULT_HTTP_TIMEOUT = 15

    def __init__(
        self,
        manifest_path: Union[str, Path],
        metabase_url: str,
        metabase_username: Optional[str] = None,
        metabase_password: Optional[str] = None,
        metabase_session_id: Optional[str] = None,
        skip_verify: bool = False,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
        http_timeout: int = DEFAULT_HTTP_TIMEOUT,
        http_headers: Optional[dict] = None,
        http_adapter: Optional[HTTPAdapter] = None,
    ):
        """
        Args:
            manifest_path (Union[str,Path]): Path to dbt manifest.json, usually in target/ directory after compilation.
            metabase_url (str): Metabase URL, e.g. "https://metabase.example.com".
            metabase_username (Optional[str], optional): Metabase username (required unless providing session ID). Defaults to None.
            metabase_password (Optional[str], optional): Metabase password (required unless providing session ID). Defaults to None.
            metabase_session_id (Optional[str], optional): Metabase session ID. Defaults to None.
            skip_verify (bool, optional): Skip TLS certificate verification (not recommended). Defaults to False.
            cert (Optional[Union[str, Tuple[str, str]]], optional): Path to a custom certificate. Defaults to None.
            http_timeout (int, optional): HTTP request timeout in secs. Defaults to 15.
            http_headers (Optional[dict], optional): Additional HTTP headers. Defaults to None.
            http_adapter (Optional[HTTPAdapter], optional): Custom requests HTTP adapter. Defaults to None.
        """

        self.manifest_reader = Manifest(path=manifest_path)

        self.metabase_url = metabase_url.rstrip("/")

        self.http_timeout = http_timeout

        self.session = requests.Session()
        self.session.verify = not skip_verify
        self.session.cert = cert

        if http_headers:
            self.session.headers.update(http_headers)

        self.session.mount(
            self.metabase_url,
            http_adapter or HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)),
        )

        if not metabase_session_id:
            if metabase_username and metabase_password:
                session = self.metabase_api(
                    method="post",
                    path="/api/session",
                    json={"username": metabase_username, "password": metabase_password},
                )
                metabase_session_id = str(session["id"])
            else:
                raise MetabaseArgumentError("Credentials or session ID required")
        self.session.headers["X-Metabase-Session"] = metabase_session_id

        _logger.info("Metabase session established")

    def read_models(self) -> Iterable[Model]:
        return self.manifest_reader.read_models()

    def metabase_api(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        critical: bool = True,
        **kwargs,
    ) -> Mapping:
        """Unified way of calling Metabase API.

        Args:
            method (str): HTTP verb, e.g. get, post, put.
            path (str): Relative path of endpoint, e.g. /api/database.
            critical (bool, optional): Raise on any HTTP errors. Defaults to True.

        Returns:
            Mapping: JSON payload of the endpoint.
        """

        if params:
            for key, value in params.items():
                if isinstance(value, bool):
                    params[key] = str(value).lower()

        response = self.session.request(
            method=method,
            url=f"{self.metabase_url}{path}",
            params=params,
            timeout=self.http_timeout,
            **kwargs,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            if critical:
                _logger.error("HTTP request failed: %s", response.text)
                raise
            return {}

        response_json = response.json()
        if "data" in response_json:
            # Since X.40.0 responses are encapsulated in "data" with pagination parameters
            return response_json["data"]

        return response_json

    def format_metabase_url(self, path: str) -> str:
        return self.metabase_url + path
