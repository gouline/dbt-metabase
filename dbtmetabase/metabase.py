import logging
from typing import Any, Dict, Mapping, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter, Retry

from .errors import ArgumentError

_logger = logging.getLogger(__name__)


class Metabase:
    def __init__(
        self,
        url: str,
        username: Optional[str],
        password: Optional[str],
        session_id: Optional[str],
        skip_verify: bool,
        cert: Optional[Union[str, Tuple[str, str]]],
        http_timeout: int,
        http_headers: Optional[dict],
        http_adapter: Optional[HTTPAdapter],
    ):
        self.url = url.rstrip("/")

        self.http_timeout = http_timeout

        self.session = requests.Session()
        self.session.verify = not skip_verify
        self.session.cert = cert

        if http_headers:
            self.session.headers.update(http_headers)

        self.session.mount(
            self.url,
            http_adapter or HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)),
        )

        if not session_id:
            if username and password:
                session = self.api(
                    method="post",
                    path="/api/session",
                    json={"username": username, "password": password},
                )
                session_id = str(session["id"])
            else:
                raise ArgumentError("Metabase credentials or session ID required")
        self.session.headers["X-Metabase-Session"] = session_id

        _logger.info("Metabase session established")

    def api(
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
            url=f"{self.url}{path}",
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

    def format_url(self, path: str) -> str:
        return self.url + path
