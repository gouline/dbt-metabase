import logging
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

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
                session = dict(
                    self._api(
                        method="post",
                        path="/api/session",
                        json={"username": username, "password": password},
                    )
                )
                session_id = str(session["id"])
            else:
                raise ArgumentError("Metabase credentials or session ID required")
        self.session.headers["X-Metabase-Session"] = session_id

        _logger.info("Metabase session established")

    def _api(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Union[Mapping, Sequence]:
        """Raw API call."""

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
            _logger.error("HTTP request failed: %s", response.text)
            raise

        response_json = response.json()
        if "data" in response_json:
            # Since X.40.0 list responses are encapsulated in "data" with pagination parameters
            return response_json["data"]

        return response_json

    def find_database(self, name: str) -> Optional[Mapping]:
        """Finds database by name attribute or returns none."""
        for api_database in list(self._api("get", "/api/database")):
            if api_database["name"].upper() == name.upper():
                return api_database
        return None

    def sync_database_schema(self, uid: str):
        """Triggers schema sync on a database."""
        self._api("post", f"/api/database/{uid}/sync_schema")

    def get_database_metadata(self, uid: str) -> Mapping:
        """Retrieves metadata for all tables and fields in a database, including hidden ones."""
        return dict(
            self._api(
                method="get",
                path=f"/api/database/{uid}/metadata",
                params={"include_hidden": True},
            )
        )

    def get_tables(self) -> Sequence[Mapping]:
        """Retrieves all tables for all databases."""
        return list(self._api("get", "/api/table"))

    def get_collections(self, exclude_personal: bool) -> Sequence[Mapping]:
        """Retrieves all collections and optionally filters out personal collections."""
        results = list(
            self._api(
                method="get",
                path="/api/collection",
                params={"exclude-other-user-collections": exclude_personal},
            )
        )
        if exclude_personal:
            results = list(filter(lambda x: not x.get("personal_owner_id"), results))
        return results

    def get_collection_items(
        self,
        uid: str,
        models: Sequence[str],
    ) -> Sequence[Mapping]:
        """Retrieves collection items of specific types (e.g. card, dashboard, collection)."""
        results = list(
            self._api(
                method="get",
                path=f"/api/collection/{uid}/items",
                params={"models": models},
            )
        )
        results = list(filter(lambda x: x["model"] in models, results))
        return results

    def get_card(self, uid: str) -> Mapping:
        """Retrieves card (known as question in Metabase UI)."""
        return dict(self._api("get", f"/api/card/{uid}"))

    def format_card_url(self, uid: str) -> str:
        """Formats URL link to a card (known as question in Metabase UI)."""
        return f"{self.url}/card/{uid}"

    def get_dashboard(self, uid: str) -> Mapping:
        """Retrieves dashboard."""
        return dict(self._api("get", f"/api/dashboard/{uid}"))

    def format_dashboard_url(self, uid: str) -> str:
        """Formats URL link to a dashboard."""
        return f"{self.url}/dashboard/{uid}"

    def find_user(self, uid: str) -> Optional[Mapping]:
        """Finds user by ID or returns none."""
        try:
            return dict(self._api("get", f"/api/user/{uid}"))
        except requests.exceptions.HTTPError as error:
            if error.response and error.response.status_code == 404:
                _logger.warning("User '%s' not found", uid)
                return None
            raise

    def update_table(self, uid: str, body: Mapping) -> Mapping:
        """Posts update to an existing table."""
        return dict(self._api("put", f"/api/table/{uid}", json=body))

    def update_field(self, uid: str, body: Mapping) -> Mapping:
        """Posts an update to an existing table field."""
        return dict(self._api("put", f"/api/field/{uid}", json=body))
