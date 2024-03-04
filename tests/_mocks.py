import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import requests

from dbtmetabase.core import DbtMetabase
from dbtmetabase.manifest import Manifest, Model
from dbtmetabase.metabase import Metabase

FIXTURES_PATH = Path("tests") / "fixtures"
TMP_PATH = Path("tests") / "tmp"


class MockMetabase(Metabase):
    def __init__(self, url: str):
        super().__init__(
            url=url,
            username=None,
            password=None,
            session_id="dummy",
            skip_verify=False,
            cert=None,
            http_timeout=1,
            http_headers=None,
            http_adapter=None,
        )

    def _api(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Mapping:
        path_toks = f"{path.lstrip('/')}.json".split("/")
        if path_toks[0] == "api" and method == "get":
            json_path = Path.joinpath(FIXTURES_PATH, *path_toks)
            if json_path.exists():
                with open(json_path, encoding="utf-8") as f:
                    return json.load(f)
            else:
                response = requests.Response()
                response.status_code = 404
                raise requests.exceptions.HTTPError(response=response)
        return {}


class MockManifest(Manifest):
    _models: Sequence[Model] = []

    def read_models(self) -> Sequence[Model]:
        if not self._models:
            self._models = super().read_models()
        return self._models


class MockDbtMetabase(DbtMetabase):
    def __init__(
        self,
        manifest_path: Path = FIXTURES_PATH / "manifest-v2.json",
        metabase_url: str = "http://localhost:3000",
    ):  # pylint: disable=super-init-not-called
        self._manifest = MockManifest(path=manifest_path)
        self._metabase = MockMetabase(url=metabase_url)
