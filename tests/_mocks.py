import json
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Union

import requests
from dotenv import dotenv_values

from dbtmetabase.core import DbtMetabase
from dbtmetabase.manifest import Column, Manifest, Model
from dbtmetabase.metabase import Metabase

FIXTURES_PATH = Path("tests") / "fixtures"
TMP_PATH = Path("tests") / "tmp"

RECORD = os.getenv("RECORD", "").lower() == "true"
SANDBOX_ENV = dotenv_values(Path().parent / "sandbox" / ".env")


class MockMetabase(Metabase):
    def __init__(self, url: str, record: bool = False):
        self.record = record

        api_key = "dummy"
        username = None
        password = None

        if record:
            api_key = None
            username = SANDBOX_ENV["MB_USER"]
            password = SANDBOX_ENV["MB_PASSWORD"]

        super().__init__(
            url=url,
            api_key=api_key,
            username=username,
            password=password,
            session_id=None,
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
    ) -> Union[Mapping, Sequence]:
        result = {}
        path_toks = f"{path.lstrip('/')}.json".split("/")
        json_path = Path.joinpath(FIXTURES_PATH, *path_toks)

        if self.record:
            is_auth = path == "/api/session"
            if method == "get" or is_auth:
                result = super()._api(method, path, params, **kwargs)

                if not is_auth:
                    json_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(result, f, indent=4)
        else:
            if method == "get":
                if json_path.exists():
                    with open(json_path, encoding="utf-8") as f:
                        result = json.load(f)
                else:
                    response = requests.Response()
                    response.status_code = 404
                    raise requests.exceptions.HTTPError(response=response)

        return result


class MockManifest(Manifest):
    _models: Sequence[Model] = []

    def read_models(self) -> Sequence[Model]:
        if not self._models:
            self._models = super().read_models()
        return self._models

    def find_model(self, model_name: str) -> Optional[Model]:
        filtered = [m for m in self._models if m.name == model_name]
        if filtered:
            return filtered[0]
        return None

    def find_column(
        self,
        model_name: str,
        column_name: str,
    ) -> Optional[Column]:
        model = self.find_model(model_name=model_name)
        if model:
            filtered = [c for c in model.columns if c.name == column_name]
            if filtered:
                return filtered[0]
        return None


class MockDbtMetabase(DbtMetabase):
    def __init__(
        self,
        manifest_path: Path = FIXTURES_PATH / "manifest-v12.json",
        metabase_url: str = f"http://localhost:{SANDBOX_ENV['MB_PORT']}",
    ):
        self._manifest = MockManifest(path=manifest_path)
        self._metabase = MockMetabase(url=metabase_url, record=RECORD)
