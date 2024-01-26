import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from dbtmetabase.core import DbtMetabase
from dbtmetabase.manifest import Manifest
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
        return {}


class MockDbtMetabase(DbtMetabase):
    def __init__(
        self,
        manifest_path: Path = FIXTURES_PATH / "manifest-v2.json",
        metabase_url: str = "http://localhost:3000",
    ):  # pylint: disable=super-init-not-called
        self._manifest = Manifest(path=manifest_path)
        self._metabase = MockMetabase(url=metabase_url)
