import logging
import json
import os.path
import subprocess
from pathlib import Path
from typing import Optional, Union, List, Tuple, MutableMapping, Mapping

from .config import MetabaseConfig, DbtConfig
from .metabase import MetabaseModel
from .exceptions import (
    NoDbtPathSupplied,
    NoDbtSchemaSupplied,
    MetabaseClientNotInstantiated,
    DbtParserNotInstantiated,
    MetabaseUnableToSync,
)
from ..parsers.dbt_folder import DbtFolderReader
from ..parsers.dbt_manifest import DbtManifestReader
from ..metabase import MetabaseClient


class MetabaseInterface(MetabaseConfig):
    """Interface for interacting with instantiating a Metabase Config and preparing a client object"""

    _client: Optional[MetabaseClient] = None

    @property
    def client(self) -> MetabaseClient:
        if self._client is None:
            raise MetabaseClientNotInstantiated(
                "Metabase client is not yet instantiated. Call `prepare_metabase_client` method first"
            )
        return self._client

    @property
    def database_id(self) -> int:
        if self._client is None:
            raise MetabaseClientNotInstantiated(
                "Metabase client is not yet instantiated. Call `prepare_metabase_client` method first"
            )
        return self.client.find_database_id(self.database)

    @property
    def instance_url(self) -> str:
        f"{'http' if self.use_http else 'https'}://{self.host}"

    def prepare_metabase_client(self, dbt_models: Optional[List[MetabaseModel]] = None):
        """Prepares the metabase client which can then after be accessed via the `client` property

        Args:
            dbt_models (Optional[List[MetabaseModel]]): Used if sync is enabled to verify all dbt models passed exist in Metabase

        Raises:
            MetabaseUnableToSync: This error is raised if sync is enabled and a timeout is explicitly set in the `Metabase` object config
        """
        if dbt_models is None:
            dbt_models = []

        self._client = MetabaseClient(
            host=self.host,
            user=self.user,
            password=self.password,
            use_http=self.use_http,
            verify=self.verify,
        )

        # Sync and attempt schema alignment prior to execution; if timeout is not explicitly set, proceed regardless of success
        if self.sync:
            if self.sync_timeout is not None and not self._client.sync_and_wait(
                self.database,
                dbt_models,
                self.sync_timeout,
            ):
                logging.critical("Sync timeout reached, models still not compatible")
                raise MetabaseUnableToSync(
                    "Unable to align models between dbt target models and Metabase"
                )


class DbtInterface(DbtConfig):
    """Interface for interacting with instantiating a Dbt Config and preparing a validated parser object"""

    _parser: Optional[Union[DbtManifestReader, DbtFolderReader]] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validate_config()
        self.prepare_dbt_parser()

    @property
    def parser(self) -> Union[DbtManifestReader, DbtFolderReader]:
        if self._parser is None:
            raise DbtParserNotInstantiated(
                "dbt reader is not yet instantiated. Call `prepare_dbt_parser` method first"
            )
        return self._parser

    def get_config(self) -> DbtConfig:
        return DbtConfig(
            database=self.database,
            manifest_path=self.manifest_path,
            path=self.path,
            schema=self.schema,
            schema_excludes=self.schema_excludes,
            includes=self.includes,
            excludes=self.excludes,
        )

    def validate_config(self) -> None:
        """Validates a dbt config object

        Raises:
            NoDbtPathSupplied: If no path for either manifest or project is supplied, this error is raised
            NoDbtSchemaSupplied: If no schema is supplied while using the folder parser, this error is raised
        """
        # Check 1 Verify Path
        if not (self.path or self.manifest_path):
            raise NoDbtPathSupplied(
                "One of either dbt_path or dbt_manifest_path is required."
            )
        # Check 2 Notify User if Both Paths Are Supplied
        if self.path and self.manifest_path:
            logging.warning(
                "Both dbt path and manifest path were supplied. Prioritizing manifest parser"
            )
        # Check 3 Validation for Folder Parser
        if self.path and not self.schema:
            raise NoDbtSchemaSupplied(
                "Must supply a schema if using YAML parser, it is used to resolve foreign key relations and which Metabase models to propagate documentation to"
            )
        # ... Add checks to interface as needed

    def prepare_dbt_parser(self) -> None:
        """Resolve dbt reader being either YAML or manifest.json based which can then after be accessed via the `parser` property"""
        if self.manifest_path:
            self._parser = DbtManifestReader(os.path.expandvars(self.manifest_path))
        elif self.path:
            self._parser = DbtFolderReader(os.path.expandvars(self.path))
        else:
            raise NoDbtPathSupplied(
                "One of either dbt_path or dbt_manifest_path is required."
            )

    def read_models(
        self,
        include_tags: bool = True,
        docs_url: Optional[str] = None,
    ) -> Tuple[List[MetabaseModel], MutableMapping]:
        return self.parser.read_models(self, include_tags, docs_url)

    def compile_model(self, model_name, target: Optional[str] = None) -> bool:
        if not self.manifest_path:
            raise NoDbtPathSupplied(
                "Cannot return compiled model result unless `dbt_manifest_path` is supplied through cli, env, or config."
            )

        proj_dir = str(
            Path(self.path).absolute()
            if self.path
            else Path(self.manifest_path).parent.parent.absolute()
        )

        subprocess.run(
            [
                "dbt",
                "compile",
                "-m",
                model_name,
                "--project-dir",
                proj_dir,
                "--no-version-check",
            ]
            + (["--target", target] if target else []),
            check=True,
        )

        with open(self.manifest_path, "r", encoding="utf-8") as manifest_file:
            compiled_manifest: Mapping = json.load(manifest_file)

        for ix, model in enumerate(
            map(lambda x: x.split(".", 2)[-1], list(compiled_manifest["nodes"].keys()))
        ):
            if model == model_name:
                compiled_sql = list(compiled_manifest["nodes"].values())[ix][
                    "compiled_sql"
                ]
                break
        else:
            raise Exception("Model not found...")

        return compiled_sql
