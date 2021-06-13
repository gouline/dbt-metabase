import re
import logging

from pathlib import Path
from typing import List

import yaml

from dbtmetabase.models.metabase import METABASE_META_FIELDS
from dbtmetabase.models.metabase import MetabaseModel, MetabaseColumn


class DbtFolderReader:
    """
    Reader for dbt project configuration.
    """

    def __init__(self, project_path: str):
        """Constructor.

        Arguments:
            project_path {str} -- Path to dbt project root.
        """

        self.project_path = project_path

    def read_models(
        self,
        database: str,
        schema: str,
        schemas_excludes: list = [],
        includes: list = [],
        excludes: list = [],
        include_tags: bool = True,
        dbt_docs_url: str = None,
    ) -> List[MetabaseModel]:
        """Reads dbt models in Metabase-friendly format.

        Keyword Arguments:
            includes {list} -- Model names to limit processing to. (default: {[]})
            excludes {list} -- Model names to exclude. (default: {[]})

        Returns:
            list -- List of dbt models in Metabase-friendly format.
        """

        mb_models: List[MetabaseModel] = []

        for path in (Path(self.project_path) / "models").rglob("*.yml"):
            logging.info("Processing model: %s", path)
            with open(path, "r") as stream:
                schema_file = yaml.safe_load(stream)
                if schema_file is None:
                    logging.warn("Skipping empty or invalid YAML: %s", path)
                    continue
                for model in schema_file.get("models", []):
                    name = model.get("identifier", model["name"])
                    logging.info("Model: %s", name)
                    if (not includes or name in includes) and (name not in excludes):
                        mb_models.append(
                            self._read_model(model, schema, include_tags=include_tags)
                        )
                for source in schema_file.get("sources", []):
                    for model in source.get("tables", []):
                        name = model.get("identifier", model["name"])
                        logging.info("Source: %s", name)
                        if (not includes or name in includes) and (
                            name not in excludes
                        ):
                            mb_models.append(
                                self._read_model(
                                    model, schema, include_tags=include_tags
                                )
                            )

        return mb_models

    def _read_model(self, model: dict, schema: str, include_tags=True) -> MetabaseModel:
        """Reads one dbt model in Metabase-friendly format.

        Arguments:
            model {dict} -- One dbt model to read.

        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        mb_columns: List[MetabaseColumn] = []

        for column in model.get("columns", []):
            mb_columns.append(self._read_column(column))

        description = model.get("description")

        if include_tags:
            tags = model.get("tags")
            if tags:
                tags = ", ".join(tags)
                if description != "":
                    description += "\n\n"
                description += f"Tags: {tags}"

        return MetabaseModel(
            name=model["name"].upper(),
            schema=schema,
            description=description,
            columns=mb_columns,
        )

    def _read_column(self, column: dict) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        mb_column = MetabaseColumn(
            name=column.get("name", "").upper(), description=column.get("description")
        )

        for test in column.get("tests", []):
            if isinstance(test, dict):
                if "relationships" in test:
                    relationships = test["relationships"]
                    mb_column.semantic_type = "type/FK"
                    mb_column.fk_target_table = (
                        column.get("meta", {})
                        .get("metabase.fk_ref", self.parse_ref(relationships["to"]))
                        .upper()
                        .strip('"')
                    )
                    mb_column.fk_target_field = relationships["field"].upper()

        if "meta" in column:
            meta = column.get("meta")
            for field in METABASE_META_FIELDS:
                if f"metabase.{field}" in meta:
                    setattr(mb_column, field, meta[f"metabase.{field}"])

        return mb_column

    @staticmethod
    def parse_ref(text: str) -> str:
        """Parses dbt ref() statement.

        Arguments:
            text {str} -- Full statement in dbt YAML.

        Returns:
            str -- Name of the reference.
        """

        matches = re.findall(r"ref\(['\"]([\w\_\-\ ]+)['\"]\)", text)
        if matches:
            return matches[0]
        return text
