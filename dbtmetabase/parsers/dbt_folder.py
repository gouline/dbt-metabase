import re
import os
import logging

from pathlib import Path
from typing import List, Iterable, Mapping, MutableMapping

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

        self.project_path = os.path.expanduser(project_path)
        self.catch_aliases: MutableMapping = {}

    def read_models(
        self,
        database: str,
        schema: str,
        schema_excludes: Iterable = None,
        includes: Iterable = None,
        excludes: Iterable = None,
        include_tags: bool = True,
        docs_url: str = None,
    ) -> List[MetabaseModel]:
        """Reads dbt models in Metabase-friendly format.

        Keyword Arguments:
            includes {list} -- Model names to limit processing to. (default: {None})
            excludes {list} -- Model names to exclude. (default: {None})

        Returns:
            list -- List of dbt models in Metabase-friendly format.
        """

        if schema_excludes is None:
            schema_excludes = []
        if includes is None:
            includes = []
        if excludes is None:
            excludes = []

        if database:
            logging.info(
                "Argument --database %s is unused in dbt_project yml parser. Use manifest parser instead.",
                database,
            )

        if docs_url:
            logging.info(
                "Argument --docs_url %s is unused in dbt_project yml parser. Use manifest parser instead.",
                docs_url,
            )

        mb_models: List[MetabaseModel] = []

        for path in (Path(self.project_path) / "models").rglob("*.yml"):
            logging.info("Processing model: %s", path)
            with open(path, "r") as stream:
                schema_file = yaml.safe_load(stream)
                if schema_file is None:
                    logging.warning("Skipping empty or invalid YAML: %s", path)
                    continue
                for model in schema_file.get("models", []):
                    name = model.get("identifier", model["name"])
                    if "identifier" in model:
                        self.catch_aliases[name] = model["name"]
                    logging.info("Model: %s", name)
                    if (not includes or name in includes) and (name not in excludes):
                        mb_models.append(
                            self._read_model(
                                model, schema.upper(), include_tags=include_tags
                            )
                        )
                for source in schema_file.get("sources", []):
                    source_schema_name = source.get("schema", source["name"])
                    if "{{" in source_schema_name and "}}" in source_schema_name:
                        logging.warning(
                            "dbt Folder Reader cannot resolve jinja expressions- use the Manifest Reader instead."
                        )

                    for model in source.get("tables", []):
                        name = model.get("identifier", model["name"])
                        if "identifier" in model:
                            self.catch_aliases[name] = model["name"]
                        logging.info("Source: %s", name)
                        if (not includes or name in includes) and (
                            name not in excludes
                        ):
                            mb_models.append(
                                self._read_model(
                                    model,
                                    source_schema_name.upper(),
                                    include_tags=include_tags,
                                )
                            )

        return mb_models

    def _read_model(
        self, model: dict, schema: str, include_tags: bool = True
    ) -> MetabaseModel:
        """Reads one dbt model in Metabase-friendly format.

        Arguments:
            model {dict} -- One dbt model to read.

        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        mb_columns: List[MetabaseColumn] = []

        for column in model.get("columns", []):
            mb_columns.append(self._read_column(column, schema))

        description = model.get("description", "")

        if include_tags:
            tags = model.get("tags")
            if tags:
                tags = ", ".join(tags)
                if description:
                    description += "\n\n"
                description += f"Tags: {tags}"

        return MetabaseModel(
            # We are implicitly complying with aliases by doing this
            name=model.get("identifier", model["name"]).upper(),
            schema=schema,
            description=description,
            columns=mb_columns,
        )

    def _read_column(self, column: Mapping, schema: str) -> MetabaseColumn:
        """Reads one dbt column in Metabase-friendly format.

        Arguments:
            column {dict} -- One dbt column to read.

        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        mb_column = MetabaseColumn(
            name=column.get("name", "").upper().strip('"'),
            description=column.get("description", ""),
        )

        for test in column.get("tests", []):
            if isinstance(test, dict):
                if "relationships" in test:
                    relationships = test["relationships"]
                    mb_column.semantic_type = "type/FK"
                    # Note: For foreign keys that point to a different schema than the target, the yml meta: metabase.fk_ref must be used
                    # Otherwise we use target schema which should be fine in 95% of cases
                    mb_column.fk_target_table = (
                        column.get("meta", {})
                        .get(
                            # Prioritize explicitly set FK in yml file which should have format schema.table unaliased
                            "metabase.fk_ref",
                            # If metabase.fk_ref not present in YAML, infer FK relation to table in target schema and parse ref/source
                            # We will be translating any potentially aliased source() calls later during FK parsing since we do not have all possible aliases yet and thus cannot unalias
                            self.parse_ref(relationships["to"], schema),
                        )
                        .strip('"')
                    )
                    if not mb_column.fk_target_table:
                        logging.warning(
                            "Could not resolve foreign key target for column %s",
                            mb_column.name,
                        )
                        continue
                    # Lets be lenient and try to infer target schema if it was not provided when specified in metabase.fk_ref
                    # Because parse_ref guarantees schema.table format, we can assume this was derived through fk_ref
                    if "." not in mb_column.fk_target_table:
                        logging.warning(
                            "Target table %s has fk ref declared through metabase.fk_ref missing schema (Format should be schema.table), inferring from target",
                            mb_column.fk_target_table,
                        )
                        mb_column.fk_target_table = (
                            f"{schema}.{mb_column.fk_target_table}"
                        )
                    mb_column.fk_target_table = mb_column.fk_target_table.upper()
                    # Account for (example) '"Id"' relationship: to: fields used as a workaround for current tests not quoting consistently
                    mb_column.fk_target_field = (
                        relationships["field"].upper().strip('"')
                    )

        if "meta" in column:
            meta = column.get("meta", [])
            for field in METABASE_META_FIELDS:
                if f"metabase.{field}" in meta:
                    setattr(mb_column, field, meta[f"metabase.{field}"])

        return mb_column

    @staticmethod
    def parse_ref(text: str, schema: str) -> str:
        """Parses dbt ref() statement.

        Arguments:
            text {str} -- Full statement in dbt YAML.

        Returns:
            str -- Name of the reference.
        """

        # matches = re.findall(r"ref\(['\"]([\w\_\-\ ]+)['\"]\)", text)
        # If we relax our matching here, we are able to catch the rightmost argument of either source or ref which is ultimately the table name
        # We can and should identify a way to handle indentifier specs, but as is this will add compatibility with many sources
        matches = re.findall(r"['\"]([\w\_\-\ ]+)['\"].*\)", text)
        if matches:
            return f"{schema}.{matches[0]}"
        return f"{schema}.{text}"
