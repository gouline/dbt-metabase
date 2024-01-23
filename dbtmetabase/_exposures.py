from __future__ import annotations

import dataclasses as dc
import logging
import re
from abc import ABCMeta, abstractmethod
from operator import itemgetter
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, MutableSequence, Optional, Tuple

import requests
import yaml

from dbtmetabase.metabase import Metabase

from .errors import ArgumentError
from .format import Filter, YAMLDumper, safe_description, safe_name
from .manifest import Manifest

_RESOURCE_VERSION = 2

# Extracting table in `from` and `join` clauses (won't recognize some valid SQL, e.g. `from "table with spaces"`)
_EXPOSURE_PARSER = re.compile(r"[FfJj][RrOo][OoIi][MmNn]\s+([\w.\"]+)")
_CTE_PARSER = re.compile(
    r"[Ww][Ii][Tt][Hh]\s+\b(\w+)\b\s+as|[)]\s*[,]\s*\b(\w+)\b\s+as"
)

_logger = logging.getLogger(__name__)


class ExposuresMixin(metaclass=ABCMeta):
    """Abstraction for extracting exposures."""

    DEFAULT_EXPOSURES_OUTPUT_PATH = "."

    @property
    @abstractmethod
    def manifest(self) -> Manifest:
        pass

    @property
    @abstractmethod
    def metabase(self) -> Metabase:
        pass

    def extract_exposures(
        self,
        output_path: str = DEFAULT_EXPOSURES_OUTPUT_PATH,
        output_grouping: Optional[str] = None,
        collection_filter: Optional[Filter] = None,
        allow_personal_collections: bool = False,
    ) -> Iterable[Mapping]:
        """Extract dbt exposures from Metabase.

        Args:
            output_path (str, optional): Path for output files. Defaults to ".".
            output_grouping (Optional[str], optional): Grouping for output YAML files, supported values: "collection" (by collection slug) or "type" (by entity type). Defaults to None.
            collection_filter (Optional[Filter], optional): Filter Metabase collections. Defaults to None.
            allow_personal_collections (bool, optional): Allow personal Metabase collections. Defaults to False.

        Returns:
            Iterable[Mapping]: List of parsed exposures.
        """

        if output_grouping not in (None, "collection", "type"):
            raise ArgumentError(f"Unsupported grouping: {output_grouping}")

        models = self.manifest.read_models()

        ctx = self.__Context(
            model_refs={m.name.upper(): m.ref for m in models if m.ref},
            table_names={
                t["id"]: t["name"] for t in self.metabase.api("get", "/api/table")
            },
        )

        exposures = []
        exposure_counts: MutableMapping[str, int] = {}

        for collection in self.metabase.api(
            method="get",
            path="/api/collection",
            params={"exclude-other-user-collections": not allow_personal_collections},
        ):
            name = collection["name"]
            is_personal = collection.get("personal_owner_id", False)

            name_selected = not collection_filter or collection_filter.match(name)
            personal_skipped = not allow_personal_collections and is_personal
            if not name_selected or personal_skipped:
                _logger.debug("Skipping collection %s", collection["name"])
                continue

            _logger.info("Exploring collection: %s", collection["name"])
            expected_models = ["card", "dashboard"]
            for item in self.metabase.api(
                method="get",
                path=f"/api/collection/{collection['id']}/items",
                params={"models": expected_models},
            ):
                # Ensure collection item is of parsable type
                exposure_type = item["model"]
                exposure_id = item["id"]
                if exposure_type not in expected_models:
                    continue

                # Prepare attributes for population through _extract_card_exposures calls
                ctx.models_exposed = []
                ctx.native_query = ""
                native_query = ""

                exposure = self.metabase.api(
                    "get", f"/api/{exposure_type}/{exposure_id}"
                )
                exposure_name = exposure.get("name", "Exposure [Unresolved Name]")
                _logger.info("Introspecting exposure: %s", exposure_name)

                header = None
                creator_name = None
                creator_email = None

                # Process exposure
                if exposure_type == "card":
                    # Build header for card and extract models to self.models_exposed
                    header = "### Visualization: {}\n\n".format(
                        exposure.get("display", "Unknown").title()
                    )

                    # Parse Metabase question
                    self.__extract_card_exposures(ctx, exposure_id, exposure)
                    native_query = ctx.native_query

                elif exposure_type == "dashboard":
                    # We expect this dict key in order to iter through questions
                    cards = exposure.get("ordered_cards", [])
                    if not cards:
                        continue

                    # Build header for dashboard and extract models for each question to self.models_exposed
                    header = f"### Dashboard Cards: {len(cards)}\n\n"

                    # Iterate through dashboard questions
                    for dashboard_item in cards:
                        dashboard_item_reference = dashboard_item.get("card", {})
                        if "id" not in dashboard_item_reference:
                            continue

                        # Parse Metabase question
                        self.__extract_card_exposures(
                            ctx, dashboard_item_reference["id"]
                        )

                if not ctx.models_exposed:
                    _logger.info("No models mapped to exposure")

                # Extract creator info
                if "creator" in exposure:
                    creator_email = exposure["creator"]["email"]
                    creator_name = exposure["creator"]["common_name"]
                elif "creator_id" in exposure:
                    try:
                        creator = self.metabase.api(
                            "get", f"/api/user/{exposure['creator_id']}"
                        )
                    except requests.exceptions.HTTPError as error:
                        # If a Metabase user is deactivated, the API returns a 404
                        creator = {}
                        if error.response is None or error.response.status_code != 404:
                            raise

                    creator_name = creator.get("common_name")
                    creator_email = creator.get("email")

                exposure_label = exposure_name
                # Unique names with letters, numbers and underscores allowed in dbt docs DAG
                exposure_name = safe_name(exposure_name)
                exposure_count = exposure_counts.get(exposure_name, 0)
                exposure_counts[exposure_name] = exposure_count + 1
                exposure_suffix = f"_{exposure_count}" if exposure_count > 0 else ""

                exposures.append(
                    {
                        "id": item["id"],
                        "type": item["model"],
                        "collection": collection,
                        "body": self.__build_exposure(
                            ctx,
                            exposure_type=exposure_type,
                            exposure_id=exposure_id,
                            name=exposure_name + exposure_suffix,
                            label=exposure_label,
                            header=header or "",
                            created_at=exposure["created_at"],
                            creator_name=creator_name or "",
                            creator_email=creator_email or "",
                            description=exposure.get("description", ""),
                            native_query=native_query,
                        ),
                    }
                )

        self.__write_exposures(exposures, output_path, output_grouping)

        return exposures

    def __extract_card_exposures(
        self,
        ctx: __Context,
        card_id: int,
        exposure: Optional[Mapping] = None,
    ):
        """Extracts exposures from Metabase questions populating `ctx.models_exposed`

        Arguments:
            card_id {int} -- Metabase question ID used to pull question from API.

        Keyword Arguments:
            exposure {str} -- API response from a question in Metabase, allows us to use the object if already in memory.

        Returns:
            None -- ctx.models_exposed is populated through this method.
        """

        # If an exposure is not passed, pull from id
        if not exposure:
            exposure = self.metabase.api("get", f"/api/card/{card_id}")

        query = exposure.get("dataset_query", {})

        if query.get("type") == "query":
            # Metabase GUI derived query
            source_table_id = query.get("query", {}).get(
                "source-table", exposure.get("table_id")
            )

            if str(source_table_id).startswith("card__"):
                # Handle questions based on other question in virtual db
                self.__extract_card_exposures(
                    ctx,
                    card_id=int(source_table_id.split("__")[-1]),
                )
            else:
                # Normal question
                source_table = ctx.table_names.get(source_table_id)
                if source_table:
                    _logger.info(
                        "Model extracted from Metabase question: %s",
                        source_table,
                    )
                    ctx.models_exposed.append(source_table)

            # Find models exposed through joins
            for query_join in query.get("query", {}).get("joins", []):
                # Handle questions based on other question in virtual db
                if str(query_join.get("source-table", "")).startswith("card__"):
                    self.__extract_card_exposures(
                        ctx,
                        card_id=int(query_join.get("source-table").split("__")[-1]),
                    )
                    continue

                # Joined model parsed
                joined_table = ctx.table_names.get(query_join.get("source-table"))
                if joined_table:
                    _logger.info(
                        "Model extracted from Metabase question join: %s",
                        joined_table,
                    )
                    ctx.models_exposed.append(joined_table)

        elif query.get("type") == "native":
            # Metabase native query
            native_query = query["native"].get("query")
            ctes: MutableSequence[str] = []

            # Parse common table expressions for exclusion
            for matched_cte in re.findall(_CTE_PARSER, native_query):
                ctes.extend(group.upper() for group in matched_cte if group)

            # Parse SQL for exposures through FROM or JOIN clauses
            for sql_ref in re.findall(_EXPOSURE_PARSER, native_query):
                # Grab just the table / model name
                clean_exposure = sql_ref.split(".")[-1].strip('"').upper()

                # Scrub CTEs (qualified sql_refs can not reference CTEs)
                if clean_exposure in ctes and "." not in sql_ref:
                    continue
                # Verify this is one of our parsed refable models so exposures dont break the DAG
                if not ctx.model_refs.get(clean_exposure):
                    continue

                if clean_exposure:
                    _logger.info(
                        "Model extracted from native query: %s",
                        clean_exposure,
                    )
                    ctx.models_exposed.append(clean_exposure)
                    ctx.native_query = native_query

    def __build_exposure(
        self,
        ctx: __Context,
        exposure_type: str,
        exposure_id: int,
        name: str,
        label: str,
        header: str,
        created_at: str,
        creator_name: str,
        creator_email: str,
        description: str = "",
        native_query: str = "",
    ) -> Mapping:
        """Builds an exposure object representation as defined here: https://docs.getdbt.com/reference/exposure-properties

        Arguments:
            exposure_type {str} -- Model type in Metabase being either `card` or `dashboard`
            exposure_id {str} -- Card or Dashboard id in Metabase
            name {str} -- Name of exposure
            label {str} -- Title of the card or dashboard in Metabase
            header {str} -- The header goes at the top of the description and is useful for prefixing metadata
            created_at {str} -- Timestamp of exposure creation derived from Metabase
            creator_name {str} -- Creator name derived from Metabase
            creator_email {str} -- Creator email derived from Metabase

        Keyword Arguments:
            description {str} -- The description of the exposure as documented in Metabase. (default: No description provided in Metabase)
            native_query {str} -- If exposure contains SQL, this arg will include the SQL in the dbt exposure documentation. (default: {""})

        Returns:
            Mapping -- JSON object representation of single exposure.
        """

        # Ensure model type is compatible
        assert exposure_type in (
            "card",
            "dashboard",
        ), "Cannot construct exposure for object type of {}".format(exposure_type)

        if native_query:
            # Format query into markdown code block
            native_query = "#### Query\n\n```\n{}\n```\n\n".format(
                "\n".join(
                    sql_line
                    for sql_line in ctx.native_query.strip().split("\n")
                    if sql_line.strip() != ""
                )
            )

        if not description:
            description = "No description provided in Metabase\n\n"

        # Format metadata as markdown
        metadata = (
            "#### Metadata\n\n"
            + "Metabase Id: __{}__\n\n".format(exposure_id)
            + "Created On: __{}__".format(created_at)
        )

        # Build description
        description = (
            header + ("{}\n\n".format(description.strip())) + native_query + metadata
        )

        # Output exposure
        return {
            "name": name,
            "label": label,
            "description": safe_description(description),
            "type": "analysis" if exposure_type == "card" else "dashboard",
            "url": self.metabase.format_url(f"/{exposure_type}/{exposure_id}"),
            "maturity": "medium",
            "owner": {
                "name": creator_name,
                "email": creator_email,
            },
            "depends_on": list(
                {
                    ctx.model_refs[exposure.upper()]
                    for exposure in list({m for m in ctx.models_exposed})
                    if exposure.upper() in ctx.model_refs
                }
            ),
        }

    def __write_exposures(
        self,
        exposures: Iterable[Mapping],
        output_path: str,
        output_grouping: Optional[str],
    ):
        """Write exposures to output files.

        Args:
            output_path (str): Path for output files.
            exposures (Iterable[Mapping]): Collection of exposures.
        """

        for group, exp in self.__group_exposures(exposures, output_grouping).items():
            path = Path(output_path).expanduser()
            path = path.joinpath(*group[:-1]) / f"{group[-1]}.yml"
            path.parent.mkdir(parents=True, exist_ok=True)

            exposures_unwrapped = map(lambda x: x["body"], exp)
            exposures_sorted = sorted(exposures_unwrapped, key=itemgetter("name"))

            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(
                    {
                        "version": _RESOURCE_VERSION,
                        "exposures": exposures_sorted,
                    },
                    f,
                    Dumper=YAMLDumper,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                )

    def __group_exposures(
        self,
        exposures: Iterable[Mapping],
        output_grouping: Optional[str],
    ) -> Mapping[Tuple[str, ...], Iterable[Mapping]]:
        """Group exposures by configured output grouping.

        Args:
            exposures (Iterable[Mapping]): Collection of exposures.

        Returns:
            Mapping[Tuple[str, ...], Iterable[Mapping]]: Exposures indexed by configured grouping.
        """

        results: MutableMapping[Tuple[str, ...], MutableSequence[Mapping]] = {}

        for exposure in exposures:
            group: Tuple[str, ...] = ("exposures",)
            if output_grouping == "collection":
                collection = exposure["collection"]
                group = (collection.get("slug") or safe_name(collection["name"]),)
            elif output_grouping == "type":
                group = (exposure["type"], exposure["id"])

            result = results.get(group, [])
            result.append(exposure)
            if group not in results:
                results[group] = result

        return results

    @dc.dataclass
    class __Context:
        model_refs: Mapping[str, str]
        table_names: Mapping[str, str]
        models_exposed: MutableSequence[str] = dc.field(default_factory=list)
        native_query = ""
