from __future__ import annotations

import dataclasses as dc
import logging
import re
from abc import ABCMeta, abstractmethod
from operator import itemgetter
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, MutableSequence, Optional, Tuple

from dbtmetabase.metabase import Metabase

from .errors import ArgumentError
from .format import Filter, dump_yaml, safe_description, safe_name
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
        exclude_unverified: bool = False,
    ) -> Iterable[Mapping]:
        """Extract dbt exposures from Metabase.

        Args:
            output_path (str, optional): Path for output files. Defaults to ".".
            output_grouping (Optional[str], optional): Grouping for output YAML files, supported values: "collection" (by collection slug) or "type" (by entity type). Defaults to None.
            collection_filter (Optional[Filter], optional): Filter Metabase collections. Defaults to None.
            allow_personal_collections (bool, optional): Allow personal Metabase collections. Defaults to False.
            exclude_unverified (bool, optional): Exclude items that have not been verified. Only applies to entity types that support verification. Defaults to False.

        Returns:
            Iterable[Mapping]: List of parsed exposures.
        """

        if output_grouping not in (None, "collection", "type"):
            raise ArgumentError(f"Unsupported grouping: {output_grouping}")

        collection_filter = collection_filter or Filter()

        models = self.manifest.read_models()

        ctx = self.__Context(
            model_refs={m.name.lower(): m.ref for m in models if m.ref},
            table_names={t["id"]: t["name"] for t in self.metabase.get_tables()},
        )

        exposures = []
        counts: MutableMapping[str, int] = {}

        for collection in self.metabase.get_collections(
            exclude_personal=not allow_personal_collections
        ):
            collection_name = collection["name"]
            collection_slug = collection.get("slug", safe_name(collection["name"]))

            if not collection_filter.match(collection_name):
                _logger.debug("Skipping collection '%s'", collection["name"])
                continue

            _logger.info("Exploring collection '%s'", collection["name"])
            for item in self.metabase.get_collection_items(
                uid=collection["id"],
                models=("card", "dashboard"),
            ):
                if (
                    exclude_unverified
                    and item["model"] == "card"
                    and item.get("moderated_status") != "verified"
                ):
                    _logger.debug("Skipping unverified card '%s'", item["name"])
                    continue

                depends = set()
                native_query = ""
                header = ""

                entity: Mapping
                if item["model"] == "card":
                    card_entity = self.metabase.find_card(uid=item["id"])
                    if card_entity is None:
                        _logger.info("Card '%s' not found, skipping", item["id"])
                        continue

                    entity = card_entity
                    header = (
                        f"Visualization: {entity.get('display', 'Unknown').title()}"
                    )

                    result = self.__extract_card_exposures(ctx, card=entity)
                    depends.update(result["depends"])
                    native_query = result["native_query"]

                elif item["model"] == "dashboard":
                    dashboard_entity = self.metabase.find_dashboard(uid=item["id"])
                    if dashboard_entity is None:
                        _logger.info("Dashboard '%s' not found, skipping", item["id"])
                        continue

                    entity = dashboard_entity
                    cards = entity.get("dashcards", entity.get("ordered_cards", []))
                    if not cards:
                        continue

                    header = f"Dashboard Cards: {len(cards)}"
                    for card_ref in cards:
                        card = card_ref.get("card", {})
                        if "id" not in card:
                            continue

                        depends.update(
                            self.__extract_card_exposures(
                                ctx,
                                card=self.metabase.find_card(uid=card["id"]),
                            )["depends"]
                        )
                else:
                    _logger.warning("Unexpected collection item '%s'", item["model"])
                    continue

                name = entity.get("name", "Exposure [Unresolved Name]")
                _logger.info("Processing %s '%s'", item["model"], name)

                creator_name = None
                creator_email = None
                if "creator" in entity:
                    creator_email = entity["creator"]["email"]
                    creator_name = entity["creator"]["common_name"]
                elif "creator_id" in entity:
                    creator = self.metabase.find_user(uid=entity["creator_id"])
                    if creator:
                        creator_name = creator.get("common_name")
                        creator_email = creator.get("email")

                label = name
                name = safe_name(name)
                count = counts.get(name, 0)
                counts[name] = count + 1

                exposures.append(
                    {
                        "id": item["id"],
                        "type": item["model"],
                        "collection": collection_slug,
                        "body": self.__format_exposure(
                            model=item["model"],
                            uid=item["id"],
                            name=name + (f"_{count}" if count > 0 else ""),
                            label=label,
                            header=header,
                            description=entity.get("description", ""),
                            created_at=entity["created_at"],
                            creator_name=creator_name or "",
                            creator_email=creator_email or "",
                            native_query=native_query,
                            depends_on=sorted(
                                [
                                    ctx.model_refs[depend.lower()]
                                    for depend in depends
                                    if depend.lower() in ctx.model_refs
                                ]
                            ),
                        ),
                    }
                )

        self.__write_exposures(exposures, output_path, output_grouping)

        return exposures

    def __extract_card_exposures(
        self,
        ctx: __Context,
        card: Optional[Mapping],
    ) -> Mapping:
        """Extracts exposures from Metabase questions."""

        depends = set()
        native_query = ""

        if card:
            query = card.get("dataset_query", {})
            if query.get("type") == "query":
                # Metabase GUI derived query
                query_source = query.get("query", {}).get(
                    "source-table", card.get("table_id")
                )

                if str(query_source).startswith("card__"):
                    # Handle questions based on other questions
                    depends.update(
                        self.__extract_card_exposures(
                            ctx,
                            card=self.metabase.find_card(
                                uid=query_source.split("__")[-1]
                            ),
                        )["depends"]
                    )
                elif query_source in ctx.table_names:
                    # Normal question
                    source_table = ctx.table_names.get(query_source)
                    if source_table:
                        source_table = source_table.lower()
                        _logger.info("Extracted model '%s' from card", source_table)
                        depends.add(source_table)

                # Find models exposed through joins
                for join in query.get("query", {}).get("joins", []):
                    join_source = join.get("source-table")

                    if str(join_source).startswith("card__"):
                        # Handle questions based on other questions
                        depends.update(
                            self.__extract_card_exposures(
                                ctx,
                                card=self.metabase.find_card(
                                    uid=join_source.split("__")[-1]
                                ),
                            )["depends"]
                        )
                        continue

                    # Joined model parsed
                    joined_table = ctx.table_names.get(join_source)
                    if joined_table:
                        joined_table = joined_table.lower()
                        _logger.info("Extracted model '%s' from join", joined_table)
                        depends.add(joined_table)

            elif query.get("type") == "native":
                # Metabase native query
                native_query = query["native"].get("query")
                ctes: MutableSequence[str] = []

                # Parse common table expressions for exclusion
                for matched_cte in re.findall(_CTE_PARSER, native_query):
                    ctes.extend(group.lower() for group in matched_cte if group)

                # Parse SQL for exposures through FROM or JOIN clauses
                for sql_ref in re.findall(_EXPOSURE_PARSER, native_query):
                    # Grab just the table / model name
                    parsed_model = sql_ref.split(".")[-1].strip('"').lower()

                    # Scrub CTEs (qualified sql_refs can not reference CTEs)
                    if parsed_model in ctes and "." not in sql_ref:
                        continue

                    # Verify this is one of our parsed refable models so exposures dont break the DAG
                    if not ctx.model_refs.get(parsed_model):
                        continue

                    if parsed_model:
                        _logger.info(
                            "Extracted model '%s' from native query", parsed_model
                        )
                        depends.add(parsed_model)

        return {
            "depends": depends,
            "native_query": native_query,
        }

    def __format_exposure(
        self,
        model: str,
        uid: str,
        name: str,
        label: str,
        header: str,
        description: str,
        created_at: str,
        creator_name: str,
        creator_email: str,
        native_query: Optional[str],
        depends_on: Iterable[str],
    ) -> Mapping:
        """Builds dbt exposure representation (see https://docs.getdbt.com/reference/exposure-properties)."""

        dbt_type: str
        url: str
        if model == "card":
            dbt_type = "analysis"
            url = self.metabase.format_card_url(uid=uid)
        elif model == "dashboard":
            dbt_type = "dashboard"
            url = self.metabase.format_dashboard_url(uid=uid)
        else:
            raise ValueError("Unexpected exposure type")

        if header:
            header = f"### {header}\n\n"

        if description:
            description = description.strip()
        else:
            description = "No description provided in Metabase"

        if native_query:
            # Format query into markdown code block
            native_query = "\n".join(l for l in native_query.split("\n") if l.strip())
            native_query = f"#### Query\n\n```\n{native_query}\n```\n\n"

        metadata = (
            "#### Metadata\n\n"
            + f"Metabase ID: __{uid}__\n\n"
            + f"Created On: __{created_at}__"
        )

        return {
            "name": name,
            "label": label,
            "description": safe_description(
                f"{header}{description}\n\n{native_query}{metadata}"
            ),
            "type": dbt_type,
            "url": url,
            "maturity": "medium",
            "owner": {
                "name": creator_name,
                "email": creator_email,
            },
            "depends_on": list(depends_on),
        }

    @staticmethod
    def __write_exposures(
        exposures: Iterable[Mapping],
        output_path: str,
        output_grouping: Optional[str],
    ):
        """Write exposures to output files."""

        grouped: MutableMapping[Tuple[str, ...], MutableSequence[Mapping]] = {}
        for exposure in exposures:
            group: Tuple[str, ...] = ("exposures",)
            if output_grouping == "collection":
                group = (exposure["collection"],)
            elif output_grouping == "type":
                group = (exposure["type"], exposure["id"])

            exps = grouped.get(group, [])
            exps.append(exposure)
            if group not in grouped:
                grouped[group] = exps

        for group, exps in grouped.items():
            path = Path(output_path).expanduser()
            path = path.joinpath(*group[:-1]) / f"{group[-1]}.yml"
            path.parent.mkdir(parents=True, exist_ok=True)

            exps_unwrapped = map(lambda x: x["body"], exps)
            exps_sorted = sorted(exps_unwrapped, key=itemgetter("name"))

            with open(path, "w", encoding="utf-8") as f:
                dump_yaml(
                    data={
                        "version": _RESOURCE_VERSION,
                        "exposures": exps_sorted,
                    },
                    stream=f,
                )

    @dc.dataclass
    class __Context:
        model_refs: Mapping[str, str]
        table_names: Mapping[str, str]
