from __future__ import annotations

import dataclasses as dc
import logging
import re
from abc import ABCMeta, abstractmethod
from operator import itemgetter
from pathlib import Path
from typing import (
    Any,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from urllib.parse import unquote

from dbtmetabase.metabase import Metabase

from .errors import ArgumentError
from .format import Filter, dump_yaml, safe_description, safe_name
from .manifest import DEFAULT_SCHEMA, Manifest

_RESOURCE_VERSION = 2

# Extracting table in `from` and `join` clauses (won't recognize some valid SQL, e.g. `from "table with spaces"`)
_EXPOSURE_PARSER = re.compile(r"[FfJj][RrOo][OoIi][MmNn]\s+([\w.\"`]+)")
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
        tags: Optional[Sequence[str]] = None,
    ) -> Iterable[Mapping]:
        """Extract dbt exposures from Metabase.

        Args:
            output_path (str, optional): Path for output files. Defaults to ".".
            output_grouping (Optional[str], optional): Grouping for output YAML files, supported values: "collection" (by collection slug) or "type" (by entity type). Defaults to None.
            collection_filter (Optional[Filter], optional): Filter Metabase collections. Defaults to None.
            allow_personal_collections (bool, optional): Allow personal Metabase collections. Defaults to False.
            exclude_unverified (bool, optional): Exclude items that have not been verified. Only applies to entity types that support verification. Defaults to False.
            tags (Sequence[str], optional): Optional tags for exported dbt exposures. Defaults to None.

        Returns:
            Iterable[Mapping]: List of parsed exposures.
        """

        if output_grouping not in (None, "collection", "type"):
            raise ArgumentError(f"Unsupported grouping: {output_grouping}")

        collection_filter = collection_filter or Filter()

        models = self.manifest.read_models()

        def dbname(details: Mapping) -> str:
            """Parse database name from Metabase database details."""
            for key in (
                "dbname",
                "db",
                "project-id",
                "project-id-from-credentials",
                "catalog",
            ):
                if key in details:
                    return details[key]
            return ""

        ctx = _Context(
            model_refs={m.alias_path.lower(): m.ref for m in models if m.ref},
            database_names={
                d["id"]: dbname(d["details"]) for d in self.metabase.get_databases()
            },
            table_names={
                t["id"]: ".".join(
                    [
                        dbname(t["db"]["details"]),
                        t["schema"] or DEFAULT_SCHEMA,
                        t["name"],
                    ]
                ).lower()
                for t in self.metabase.get_tables()
            },
        )

        exposures = []
        counts: MutableMapping[str, int] = {}

        for collection in self.metabase.get_collections(
            exclude_personal=not allow_personal_collections
        ):
            collection_name = collection["name"]

            if "slug" in collection:
                collection_slug = unquote(collection["slug"])
            else:
                collection_slug = safe_name(collection["name"])

            if not collection_filter.match(collection_name):
                _logger.debug("Skipping collection '%s'", collection["name"])
                continue

            _logger.info("Exploring collection '%s'", collection["name"])
            for item in self.metabase.get_collection_items(
                uid=collection["id"],
                models=("card", "dashboard"),
            ):
                exposure = _Exposure(
                    model=item["model"],
                    uid=item["id"],
                    label="Exposure [Unresolved Name]",
                )

                if (
                    exclude_unverified
                    and exposure.model == "card"
                    and item.get("moderated_status") != "verified"
                ):
                    _logger.debug("Skipping unverified card '%s'", item["name"])
                    continue

                entity: Mapping
                if exposure.model == "card":
                    card_entity = self.metabase.find_card(uid=item["id"])
                    if card_entity is None:
                        _logger.info("Card '%s' not found, skipping", item["id"])
                        continue

                    entity = card_entity
                    exposure.header = (
                        f"Visualization: {entity.get('display', 'Unknown').title()}"
                    )

                    self._exposure_card(ctx, exposure, entity)

                    if average_query_time_ms := entity.get("average_query_time"):
                        average_query_time_s = average_query_time_ms / 1000
                        exposure.average_query_time = f"{(average_query_time_s // 60):.0f}:{(average_query_time_s % 60):06.3f}"

                    exposure.last_used_at = entity.get("last_used_at")

                elif exposure.model == "dashboard":
                    dashboard_entity = self.metabase.find_dashboard(uid=item["id"])
                    if dashboard_entity is None:
                        _logger.info("Dashboard '%s' not found, skipping", item["id"])
                        continue

                    entity = dashboard_entity
                    cards = entity.get("dashcards", entity.get("ordered_cards", []))
                    if not cards:
                        continue

                    exposure.header = f"Dashboard Cards: {len(cards)}"
                    for card_ref in cards:
                        card = card_ref.get("card", {})
                        if "id" not in card:
                            continue

                        if card := self.metabase.find_card(uid=card["id"]):
                            self._exposure_card(ctx, exposure, card)
                else:
                    _logger.warning("Unexpected collection item '%s'", item["model"])
                    continue

                exposure.label = entity.get("name") or exposure.label
                exposure.description = entity.get("description") or exposure.description
                exposure.created_at = entity["created_at"]
                _logger.info("Processing %s '%s'", exposure.model, exposure.label)

                if "creator" in entity:
                    exposure.creator_email = entity["creator"]["email"]
                    exposure.creator_name = entity["creator"]["common_name"]
                elif "creator_id" in entity:
                    if creator := self.metabase.find_user(uid=entity["creator_id"]):
                        exposure.creator_name = creator.get("common_name", "")
                        exposure.creator_email = creator.get("email", "")

                exposure.name = safe_name(exposure.label)
                count = counts.get(exposure.name, 0)
                counts[exposure.name] = count + 1
                exposure.name = exposure.name + (f"_{count}" if count > 0 else "")

                exposures.append(
                    {
                        "id": item["id"],
                        "type": item["model"],
                        "collection": collection_slug,
                        "body": self.__format_exposure(
                            model=exposure.model,
                            uid=exposure.uid,
                            name=exposure.name,
                            label=exposure.label,
                            header=exposure.header,
                            description=exposure.description,
                            created_at=exposure.created_at,
                            creator_name=exposure.creator_name,
                            creator_email=exposure.creator_email,
                            last_used_at=exposure.last_used_at,
                            average_query_time=exposure.average_query_time,
                            native_query=exposure.native_query,
                            depends_on=sorted(
                                [
                                    ctx.model_refs[depend.lower()]
                                    for depend in exposure.depends
                                    if depend.lower() in ctx.model_refs
                                ]
                            ),
                            tags=tags,
                        ),
                    }
                )

        self.__write_exposures(exposures, output_path, output_grouping)

        return exposures

    def _exposure_card(self, ctx: _Context, exposure: _Exposure, card: Mapping):
        """Extracts exposures from Metabase questions."""

        dataset_query = card.get("dataset_query", {})
        card_type = dataset_query.get("type")
        if card_type == "query":
            self.__exposure_query(ctx, exposure, card)
        elif card_type == "native":
            self.__exposure_native(ctx, exposure, card)
        else:
            _logger.warning("Unsupported card type '%s'", card_type)

    def __exposure_query(self, ctx: _Context, exposure: _Exposure, card: Mapping):
        """Extracts exposures from Metabase GUI queries."""

        dataset_query = card.get("dataset_query", {})
        query = dataset_query.get("query", {})

        query_source: Union[str, int] = query.get("source-table", card.get("table_id"))
        if isinstance(query_source, str) and query_source.startswith("card__"):
            # Question based on another question
            source_card_uid = query_source.split("__")[-1]
            if source_card := self.metabase.find_card(uid=source_card_uid):
                self._exposure_card(ctx, exposure, source_card)

        elif isinstance(query_source, int) and query_source in ctx.table_names:
            # Question based on table
            source_table = ctx.table_names[query_source].lower()
            exposure.depends.add(source_table)
            _logger.info("Extracted model '%s' from card", source_table)

        # Find models in joins
        for join in query.get("joins", []):
            join_source: Union[str, int] = join.get("source-table")
            if isinstance(join_source, str) and join_source.startswith("card__"):
                # Question based on another question
                source_card_uid = join_source.split("__")[-1]
                if source_card := self.metabase.find_card(uid=source_card_uid):
                    self._exposure_card(ctx, exposure, source_card)

                continue

            elif isinstance(join_source, int) and join_source in ctx.table_names:
                # Joined model parsed
                joined_table = ctx.table_names[join_source].lower()
                exposure.depends.add(joined_table)
                _logger.info("Extracted model '%s' from join", joined_table)

    def __exposure_native(self, ctx: _Context, exposure: _Exposure, card: Mapping):
        """Extracts exposures from Metabase native queries."""

        dataset_query = card.get("dataset_query", {})
        database = dataset_query["database"]
        native_query = dataset_query["native"]["query"]

        # Parse common table expressions for exclusion
        ctes: MutableSequence[str] = []
        for matched_cte in re.findall(_CTE_PARSER, native_query):
            ctes.extend(group.lower() for group in matched_cte if group)

        # Parse SQL for exposures through FROM or JOIN clauses
        for sql_ref in re.findall(_EXPOSURE_PARSER, native_query):
            sql_ref = sql_ref.strip("`")  # BigQuery uses backticks `dataset.table`

            # DATABASE.schema.table -> [database, schema, table]
            parsed_model_path = [s.strip('"').lower() for s in sql_ref.split(".")]

            # Scrub CTEs (qualified sql_refs can not reference CTEs)
            if parsed_model_path[-1] in ctes and "." not in sql_ref:
                continue

            # Missing schema -> use default schema
            if len(parsed_model_path) < 2:
                parsed_model_path.insert(0, DEFAULT_SCHEMA.lower())
            # Missing database -> use query's database
            if len(parsed_model_path) < 3:
                database_name = ctx.database_names.get(database, "")
                parsed_model_path.insert(0, database_name.lower())

            # Fully-qualified database.schema.table
            parsed_model = ".".join(parsed_model_path)

            # Verify this is one of our parsed refable models so exposures dont break the DAG
            if not ctx.model_refs.get(parsed_model):
                continue

            if parsed_model:
                exposure.depends.add(parsed_model)
                _logger.info("Extracted model '%s' from native query", parsed_model)

        if exposure.model != "dashboard":
            # Only include SQL for query exposures
            exposure.native_query = native_query

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
        last_used_at: Optional[str],
        average_query_time: Optional[str],
        native_query: Optional[str],
        depends_on: Iterable[str],
        tags: Optional[Sequence[str]],
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
            native_query = "\n".join(x for x in native_query.split("\n") if x.strip())
            native_query = f"#### Query\n\n```\n{native_query}\n```\n\n"
        else:
            native_query = ""

        metadata = (
            "#### Metadata\n\n"
            + f"Metabase ID: __{uid}__\n\n"
            + f"Created On: __{created_at}__"
        )

        exposure: dict[str, Any] = {
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

        meta = {}
        if average_query_time:
            meta["average_query_time"] = average_query_time
        if last_used_at:
            meta["last_used_at"] = last_used_at
        if meta:
            exposure.setdefault("config", {})["meta"] = meta

        if tags:
            exposure["tags"] = list(tags)

        return exposure

    def __write_exposures(
        self,
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

            exps_unwrapped = (x["body"] for x in exps)
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
class _Context:
    model_refs: Mapping[str, str]
    database_names: Mapping[int, str]
    table_names: Mapping[int, str]


@dc.dataclass
class _Exposure:
    model: str
    uid: str
    label: str
    name: str = ""
    description: str = ""
    created_at: str = ""
    header: str = ""
    creator_name: str = ""
    creator_email: str = ""
    average_query_time: Optional[str] = None
    last_used_at: Optional[str] = None
    native_query: Optional[str] = None
    depends: Set[str] = dc.field(default_factory=set)
