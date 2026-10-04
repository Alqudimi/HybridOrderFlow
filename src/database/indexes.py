from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IndexDefinition:
    collection_name: str
    name: str
    keys: list[tuple[str, int]]
    unique: bool = False
    index_type: str = "single"  # single | multikey | compound
    description: str = ""
    target_query: str = ""
    rationale: str = ""


PHASE2_INDEXES: list[IndexDefinition] = [
    IndexDefinition(
        collection_name="orders_validated",
        name="idx_customer_id",
        keys=[("customer_id", ASCENDING)],
        index_type="single",
        description="Single-field index on customer_id for high-frequency customer order lookups.",
        target_query="orders_by_customer",
        rationale=(
            "Customer dashboards and customer support query order history by customer_id. "
            "Without this index, MongoDB must scan every document in the collection (COLLSCAN). "
            "With this index, MongoDB utilizes an index scan (IXSCAN) with logarithmic complexity."
        ),
    ),
    IndexDefinition(
        collection_name="orders_validated",
        name="idx_items_sku",
        keys=[("items_json.sku", ASCENDING)],
        index_type="multikey",
        description="Multikey index on nested array field items_json.sku for product-level queries.",
        target_query="orders_containing_item_sku",
        rationale=(
            "Orders contain an array of purchased products (items_json). Searching for orders containing "
            "a specific SKU requires inspecting every item element in every order without an index. "
            "The multikey B-tree index indexes each array element individually, allowing instant lookup."
        ),
    ),
    IndexDefinition(
        collection_name="orders_validated",
        name="idx_city_status_order_date",
        keys=[
            ("city", ASCENDING),
            ("status", ASCENDING),
            ("order_date", DESCENDING),
        ],
        index_type="compound",
        description="Compound index on (city, status, order_date DESC) following the ESR rule.",
        target_query="orders_by_city_and_status",
        rationale=(
            "Regional dispatchers filter orders by city and status and demand results sorted by order_date DESC. "
            "Following the ESR (Equality, Sort, Range) design pattern: equality fields ('city', 'status') "
            "come first, followed by the sort field ('order_date' DESC). This eliminates both the full collection "
            "scan (COLLSCAN) and the expensive in-memory sort stage (SORT), executing an efficient IXSCAN that directly "
            "returns documents in the required sorted order."
        ),
    ),
]


def create_database_indexes(db: Database) -> dict[str, Any]:
    """Idempotently creates all designated operational and analytics indexes.

    Returns a summary of newly created and existing indexes.
    """
    results: list[dict[str, Any]] = []

    for idx in PHASE2_INDEXES:
        col = db[idx.collection_name]
        existing_indexes = {spec.get("name"): spec for spec in col.list_indexes()}

        if idx.name in existing_indexes:
            logger.info("Index %s already exists on %s", idx.name, idx.collection_name)
            results.append(
                {
                    "name": idx.name,
                    "collection": idx.collection_name,
                    "keys": idx.keys,
                    "type": idx.index_type,
                    "status": "already_exists",
                }
            )
        else:
            created_name = col.create_index(
                idx.keys,
                name=idx.name,
                unique=idx.unique,
                background=True,
            )
            logger.info("Created index %s on %s", created_name, idx.collection_name)
            results.append(
                {
                    "name": created_name,
                    "collection": idx.collection_name,
                    "keys": idx.keys,
                    "type": idx.index_type,
                    "status": "created",
                }
            )

    return {
        "status": "success",
        "total_indexes": len(PHASE2_INDEXES),
        "indexes": results,
    }


def drop_database_indexes(db: Database) -> dict[str, Any]:
    """Drops Phase 2 indexes safely (primarily used for before/after explain benchmarking)."""
    dropped: list[str] = []
    for idx in PHASE2_INDEXES:
        col = db[idx.collection_name]
        existing_names = {spec.get("name") for spec in col.list_indexes()}
        if idx.name in existing_names:
            col.drop_index(idx.name)
            dropped.append(idx.name)
            logger.info("Dropped index %s from %s", idx.name, idx.collection_name)

    return {"status": "success", "dropped": dropped}


def get_index_strategy_documentation() -> list[dict[str, Any]]:
    """Returns structured metadata documenting problem, index choice, rationale, and target query."""
    return [
        {
            "query": idx.target_query,
            "problem": f"Without index, execution forces a full collection scan (COLLSCAN) on {idx.collection_name}.",
            "index": idx.name,
            "keys": [f"{k}: {v}" for k, v in idx.keys],
            "type": idx.index_type,
            "reason": idx.rationale,
            "esr_pattern": (
                "Equality: [city, status] -> Sort: [order_date DESC]"
                if idx.index_type == "compound"
                else "Single/Multikey target lookup"
            ),
        }
        for idx in PHASE2_INDEXES
    ]
