from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from pymongo import ASCENDING, UpdateOne
from pymongo.database import Database
from src.utils.serialization import serialize_mongo_document

logger = logging.getLogger(__name__)

WATERMARK_COLLECTION = "mv_refresh_watermarks"
WATERMARK_DOC_ID = "global_mv_watermark"
DAILY_SALES_MV = "mv_daily_sales_summary"
TOP_PRODUCTS_MV = "mv_top_products_summary"


def _ensure_mv_indexes(db: Database) -> None:
    db[DAILY_SALES_MV].create_index([("date", ASCENDING)], unique=True)
    db[TOP_PRODUCTS_MV].create_index([("sku", ASCENDING)], unique=True)


def _get_watermark(db: Database) -> dict[str, Any] | None:
    return db[WATERMARK_COLLECTION].find_one({"_id": WATERMARK_DOC_ID})


def _update_watermark(
    db: Database,
    last_doc_id: Any,
    total_processed_orders: int,
) -> None:
    db[WATERMARK_COLLECTION].update_one(
        {"_id": WATERMARK_DOC_ID},
        {
            "$set": {
                "last_order_mongo_id": str(last_doc_id) if last_doc_id else None,
                "total_orders_at_refresh": total_processed_orders,
                "last_refreshed_at": datetime.now(UTC).isoformat(),
            }
        },
        upsert=True,
    )


def build_initial_materialized_views(db: Database) -> dict[str, Any]:
    """Performs a full build of both Materialized Views and initializes the watermark."""
    _ensure_mv_indexes(db)
    col = db["orders_validated"]
    total_orders = col.count_documents({})

    if total_orders == 0:
        return {
            "status": "empty",
            "message": "No validated orders available to build materialized views.",
            "records_processed": 0,
        }

    now_iso = datetime.now(UTC).isoformat()

    # 1. Full build for Daily Sales Summary
    daily_pipeline: list[dict[str, Any]] = [
        {
            "$project": {
                "date": {"$substrCP": [{"$ifNull": ["$order_date", "1970-01-01"]}, 0, 10]},
                "total_amount": 1,
                "status": 1,
            }
        },
        {
            "$group": {
                "_id": "$date",
                "date": {"$first": "$date"},
                "total_orders": {"$sum": 1},
                "total_revenue": {"$sum": "$total_amount"},
                "avg_order_value": {"$avg": "$total_amount"},
                "delivered_orders": {
                    "$sum": {"$cond": [{"$eq": ["$status", "تم التسليم"]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "_id": "$date",
                "date": 1,
                "total_orders": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "delivered_orders": 1,
                "last_refreshed_at": {"$literal": now_iso},
            }
        },
    ]

    daily_results = list(col.aggregate(daily_pipeline))
    if daily_results:
        daily_ops = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            for doc in daily_results
        ]
        db[DAILY_SALES_MV].bulk_write(daily_ops, ordered=False)

    # 2. Full build for Top Products Summary
    products_pipeline: list[dict[str, Any]] = [
        {"$unwind": "$items_json"},
        {
            "$group": {
                "_id": "$items_json.sku",
                "sku": {"$first": "$items_json.sku"},
                "product_name": {"$first": "$items_json.name"},
                "total_units_sold": {"$sum": "$items_json.qty"},
                "total_revenue": {"$sum": "$items_json.total"},
                "orders_count": {"$sum": 1},
                "avg_unit_price": {"$avg": "$items_json.unit_price"},
            }
        },
        {
            "$project": {
                "_id": "$sku",
                "sku": 1,
                "product_name": 1,
                "total_units_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
                "last_refreshed_at": {"$literal": now_iso},
            }
        },
    ]

    product_results = list(col.aggregate(products_pipeline))
    if product_results:
        product_ops = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            for doc in product_results
        ]
        db[TOP_PRODUCTS_MV].bulk_write(product_ops, ordered=False)

    # Find highest _id for watermark
    latest_doc = col.find_one(sort=[("_id", -1)])
    latest_id = latest_doc["_id"] if latest_doc else None
    _update_watermark(db, latest_id, total_orders)

    return {
        "status": "success",
        "mode": "initial_build",
        "updated_dates": len(daily_results),
        "updated_products": len(product_results),
        "records_processed": total_orders,
    }


def refresh_materialized_views(
    db: Database,
    force_rebuild: bool = False,
) -> dict[str, Any]:
    """Synchronizes Materialized Views incrementally using delta watermark tracking.

    - If collections are empty or force_rebuild=True: performs initial build.
    - If new records arrived: calculates and replaces ONLY the affected dates and affected product partitions.
    - If no new data: safely returns 'up_to_date' without redundant computation.
    """
    _ensure_mv_indexes(db)
    col = db["orders_validated"]
    total_orders = col.count_documents({})

    daily_count = db[DAILY_SALES_MV].count_documents({})
    products_count = db[TOP_PRODUCTS_MV].count_documents({})
    watermark = _get_watermark(db)

    if force_rebuild or daily_count == 0 or products_count == 0 or watermark is None:
        logger.info("Executing initial/forced build of Materialized Views.")
        return build_initial_materialized_views(db)

    # Check for new data
    prev_total = watermark.get("total_orders_at_refresh", 0)
    if total_orders == prev_total:
        logger.info("Materialized Views are up to date (no new records).")
        return {
            "status": "up_to_date",
            "mode": "incremental_refresh",
            "message": "No new records detected since last refresh watermark.",
            "records_processed": 0,
            "total_orders": total_orders,
        }

    # Fetch new records that arrived after previous watermark
    from bson import ObjectId

    query: dict[str, Any] = {}
    last_id_str = watermark.get("last_order_mongo_id")
    if last_id_str:
        try:
            query = {"_id": {"$gt": ObjectId(last_id_str)}}
        except Exception:
            query = {}

    delta_orders = list(col.find(query))
    if not delta_orders:
        # Fallback if _id comparison returned empty but count differs
        delta_orders = list(col.find().sort("_id", -1).limit(max(1, total_orders - prev_total)))

    # Collect distinct affected dates and SKUs
    affected_dates: set[str] = set()
    affected_skus: set[str] = set()

    for doc in delta_orders:
        od = doc.get("order_date")
        if od and len(str(od)) >= 10:
            affected_dates.add(str(od)[:10])

        items = doc.get("items_json") or []
        for item in items:
            sku = item.get("sku")
            if sku:
                affected_skus.add(str(sku))

    now_iso = datetime.now(UTC).isoformat()

    # Incrementally re-aggregate ONLY affected dates
    updated_dates_count = 0
    if affected_dates:
        daily_pipeline = [
            {"$match": {"order_date": {"$regex": f"^({'|'.join(affected_dates)})"}}},
            {
                "$project": {
                    "date": {"$substrCP": ["$order_date", 0, 10]},
                    "total_amount": 1,
                    "status": 1,
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "date": {"$first": "$date"},
                    "total_orders": {"$sum": 1},
                    "total_revenue": {"$sum": "$total_amount"},
                    "avg_order_value": {"$avg": "$total_amount"},
                    "delivered_orders": {
                        "$sum": {"$cond": [{"$eq": ["$status", "تم التسليم"]}, 1, 0]}
                    },
                }
            },
            {
                "$project": {
                    "_id": "$date",
                    "date": 1,
                    "total_orders": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                    "delivered_orders": 1,
                    "last_refreshed_at": {"$literal": now_iso},
                }
            },
        ]
        date_summaries = list(col.aggregate(daily_pipeline))
        if date_summaries:
            ops = [
                UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True)
                for d in date_summaries
            ]
            db[DAILY_SALES_MV].bulk_write(ops, ordered=False)
            updated_dates_count = len(date_summaries)

    # Incrementally re-aggregate ONLY affected SKUs
    updated_skus_count = 0
    if affected_skus:
        products_pipeline = [
            {"$unwind": "$items_json"},
            {"$match": {"items_json.sku": {"$in": list(affected_skus)}}},
            {
                "$group": {
                    "_id": "$items_json.sku",
                    "sku": {"$first": "$items_json.sku"},
                    "product_name": {"$first": "$items_json.name"},
                    "total_units_sold": {"$sum": "$items_json.qty"},
                    "total_revenue": {"$sum": "$items_json.total"},
                    "orders_count": {"$sum": 1},
                    "avg_unit_price": {"$avg": "$items_json.unit_price"},
                }
            },
            {
                "$project": {
                    "_id": "$sku",
                    "sku": 1,
                    "product_name": 1,
                    "total_units_sold": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "orders_count": 1,
                    "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
                    "last_refreshed_at": {"$literal": now_iso},
                }
            },
        ]
        sku_summaries = list(col.aggregate(products_pipeline))
        if sku_summaries:
            ops = [
                UpdateOne({"_id": s["_id"]}, {"$set": s}, upsert=True)
                for s in sku_summaries
            ]
            db[TOP_PRODUCTS_MV].bulk_write(ops, ordered=False)
            updated_skus_count = len(sku_summaries)

    latest_doc = col.find_one(sort=[("_id", -1)])
    latest_id = latest_doc["_id"] if latest_doc else None
    _update_watermark(db, latest_id, total_orders)

    return {
        "status": "success",
        "mode": "incremental_refresh",
        "records_processed": len(delta_orders),
        "updated_dates": updated_dates_count,
        "updated_products": updated_skus_count,
        "total_orders": total_orders,
    }


def get_materialized_view_data(
    db: Database,
    view_name: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Reads stored documents directly from the specified Materialized View collection."""
    if view_name not in (DAILY_SALES_MV, TOP_PRODUCTS_MV):
        raise ValueError(
            f"Invalid view name '{view_name}'. Valid views: {[DAILY_SALES_MV, TOP_PRODUCTS_MV]}"
        )
    cursor = db[view_name].find().limit(limit)
    return [serialize_mongo_document(d) for d in cursor]
