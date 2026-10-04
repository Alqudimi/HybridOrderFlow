from __future__ import annotations

import logging
from typing import Any
from pymongo.database import Database
from src.utils.serialization import serialize_mongo_document

logger = logging.getLogger(__name__)


def get_orders_by_customer(
    db: Database,
    customer_id: str = "عميل-1",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve all validated orders for a specific customer, sorted by order_date DESC.

    Leverages single-field index: idx_customer_id
    """
    col = db["orders_validated"]
    cursor = (
        col.find({"customer_id": customer_id})
        .sort("order_date", -1)
        .limit(limit)
    )
    results = [serialize_mongo_document(doc) for doc in cursor]
    return results


def get_orders_by_city_and_status(
    db: Database,
    city: str = "صنعاء",
    status: str = "قيد الانتظار",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve orders filtered by city and order status, sorted by order_date DESC.

    Leverages compound index: idx_city_status_order_date (ESR pattern).
    """
    col = db["orders_validated"]
    cursor = (
        col.find({"city": city, "status": status})
        .sort("order_date", -1)
        .limit(limit)
    )
    results = [serialize_mongo_document(doc) for doc in cursor]
    return results


def get_high_value_orders(
    db: Database,
    min_amount: float = 500_000.0,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve high-value transactions exceeding a specified minimum total_amount.

    Useful for VIP customer audits, high-exposure risk monitoring, and revenue verification.
    """
    col = db["orders_validated"]
    cursor = (
        col.find({"total_amount": {"$gte": float(min_amount)}})
        .sort("total_amount", -1)
        .limit(limit)
    )
    results = [serialize_mongo_document(doc) for doc in cursor]
    return results


def get_orders_by_payment_status(
    db: Database,
    payment_status: str = "بانتظار الدفع",
    payment_method: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve orders based on payment reconciliation status and optional payment method filter.

    Used by financial reconciliation to track uncollected cash-on-delivery or pending wallet transfers.
    """
    col = db["orders_validated"]
    query: dict[str, Any] = {"payment_status": payment_status}
    if payment_method:
        query["payment_method"] = payment_method

    cursor = col.find(query).sort("order_date", -1).limit(limit)
    results = [serialize_mongo_document(doc) for doc in cursor]
    return results


def get_orders_containing_item_sku(
    db: Database,
    sku: str = "SKU-1010",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve orders containing a specific product SKU within items_json array.

    Leverages multikey index: idx_items_sku
    """
    col = db["orders_validated"]
    cursor = (
        col.find({"items_json.sku": sku})
        .sort("order_date", -1)
        .limit(limit)
    )
    results = [serialize_mongo_document(doc) for doc in cursor]
    return results
