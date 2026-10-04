from __future__ import annotations

import logging
from typing import Any
from pymongo.database import Database
from src.utils.serialization import serialize_mongo_document

logger = logging.getLogger(__name__)


def report_sales_by_city(
    db: Database,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Generates revenue and order distribution metrics grouped by governorate/city.

    Computes: total_revenue, order_count, average_order_value, and delivered_orders count.
    """
    col = db["orders_validated"]
    pipeline: list[dict[str, Any]] = [
        {
            "$group": {
                "_id": "$city",
                "total_revenue": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "delivered_orders": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "تم التسليم"]}, 1, 0]
                    }
                },
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "delivered_orders": 1,
            }
        },
    ]
    results = [serialize_mongo_document(doc) for doc in col.aggregate(pipeline)]
    return results


def report_top_products(
    db: Database,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Unwinds line items to rank top-selling products by total revenue and quantity sold.

    Computes: sku, product_name, total_units_sold, total_sales_revenue, and order_occurrences.
    """
    col = db["orders_validated"]
    pipeline: list[dict[str, Any]] = [
        {"$unwind": "$items_json"},
        {
            "$group": {
                "_id": {
                    "sku": "$items_json.sku",
                    "name": "$items_json.name",
                },
                "total_units_sold": {"$sum": "$items_json.qty"},
                "total_revenue": {"$sum": "$items_json.total"},
                "orders_count": {"$sum": 1},
                "avg_unit_price": {"$avg": "$items_json.unit_price"},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "sku": "$_id.sku",
                "product_name": "$_id.name",
                "total_units_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
            }
        },
    ]
    results = [serialize_mongo_document(doc) for doc in col.aggregate(pipeline)]
    return results


def report_top_customers(
    db: Database,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Ranks most valuable customers (LTV) by total historical spending and order frequency.

    Computes: customer_id, customer_name, total_spent, orders_count, avg_order_value, and cities_active.
    """
    col = db["orders_validated"]
    pipeline: list[dict[str, Any]] = [
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$first": "$customer_name"},
                "total_spent": {"$sum": "$total_amount"},
                "orders_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "cities_active": {"$addToSet": "$city"},
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "total_spent": {"$round": ["$total_spent", 2]},
                "orders_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "cities_active": 1,
            }
        },
    ]
    results = [serialize_mongo_document(doc) for doc in col.aggregate(pipeline)]
    return results


def report_sales_by_period(
    db: Database,
) -> list[dict[str, Any]]:
    """Analyzes sales trends grouped chronologically by month (YYYY-MM).

    Computes: period, total_revenue, order_count, avg_order_value, and total_delivery_fees.
    """
    col = db["orders_validated"]
    pipeline: list[dict[str, Any]] = [
        {
            "$project": {
                "period": {"$substrCP": [{"$ifNull": ["$order_date", "1970-01-01"]}, 0, 7]},
                "total_amount": 1,
                "delivery_cost": 1,
            }
        },
        {
            "$group": {
                "_id": "$period",
                "total_revenue": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "period": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "total_delivery_fees": {"$round": ["$total_delivery_fees", 2]},
            }
        },
    ]
    results = [serialize_mongo_document(doc) for doc in col.aggregate(pipeline)]
    return results


def report_orders_by_status(
    db: Database,
) -> list[dict[str, Any]]:
    """Analyzes workflow distribution across operational order statuses.

    Computes: status, orders_count, cumulative_value, and associated payment methods.
    """
    col = db["orders_validated"]
    pipeline: list[dict[str, Any]] = [
        {
            "$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "cumulative_value": {"$sum": "$total_amount"},
                "payment_methods_used": {"$addToSet": "$payment_method"},
            }
        },
        {"$sort": {"count": -1}},
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "count": 1,
                "cumulative_value": {"$round": ["$cumulative_value", 2]},
                "payment_methods_used": 1,
            }
        },
    ]
    results = [serialize_mongo_document(doc) for doc in col.aggregate(pipeline)]
    return results
