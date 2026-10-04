from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from pymongo.database import Database

from src.queries.repository import (
    get_high_value_orders,
    get_orders_by_city_and_status,
    get_orders_by_customer,
    get_orders_by_payment_status,
    get_orders_containing_item_sku,
)


@dataclass(frozen=True)
class QueryMetadata:
    name: str
    description: str
    target_collection: str
    parameters: dict[str, Any]
    function: Callable[..., list[dict[str, Any]]]


QUERY_CATALOG: dict[str, QueryMetadata] = {
    "orders_by_customer": QueryMetadata(
        name="orders_by_customer",
        description="Finds all orders placed by a specific customer, sorted by order_date descending.",
        target_collection="orders_validated",
        parameters={
            "customer_id": {"type": "str", "default": "عميل-1", "required": False},
            "limit": {"type": "int", "default": 50, "required": False},
        },
        function=get_orders_by_customer,
    ),
    "orders_by_city_and_status": QueryMetadata(
        name="orders_by_city_and_status",
        description="Finds orders for a specific city and status, sorted by order_date descending.",
        target_collection="orders_validated",
        parameters={
            "city": {"type": "str", "default": "صنعاء", "required": False},
            "status": {"type": "str", "default": "قيد الانتظار", "required": False},
            "limit": {"type": "int", "default": 50, "required": False},
        },
        function=get_orders_by_city_and_status,
    ),
    "high_value_orders": QueryMetadata(
        name="high_value_orders",
        description="Filters high-value orders exceeding a threshold, sorted by total_amount descending.",
        target_collection="orders_validated",
        parameters={
            "min_amount": {"type": "float", "default": 500000.0, "required": False},
            "limit": {"type": "int", "default": 50, "required": False},
        },
        function=get_high_value_orders,
    ),
    "orders_by_payment_status": QueryMetadata(
        name="orders_by_payment_status",
        description="Queries orders by payment reconciliation status and optional payment method.",
        target_collection="orders_validated",
        parameters={
            "payment_status": {"type": "str", "default": "بانتظار الدفع", "required": False},
            "payment_method": {"type": "str", "default": None, "required": False},
            "limit": {"type": "int", "default": 50, "required": False},
        },
        function=get_orders_by_payment_status,
    ),
    "orders_containing_item_sku": QueryMetadata(
        name="orders_containing_item_sku",
        description="Queries orders containing a specific product SKU inside items_json array.",
        target_collection="orders_validated",
        parameters={
            "sku": {"type": "str", "default": "SKU-1010", "required": False},
            "limit": {"type": "int", "default": 50, "required": False},
        },
        function=get_orders_containing_item_sku,
    ),
}


def list_available_queries() -> list[dict[str, Any]]:
    """Returns a list of all available queries and their metadata."""
    return [
        {
            "name": meta.name,
            "description": meta.description,
            "target_collection": meta.target_collection,
            "parameters": meta.parameters,
        }
        for meta in QUERY_CATALOG.values()
    ]


def execute_query(
    db: Database,
    query_name: str,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dynamically executes a catalog query with parameter resolution and validation."""
    if query_name not in QUERY_CATALOG:
        available = list(QUERY_CATALOG.keys())
        raise ValueError(f"Unknown query '{query_name}'. Available queries: {available}")

    meta = QUERY_CATALOG[query_name]
    raw_params = parameters or {}
    call_kwargs: dict[str, Any] = {}

    for param_name, param_spec in meta.parameters.items():
        if param_name in raw_params and raw_params[param_name] is not None:
            val = raw_params[param_name]
            expected_type = param_spec["type"]
            if expected_type == "int":
                call_kwargs[param_name] = int(val)
            elif expected_type == "float":
                call_kwargs[param_name] = float(val)
            elif expected_type == "str":
                call_kwargs[param_name] = str(val)
            else:
                call_kwargs[param_name] = val
        else:
            call_kwargs[param_name] = param_spec["default"]

    results = meta.function(db, **call_kwargs)

    return {
        "query": query_name,
        "parameters": call_kwargs,
        "count": len(results),
        "results": results,
    }
