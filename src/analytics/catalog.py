from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from pymongo.database import Database

from src.analytics.reports import (
    report_orders_by_status,
    report_sales_by_city,
    report_sales_by_period,
    report_top_customers,
    report_top_products,
)


@dataclass(frozen=True)
class AggregationMetadata:
    name: str
    description: str
    target_collection: str
    parameters: dict[str, Any]
    function: Callable[..., list[dict[str, Any]]]


AGGREGATION_CATALOG: dict[str, AggregationMetadata] = {
    "sales_by_city": AggregationMetadata(
        name="sales_by_city",
        description="Aggregates sales revenue, order volumes, average order value, and delivered counts by city.",
        target_collection="orders_validated",
        parameters={
            "limit": {"type": "int", "default": 20, "required": False},
        },
        function=report_sales_by_city,
    ),
    "top_products": AggregationMetadata(
        name="top_products",
        description="Ranks top-selling products by total revenue and unit sales from nested order items.",
        target_collection="orders_validated",
        parameters={
            "limit": {"type": "int", "default": 10, "required": False},
        },
        function=report_top_products,
    ),
    "top_customers": AggregationMetadata(
        name="top_customers",
        description="Ranks highest lifetime-value (LTV) customers by cumulative spend and order counts.",
        target_collection="orders_validated",
        parameters={
            "limit": {"type": "int", "default": 10, "required": False},
        },
        function=report_top_customers,
    ),
    "sales_by_period": AggregationMetadata(
        name="sales_by_period",
        description="Chronological sales trends aggregated by month (YYYY-MM).",
        target_collection="orders_validated",
        parameters={},
        function=report_sales_by_period,
    ),
    "orders_by_status": AggregationMetadata(
        name="orders_by_status",
        description="Distribution of orders and total values across workflow statuses.",
        target_collection="orders_validated",
        parameters={},
        function=report_orders_by_status,
    ),
}


def list_available_aggregations() -> list[dict[str, Any]]:
    """Returns a list of available aggregation reports and their configuration."""
    return [
        {
            "name": meta.name,
            "description": meta.description,
            "target_collection": meta.target_collection,
            "parameters": meta.parameters,
        }
        for meta in AGGREGATION_CATALOG.values()
    ]


def execute_aggregation(
    db: Database,
    name: str,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dynamically executes an aggregation report with parameter validation."""
    if name not in AGGREGATION_CATALOG:
        available = list(AGGREGATION_CATALOG.keys())
        raise ValueError(f"Unknown aggregation report '{name}'. Available: {available}")

    meta = AGGREGATION_CATALOG[name]
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
        "aggregation": name,
        "parameters": call_kwargs,
        "count": len(results),
        "results": results,
    }
