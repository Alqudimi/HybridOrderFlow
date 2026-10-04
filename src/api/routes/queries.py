from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo.database import Database

from src.api.dependencies import get_db
from src.queries.catalog import (
    QUERY_CATALOG,
    execute_query,
    list_available_queries,
)

router = APIRouter(tags=["Operational Queries"])


@router.get("/queries", summary="List Available Queries")
def get_queries() -> dict[str, Any]:
    """Returns a list of all available operational queries."""
    query_names = list(QUERY_CATALOG.keys())
    return {
        "queries": query_names,
        "catalog": list_available_queries(),
    }


@router.get("/queries/{name}", summary="Execute Named Query")
def get_named_query(
    name: str,
    customer_id: str | None = Query(None, description="Customer identifier (e.g. عميل-1)"),
    city: str | None = Query(None, description="City / Governorate (e.g. صنعاء, عدن)"),
    status: str | None = Query(None, description="Order status (e.g. قيد الانتظار, مؤكد)"),
    sku: str | None = Query(None, description="Product SKU (e.g. SKU-1010)"),
    min_amount: float | None = Query(None, description="Minimum order amount threshold"),
    payment_status: str | None = Query(None, description="Payment status (e.g. بانتظار الدفع, تم الدفع)"),
    payment_method: str | None = Query(None, description="Payment method filter"),
    limit: int = Query(50, ge=1, le=500, description="Max documents to return"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    """Executes a specific query by name against MongoDB."""
    if name not in QUERY_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"Query '{name}' not found. Available queries: {list(QUERY_CATALOG.keys())}",
        )

    # Filter out None parameters to allow catalog defaults to apply
    passed_params: dict[str, Any] = {"limit": limit}
    if customer_id is not None:
        passed_params["customer_id"] = customer_id
    if city is not None:
        passed_params["city"] = city
    if status is not None:
        passed_params["status"] = status
    if sku is not None:
        passed_params["sku"] = sku
    if min_amount is not None:
        passed_params["min_amount"] = min_amount
    if payment_status is not None:
        passed_params["payment_status"] = payment_status
    if payment_method is not None:
        passed_params["payment_method"] = payment_method

    try:
        return execute_query(db, query_name=name, parameters=passed_params)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute query '{name}': {type(exc).__name__}: {str(exc)}",
        ) from exc
