from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo.database import Database

from src.analytics.catalog import (
    AGGREGATION_CATALOG,
    execute_aggregation,
    list_available_aggregations,
)
from src.api.dependencies import get_db

router = APIRouter(tags=["Aggregation Reports"])


@router.get("/aggregations", summary="List Available Aggregation Reports")
def get_aggregations() -> dict[str, Any]:
    """Returns a list of all available business intelligence aggregation reports."""
    agg_names = list(AGGREGATION_CATALOG.keys())
    return {
        "aggregations": agg_names,
        "catalog": list_available_aggregations(),
    }


@router.get("/aggregations/{name}", summary="Execute Named Aggregation Report")
def get_named_aggregation(
    name: str,
    limit: int | None = Query(None, ge=1, le=1000, description="Optional row limit for ranked reports"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    """Executes a specific aggregation pipeline by name against MongoDB."""
    if name not in AGGREGATION_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"Aggregation '{name}' not found. Available aggregations: {list(AGGREGATION_CATALOG.keys())}",
        )

    passed_params: dict[str, Any] = {}
    if limit is not None:
        passed_params["limit"] = limit

    try:
        return execute_aggregation(db, name=name, parameters=passed_params)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute aggregation '{name}': {type(exc).__name__}: {str(exc)}",
        ) from exc
