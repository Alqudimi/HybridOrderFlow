from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo.database import Database

from src.api.dependencies import get_db
from src.materialized_views.manager import (
    DAILY_SALES_MV,
    TOP_PRODUCTS_MV,
    get_materialized_view_data,
    refresh_materialized_views,
)

router = APIRouter(tags=["Materialized Views"])


@router.post("/refresh-mv", summary="Incrementally Refresh Materialized Views")
def post_refresh_materialized_views(
    force: bool = Query(False, description="Force a full rebuild from scratch"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    """Refreshes Materialized Views incrementally using watermark delta detection.

    Updates only the affected partitions (dates and products) corresponding to newly ingested records.
    """
    try:
        result = refresh_materialized_views(db, force_rebuild=force)
        return result
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh materialized views: {type(exc).__name__}: {str(exc)}",
        ) from exc


@router.get("/views", summary="List Materialized Views")
def list_materialized_views(db: Database = Depends(get_db)) -> dict[str, Any]:
    """Lists available materialized view collections and document counts."""
    return {
        "materialized_views": [
            {
                "name": DAILY_SALES_MV,
                "description": "Daily aggregated sales, revenues, and order counts.",
                "count": db[DAILY_SALES_MV].count_documents({}),
            },
            {
                "name": TOP_PRODUCTS_MV,
                "description": "Product-level cumulative units sold and revenue totals.",
                "count": db[TOP_PRODUCTS_MV].count_documents({}),
            },
        ]
    }


@router.get("/views/{name}", summary="Fetch Stored Materialized View Data")
def get_view_data(
    name: str,
    limit: int = Query(50, ge=1, le=500),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    """Retrieves cached summary documents from a materialized view collection."""
    try:
        data = get_materialized_view_data(db, view_name=name, limit=limit)
        return {
            "view_name": name,
            "count": len(data),
            "data": data,
        }
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
