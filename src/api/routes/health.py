from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends
from pymongo.database import Database
from src.api.dependencies import get_db

router = APIRouter(tags=["Health & Diagnostics"])


@router.get("/health", summary="Service Health & Database Status")
def get_health(db: Database = Depends(get_db)) -> dict[str, Any]:
    """Verifies that FastAPI is running and connected to MongoDB."""
    try:
        server_info = db.client.server_info()
        collections = db.list_collection_names()
        counts = {col: db[col].estimated_document_count() for col in collections}

        return {
            "status": "ok",
            "database": {
                "connected": True,
                "database_name": db.name,
                "mongodb_version": server_info.get("version"),
                "collections_count": len(collections),
                "collections": counts,
            },
        }
    except Exception as exc:
        return {
            "status": "degraded",
            "database": {
                "connected": False,
                "error": type(exc).__name__,
            },
        }
