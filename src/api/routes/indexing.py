from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends
from pymongo.database import Database

from src.api.dependencies import get_db
from src.database.indexes import create_database_indexes, get_index_strategy_documentation

router = APIRouter(tags=["Indexes & Performance"])


@router.post("/indexes", summary="Create or Verify Database Indexes")
def post_indexes(db: Database = Depends(get_db)) -> dict[str, Any]:
    """Idempotently creates all required performance indexes.

    Includes single-field, multikey, and compound (ESR pattern) indexes.
    Avoids unnecessary rebuilds if indexes are already present.
    """
    result = create_database_indexes(db)
    result["strategy_documentation"] = get_index_strategy_documentation()
    return result
