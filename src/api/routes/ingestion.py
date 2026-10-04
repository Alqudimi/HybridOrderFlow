from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from config.settings import Settings
from src.api.dependencies import get_settings
from src.elt_pipeline import run_python_elt
from src.file_router import choose_engine, inspect_file
from src.incremental_loader import load_delta
from src.mongo_setup import create_repository

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ELT Ingestion Pipeline"])


class IngestRequest(BaseModel):
    file_path: str = Field(
        default="data/orders_sample.csv",
        description="Path to dirty CSV data file.",
    )
    incremental: bool = Field(
        default=False,
        description="Process as incremental delta file (Path B).",
    )
    version_field: str = Field(
        default="version",
        description="Watermark version field for incremental updates.",
    )
    force_engine: str | None = Field(
        default=None,
        description="Optional engine override: 'python_batch' or 'pyspark'.",
    )


@router.post("/ingest", summary="Run Unified ELT Ingestion Pipeline")
def run_ingest(
    request: IngestRequest = IngestRequest(),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Executes the existing midterm ELT ingestion pipeline.

    Supports both full batch ingestion and incremental delta loading.
    Strictly uses the existing data quality rules and idempotent repositories.
    """
    path = Path(request.file_path)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Input data file '{request.file_path}' does not exist.",
        )

    file_info = inspect_file(str(path))
    decision = choose_engine(file_info, settings, request.force_engine)
    repository = create_repository(settings)
    run_id = f"api-{uuid.uuid4()}"

    try:
        if request.incremental and decision.engine == "python_batch":
            delta_result = load_delta(
                delta_path=str(path),
                run_id=run_id,
                repository=repository,
                version_field=request.version_field,
                batch_size=settings.batch_size,
            )
            return {
                "status": "success",
                "run_id": run_id,
                "engine_used": "incremental_delta",
                "file_path": str(path),
                "metrics": {
                    "rows_read": delta_result.rows_read,
                    "valid_count": delta_result.valid_count,
                    "corrected_count": delta_result.corrected_count,
                    "quarantine_count": delta_result.quarantine_count,
                    "inserted_count": delta_result.inserted_count,
                    "updated_count": delta_result.updated_count,
                    "unchanged_count": delta_result.unchanged_count,
                    "batches": delta_result.batches,
                },
            }
        else:
            metrics = run_python_elt(
                decision=decision,
                run_id=run_id,
                settings=settings,
                repository=repository,
            )
            return {
                "status": "success",
                "run_id": run_id,
                "engine_used": metrics.engine_used,
                "file_path": str(path),
                "metrics": {
                    "rows_read": metrics.rows_read,
                    "raw_loaded": metrics.raw_loaded,
                    "valid_count": metrics.valid_count,
                    "corrected_count": metrics.corrected_count,
                    "quarantine_count": metrics.quarantine_count,
                    "inserted_count": metrics.inserted_count,
                    "updated_count": metrics.updated_count,
                    "unchanged_count": metrics.unchanged_count,
                    "batches": metrics.batches,
                },
            }
    except Exception as exc:
        logger.exception("Ingestion failed for file %s", request.file_path)
        raise HTTPException(
            status_code=500,
            detail=f"Ingestion pipeline failure: {type(exc).__name__}: {str(exc)}",
        ) from exc
