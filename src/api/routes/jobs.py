from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo.database import Database

from src.api.dependencies import get_db
from src.jobs.logger import get_job_execution_history
from src.jobs.scheduler import get_jobs_status
from src.jobs.tasks import JOB_REGISTRY, run_job_by_name

router = APIRouter(tags=["Scheduled Jobs"])


@router.get("/jobs", summary="List Scheduled Jobs and Status")
def get_jobs(db: Database = Depends(get_db)) -> dict[str, Any]:
    """Returns all configured scheduled jobs, their intervals, and latest run status."""
    return {
        "total_jobs": len(JOB_REGISTRY),
        "jobs": get_jobs_status(db),
    }


@router.post("/jobs/{name}/run", summary="Trigger Job Manually")
def trigger_job(name: str, db: Database = Depends(get_db)) -> dict[str, Any]:
    """Manually triggers execution of a scheduled job and records execution audit log."""
    if name not in JOB_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{name}' not found. Available jobs: {list(JOB_REGISTRY.keys())}",
        )

    result = run_job_by_name(db, job_name=name, trigger_type="api")
    return result


@router.get("/jobs/{name}/history", summary="Get Execution History for Job")
def get_history(
    name: str,
    limit: int = Query(20, ge=1, le=100),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    """Fetches historical execution audit logs for a given job."""
    if name not in JOB_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{name}' not found. Available jobs: {list(JOB_REGISTRY.keys())}",
        )
    logs = get_job_execution_history(db, job_name=name, limit=limit)
    return {
        "job_name": name,
        "count": len(logs),
        "history": logs,
    }
