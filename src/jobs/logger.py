from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from pymongo.database import Database
from src.utils.serialization import serialize_mongo_document

logger = logging.getLogger(__name__)

JOB_LOGS_COLLECTION = "job_execution_logs"


def log_job_execution(
    db: Database,
    job_name: str,
    trigger_type: str,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    status: str,
    records_processed: int = 0,
    details: dict[str, Any] | None = None,
    error: str | None = None,
) -> str:
    """Inserts a durable execution log entry into MongoDB."""
    entry = {
        "job_name": job_name,
        "trigger_type": trigger_type,  # 'scheduled' | 'manual' | 'api'
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(duration_seconds, 4),
        "status": status,  # 'SUCCESS' | 'FAILED'
        "records_processed": records_processed,
        "details": details or {},
        "error": error,
        "created_at": datetime.now(UTC).isoformat(),
    }
    result = db[JOB_LOGS_COLLECTION].insert_one(entry)
    return str(result.inserted_id)


def get_job_execution_history(
    db: Database,
    job_name: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieves recent job execution audit records."""
    query = {"job_name": job_name} if job_name else {}
    cursor = db[JOB_LOGS_COLLECTION].find(query).sort("started_at", -1).limit(limit)
    return [serialize_mongo_document(doc) for doc in cursor]
