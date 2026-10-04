from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable
from pymongo.database import Database

from src.analytics.reports import report_orders_by_status, report_sales_by_city
from src.jobs.logger import log_job_execution
from src.materialized_views.manager import refresh_materialized_views

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobDefinition:
    name: str
    description: str
    schedule_interval_seconds: int
    schedule_human: str
    handler: Callable[[Database], tuple[int, dict[str, Any]]]


def _run_refresh_materialized_views(db: Database) -> tuple[int, dict[str, Any]]:
    """Refreshes materialized views incrementally."""
    result = refresh_materialized_views(db, force_rebuild=False)
    processed = result.get("records_processed", 0)
    return processed, result


def _run_daily_sales_report_snapshot(db: Database) -> tuple[int, dict[str, Any]]:
    """Generates an operational reporting snapshot and persists it to reports_archive."""
    now_iso = datetime.now(UTC).isoformat()
    city_report = report_sales_by_city(db, limit=5)
    status_report = report_orders_by_status(db)

    total_orders = sum(item["count"] for item in status_report)
    total_rev = sum(item["total_revenue"] for item in city_report)

    snapshot_doc = {
        "report_type": "operational_sales_snapshot",
        "generated_at": now_iso,
        "total_active_orders": total_orders,
        "top_cities_revenue": round(total_rev, 2),
        "status_breakdown": status_report,
        "top_cities": city_report,
    }

    db["reports_archive"].insert_one(snapshot_doc)
    return total_orders, {
        "snapshot_id": str(snapshot_doc["_id"]),
        "total_orders": total_orders,
        "top_cities_count": len(city_report),
        "generated_at": now_iso,
    }


JOB_REGISTRY: dict[str, JobDefinition] = {
    "refresh_materialized_views_job": JobDefinition(
        name="refresh_materialized_views_job",
        description="Periodically synchronizes Materialized Views with newly ingested orders.",
        schedule_interval_seconds=900,  # Every 15 minutes
        schedule_human="Every 15 minutes",
        handler=_run_refresh_materialized_views,
    ),
    "daily_sales_report_job": JobDefinition(
        name="daily_sales_report_job",
        description="Generates an executive sales & status operational snapshot and archives it in reports_archive.",
        schedule_interval_seconds=3600,  # Every hour
        schedule_human="Hourly (every 60 minutes)",
        handler=_run_daily_sales_report_snapshot,
    ),
}


def run_job_by_name(
    db: Database,
    job_name: str,
    trigger_type: str = "manual",
) -> dict[str, Any]:
    """Executes a registered job with execution metrics and error handling."""
    if job_name not in JOB_REGISTRY:
        available = list(JOB_REGISTRY.keys())
        raise ValueError(f"Unknown job '{job_name}'. Available: {available}")

    job_def = JOB_REGISTRY[job_name]
    started_at = datetime.now(UTC)
    start_perf = time.perf_counter()

    status = "SUCCESS"
    error_msg: str | None = None
    records_processed = 0
    details: dict[str, Any] = {}

    try:
        records_processed, details = job_def.handler(db)
    except Exception as exc:
        status = "FAILED"
        error_msg = f"{type(exc).__name__}: {str(exc)}"
        logger.exception("Scheduled job %s failed", job_name)
    finally:
        finished_at = datetime.now(UTC)
        duration = time.perf_counter() - start_perf
        log_id = log_job_execution(
            db=db,
            job_name=job_name,
            trigger_type=trigger_type,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            status=status,
            records_processed=records_processed,
            details=details,
            error=error_msg,
        )

    return {
        "job_name": job_name,
        "trigger_type": trigger_type,
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(duration, 4),
        "records_processed": records_processed,
        "details": details,
        "error": error_msg,
        "log_id": log_id,
    }
