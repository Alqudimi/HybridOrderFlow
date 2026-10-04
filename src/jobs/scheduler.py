from __future__ import annotations

import logging
from typing import Any
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo.database import Database

from src.jobs.logger import get_job_execution_history
from src.jobs.tasks import JOB_REGISTRY, run_job_by_name

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def init_scheduler(db: Database) -> BackgroundScheduler:
    """Initializes and configures the APScheduler background daemon."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler

    scheduler = BackgroundScheduler(daemon=True)

    for job_name, job_def in JOB_REGISTRY.items():
        scheduler.add_job(
            func=run_job_by_name,
            args=[db, job_name, "scheduled"],
            trigger="interval",
            seconds=job_def.schedule_interval_seconds,
            id=job_name,
            name=job_def.name,
            replace_existing=True,
        )
        logger.info(
            "Registered scheduled job '%s' (%s)", job_name, job_def.schedule_human
        )

    _scheduler = scheduler
    return scheduler


def start_scheduler(db: Database) -> None:
    """Starts the background scheduler."""
    scheduler = init_scheduler(db)
    if not scheduler.running:
        scheduler.start()
        logger.info("Background job scheduler started successfully.")


def shutdown_scheduler() -> None:
    """Gracefully terminates background scheduler execution."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Background job scheduler shut down successfully.")
    _scheduler = None


def get_jobs_status(db: Database) -> list[dict[str, Any]]:
    """Returns a list of all registered scheduled jobs with status and last execution."""
    results: list[dict[str, Any]] = []

    for name, job_def in JOB_REGISTRY.items():
        recent_logs = get_job_execution_history(db, job_name=name, limit=1)
        last_run = recent_logs[0] if recent_logs else None

        results.append(
            {
                "name": job_def.name,
                "description": job_def.description,
                "schedule": job_def.schedule_human,
                "interval_seconds": job_def.schedule_interval_seconds,
                "is_active": _scheduler is not None and _scheduler.running,
                "last_execution": last_run,
            }
        )

    return results
