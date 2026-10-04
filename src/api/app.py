from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.dependencies import close_mongo_client, get_db
from src.api.routes import (
    analytics_router,
    health_router,
    indexing_router,
    ingestion_router,
    jobs_router,
    mv_router,
    queries_router,
)
from src.jobs.scheduler import shutdown_scheduler, start_scheduler

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages application startup and graceful shutdown."""
    logger.info("Initializing Big Data Final Project API...")
    # Initialize background scheduler with database connection
    db_gen = get_db()
    db = next(db_gen)
    try:
        start_scheduler(db)
    except Exception as exc:
        logger.warning("Could not auto-start background scheduler: %s", exc)

    yield

    logger.info("Shutting down Big Data API...")
    shutdown_scheduler()
    close_mongo_client()


def create_app() -> FastAPI:
    """Factory creating and configuring the unified enterprise FastAPI application."""
    application = FastAPI(
        title="Hybrid Orders Big Data Platform API",
        description=(
            "Unified Big Data API providing access to the ELT Ingestion Pipeline, "
            "Optimized Operational Queries with ESR Indexes, MongoDB Aggregation Reports, "
            "Incremental Materialized Views, and Scheduled Background Jobs."
        ),
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Register Routers
    application.include_router(health_router)
    application.include_router(ingestion_router)
    application.include_router(indexing_router)
    application.include_router(queries_router)
    application.include_router(analytics_router)
    application.include_router(mv_router)
    application.include_router(jobs_router)

    @application.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error_type": type(exc).__name__,
                "detail": str(exc),
                "path": request.url.path,
            },
        )

    @application.get("/", tags=["Root"])
    def root() -> dict[str, str]:
        return {
            "title": "Hybrid Orders Big Data Platform API",
            "version": "2.0.0",
            "documentation": "/docs",
            "health_check": "/health",
        }

    return application


app = create_app()
