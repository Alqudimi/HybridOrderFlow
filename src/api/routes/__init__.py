"""API route modules."""

from src.api.routes.analytics import router as analytics_router
from src.api.routes.health import router as health_router
from src.api.routes.indexing import router as indexing_router
from src.api.routes.ingestion import router as ingestion_router
from src.api.routes.jobs import router as jobs_router
from src.api.routes.materialized_views import router as mv_router
from src.api.routes.queries import router as queries_router

__all__ = [
    "analytics_router",
    "health_router",
    "indexing_router",
    "ingestion_router",
    "jobs_router",
    "mv_router",
    "queries_router",
]
