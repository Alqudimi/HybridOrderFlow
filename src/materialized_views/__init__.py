"""Materialized views package with incremental watermark updating."""

from src.materialized_views.manager import (
    DAILY_SALES_MV,
    TOP_PRODUCTS_MV,
    build_initial_materialized_views,
    get_materialized_view_data,
    refresh_materialized_views,
)

__all__ = [
    "DAILY_SALES_MV",
    "TOP_PRODUCTS_MV",
    "build_initial_materialized_views",
    "get_materialized_view_data",
    "refresh_materialized_views",
]
