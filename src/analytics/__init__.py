"""Analytics and business intelligence package."""

from src.analytics.catalog import (
    AGGREGATION_CATALOG,
    execute_aggregation,
    list_available_aggregations,
)
from src.analytics.reports import (
    report_orders_by_status,
    report_sales_by_city,
    report_sales_by_period,
    report_top_customers,
    report_top_products,
)

__all__ = [
    "AGGREGATION_CATALOG",
    "execute_aggregation",
    "list_available_aggregations",
    "report_orders_by_status",
    "report_sales_by_city",
    "report_sales_by_period",
    "report_top_customers",
    "report_top_products",
]
