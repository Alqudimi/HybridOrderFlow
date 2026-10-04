"""Queries package for operational and analytical data access."""

from src.queries.catalog import (
    QUERY_CATALOG,
    execute_query,
    list_available_queries,
)
from src.queries.repository import (
    get_high_value_orders,
    get_orders_by_city_and_status,
    get_orders_by_customer,
    get_orders_by_payment_status,
    get_orders_containing_item_sku,
)

__all__ = [
    "QUERY_CATALOG",
    "execute_query",
    "get_high_value_orders",
    "get_orders_by_city_and_status",
    "get_orders_by_customer",
    "get_orders_by_payment_status",
    "get_orders_containing_item_sku",
    "list_available_queries",
]
