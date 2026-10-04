"""Database management, indexing, and performance profiling package."""

from src.database.indexes import (
    PHASE2_INDEXES,
    IndexDefinition,
    create_database_indexes,
    drop_database_indexes,
    get_index_strategy_documentation,
)

__all__ = [
    "IndexDefinition",
    "PHASE2_INDEXES",
    "create_database_indexes",
    "drop_database_indexes",
    "get_index_strategy_documentation",
]
