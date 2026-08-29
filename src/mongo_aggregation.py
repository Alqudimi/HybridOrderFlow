"""Reusable MongoDB aggregation pipelines for operational reporting."""

from __future__ import annotations

from typing import Any


def quality_error_counts_pipeline(run_id: str | None = None) -> list[dict[str, Any]]:
    match: dict[str, Any] = {}
    if run_id:
        match["run_id"] = run_id
    return [
        {"$match": match},
        {"$unwind": "$error_codes"},
        {
            "$group": {
                "_id": "$error_codes",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "error_code": "$_id",
                "count": 1,
            }
        },
    ]


def validated_status_counts_pipeline() -> list[dict[str, Any]]:
    return [
        {
            "$group": {
                "_id": "$quality_status",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "quality_status": "$_id",
                "count": 1,
            }
        },
    ]


def latest_version_per_order_pipeline(
    version_field: str = "version",
) -> list[dict[str, Any]]:
    """Select the newest version without doing client-side deduplication."""
    return [
        {
            "$sort": {
                "order_id": 1,
                version_field: -1,
                "updated_at": -1,
            }
        },
        {
            "$group": {
                "_id": "$order_id",
                "latest": {"$first": "$$ROOT"},
            }
        },
        {"$replaceWith": "$latest"},
    ]


def run_metrics_summary_pipeline(run_id: str) -> list[dict[str, Any]]:
    return [
        {"$match": {"run_id": run_id}},
        {
            "$group": {
                "_id": None,
                "raw_loaded": {"$sum": 1},
                "quarantine_count": {
                    "$sum": {
                        "$cond": [{"$eq": ["$quality_status", "quarantined"]}, 1, 0]
                    }
                },
            }
        },
        {"$project": {"_id": 0, "raw_loaded": 1, "quarantine_count": 1}},
    ]
