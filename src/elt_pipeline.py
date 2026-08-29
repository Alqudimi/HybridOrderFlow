from __future__ import annotations

import logging
import time
from collections import Counter
from typing import Any, Callable

from config.settings import Settings
from src.batch_loader import load_raw_streaming
from src.file_router import RouteDecision
from src.metrics import RunMetrics, write_results
from src.quality_rules import classify_record
from src.repositories import OrdersRepository, UpsertStats

logger = logging.getLogger(__name__)


def run_python_elt(
    decision: RouteDecision,
    run_id: str,
    settings: Settings,
    repository: OrdersRepository,
    resume_after_source_row: int | None = None,
    checkpoint: dict[str, Any] | None = None,
    progress_callback: Callable[[int, dict[str, Any]], None] | None = None,
) -> RunMetrics:
    """Run ELT with idempotent batches and optional row-level resumption.

    Raw loading always runs first. Its deterministic batch keys make a retry
    safe, while ``resume_after_source_row`` prevents quality rules from being
    applied again to rows already checkpointed successfully.
    """
    started = time.perf_counter()
    previous = checkpoint or {}
    metrics = RunMetrics(
        run_id=run_id,
        file_name=decision.file.path.name,
        file_size_mb=round(decision.file.size_mb, 6),
        engine_used="python_batch",
        batch_size=settings.batch_size,
        valid_count=int(previous.get("valid_count", 0)),
        corrected_count=int(previous.get("corrected_count", 0)),
        quarantine_count=int(previous.get("quarantine_count", 0)),
        inserted_count=int(previous.get("inserted_count", 0)),
        updated_count=int(previous.get("updated_count", 0)),
        unchanged_count=int(previous.get("unchanged_count", 0)),
    )
    metrics.error_case_counts.update(previous.get("error_case_counts", {}))
    repository.ensure_schema()

    # Critical ELT invariant: this call finishes before iter_raw is consumed.
    raw_result = load_raw_streaming(
        decision.file.path,
        run_id,
        settings,
        repository,
        engine_used="python_batch",
    )
    metrics.rows_read = raw_result.rows_read
    metrics.raw_loaded = max(
        raw_result.raw_loaded,
        int(previous.get("raw_loaded", 0)),
        raw_result.rows_read,
    )
    metrics.batches = raw_result.batches
    metrics.batch_timings = raw_result.batch_timings
    metrics.batch_errors = raw_result.batch_errors

    seen_order_ids: set[str] = set()
    if resume_after_source_row is not None:
        # Rebuild only the duplicate-key index, not the quality results. This
        # preserves duplicate classification while avoiding a full transform.
        for raw_document in repository.iter_raw(run_id):
            if raw_document["source_row_number"] <= resume_after_source_row:
                order_id = str(raw_document["raw_record"].get("order_id", "")).strip()
                if order_id:
                    seen_order_ids.add(order_id)

    valid_batch: list[dict[str, Any]] = []
    quarantine_batch: list[dict[str, Any]] = []
    batch_start_row: int | None = None
    batch_end_row: int | None = None

    def flush_processing_batch() -> None:
        nonlocal batch_start_row, batch_end_row, valid_batch, quarantine_batch
        if batch_start_row is None or batch_end_row is None:
            return
        if valid_batch:
            _apply_upserts(
                repository,
                valid_batch,
                metrics,
                request_key=(
                    f"validated:{run_id}:{batch_start_row}:{batch_end_row}"
                ),
            )
        if quarantine_batch:
            for document in quarantine_batch:
                document["run_id"] = run_id
            repository.insert_quarantine(
                quarantine_batch,
                request_key=(
                    f"quarantine:{run_id}:{batch_start_row}:{batch_end_row}"
                ),
            )
        if progress_callback is not None:
            progress_callback(
                batch_end_row,
                {
                    "raw_loaded": metrics.raw_loaded,
                    "valid_count": metrics.valid_count,
                    "corrected_count": metrics.corrected_count,
                    "quarantine_count": metrics.quarantine_count,
                    "inserted_count": metrics.inserted_count,
                    "updated_count": metrics.updated_count,
                    "unchanged_count": metrics.unchanged_count,
                    "error_case_counts": dict(metrics.error_case_counts),
                },
            )
        batch_start_row = None
        batch_end_row = None
        valid_batch = []
        quarantine_batch = []

    for raw_document in repository.iter_raw(run_id):
        source_row_number = int(raw_document["source_row_number"])
        if (
            resume_after_source_row is not None
            and source_row_number <= resume_after_source_row
        ):
            continue
        raw_record = raw_document["raw_record"]
        order_id = str(raw_record.get("order_id", "")).strip()
        is_duplicate = bool(order_id and order_id in seen_order_ids)
        if order_id:
            seen_order_ids.add(order_id)
        final_document = classify_record(raw_record, is_duplicate)
        # Run-specific lineage remains in orders_raw. The final business
        # document intentionally contains only stable source facts, so replaying
        # the same input does not mutate its terminal state.
        final_document["source"] = {
            "source_file": raw_document["source_file"],
            "source_row_number": raw_document["source_row_number"],
            "engine_used": raw_document["engine_used"],
        }
        status = final_document["quality_status"]
        if status == "quarantined":
            quarantine_batch.append(final_document)
            metrics.quarantine_count += 1
            metrics.error_case_counts.update(final_document["error_codes"])
        elif status == "corrected":
            valid_batch.append(final_document)
            metrics.corrected_count += 1
        else:
            valid_batch.append(final_document)
            metrics.valid_count += 1
        if batch_start_row is None:
            batch_start_row = source_row_number
        batch_end_row = source_row_number
        if len(valid_batch) + len(quarantine_batch) >= settings.batch_size:
            flush_processing_batch()

    flush_processing_batch()
    metrics.finish(time.perf_counter() - started)
    write_results(settings.results_path, metrics)
    logger.info(
        "Run %s finished: %s read, %s valid, %s corrected, %s quarantined",
        run_id,
        metrics.rows_read,
        metrics.valid_count,
        metrics.corrected_count,
        metrics.quarantine_count,
    )
    return metrics


def _apply_upserts(
    repository: OrdersRepository,
    documents: list[dict[str, Any]],
    metrics: RunMetrics,
    request_key: str | None = None,
) -> None:
    stats: UpsertStats = repository.upsert_validated(
        documents, request_key=request_key
    )
    metrics.inserted_count += stats.inserted_count
    metrics.updated_count += stats.updated_count
    metrics.unchanged_count += stats.unchanged_count
