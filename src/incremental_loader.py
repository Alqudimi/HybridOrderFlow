from __future__ import annotations

import csv
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.quality_rules import classify_record
from src.repositories import OrdersRepository, UpsertStats


@dataclass
class DeltaLoadResult(UpsertStats):
    rows_read: int = 0
    valid_count: int = 0
    corrected_count: int = 0
    quarantine_count: int = 0
    batches: int = 0
    error_case_counts: Counter[str] = field(default_factory=Counter)


def load_delta(
    delta_path: str | Path,
    run_id: str,
    repository: OrdersRepository,
    version_field: str = "version",
    batch_size: int = 1_000,
) -> DeltaLoadResult:
    """Path B: apply only new/changed delta rows using version handling.

    The business document is replaced only when its incoming version is newer
    than the stored version. Replaying the same delta therefore has no effect.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    path = Path(delta_path)
    totals = DeltaLoadResult()
    valid_batch: list[dict[str, Any]] = []
    quarantine_batch: list[dict[str, Any]] = []
    batch_start_row: int | None = None
    batch_end_row: int | None = None

    def flush() -> None:
        nonlocal batch_start_row, batch_end_row, valid_batch, quarantine_batch
        if batch_start_row is None or batch_end_row is None:
            return
        if valid_batch:
            written = repository.upsert_validated(
                valid_batch,
                request_key=(
                    f"validated:delta:{path.resolve()}:{batch_start_row}:"
                    f"{batch_end_row}"
                ),
                version_field=version_field,
            )
            _add_stats(totals, written)
        totals.batches += 1
        if quarantine_batch:
            repository.insert_quarantine(
                quarantine_batch,
                request_key=(
                    f"quarantine:delta:{path.resolve()}:{batch_start_row}:"
                    f"{batch_end_row}"
                ),
            )
        valid_batch = []
        quarantine_batch = []
        batch_start_row = None
        batch_end_row = None

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for source_row_number, row in enumerate(csv.DictReader(handle), start=2):
            totals.rows_read += 1
            cleaned = classify_record(row)
            if cleaned["quality_status"] == "quarantined":
                totals.quarantine_count += 1
                totals.error_case_counts.update(cleaned["error_codes"])
                quarantine_batch.append(
                    {
                        **cleaned,
                        "run_id": run_id,
                        "source_file": str(delta_path),
                        "source_row_number": source_row_number,
                        "version_field": version_field,
                    }
                )
                batch_start_row = (
                    source_row_number
                    if batch_start_row is None
                    else batch_start_row
                )
                batch_end_row = source_row_number
                if len(valid_batch) + len(quarantine_batch) >= batch_size:
                    flush()
                continue
            cleaned[version_field] = _version(row.get(version_field))
            # Keep the terminal business document stable across a replay. The
            # run id remains in the quarantine/history records and metrics.
            cleaned["source"] = {
                "source_file": str(delta_path),
                "source_row_number": source_row_number,
                "incremental": True,
                "version_field": version_field,
            }
            valid_batch.append(cleaned)
            if cleaned["quality_status"] == "corrected":
                totals.corrected_count += 1
            else:
                totals.valid_count += 1
            batch_start_row = (
                source_row_number if batch_start_row is None else batch_start_row
            )
            batch_end_row = source_row_number
            if len(valid_batch) + len(quarantine_batch) >= batch_size:
                flush()
    flush()
    return totals


def _versioned_upsert(
    repository: OrdersRepository, documents: Iterable[dict[str, Any]]
) -> UpsertStats:
    # The repository performs the version comparison as part of its atomic
    # MongoDB update, avoiding a check-then-write race.
    stats = repository.upsert_validated(documents, version_field="version")
    return stats


def _add_stats(target: UpsertStats, source: UpsertStats) -> None:
    target.inserted_count += source.inserted_count
    target.updated_count += source.updated_count
    target.unchanged_count += source.unchanged_count


def _version(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
