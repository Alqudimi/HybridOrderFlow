from __future__ import annotations

import csv
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import Settings
from src.repositories import OrdersRepository

logger = logging.getLogger(__name__)


@dataclass
class RawLoadResult:
    rows_read: int = 0
    raw_loaded: int = 0
    batches: int = 0
    batch_timings: list[dict[str, Any]] = field(default_factory=list)
    batch_errors: list[dict[str, str]] = field(default_factory=list)


def stream_csv_rows(path: str | Path) -> Iterator[tuple[int, dict[str, str]]]:
    """Yield one row at a time; never materializes the CSV in memory."""
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV must contain a header row")
        for source_row_number, row in enumerate(reader, start=2):
            if None in row:
                raise ValueError(
                    f"CSV row {source_row_number} has more columns than its header"
                )
            yield source_row_number, {
                key: value if value is not None else "" for key, value in row.items()
            }


def build_raw_document(
    raw_record: dict[str, str],
    run_id: str,
    source_file: str,
    source_row_number: int,
    engine_used: str,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "source_file": source_file,
        "source_row_number": source_row_number,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "engine_used": engine_used,
        "raw_record": dict(raw_record),
    }


def load_raw_streaming(
    path: str | Path,
    run_id: str,
    settings: Settings,
    repository: OrdersRepository,
    engine_used: str = "python_batch",
) -> RawLoadResult:
    """Stage 1: stream CSV rows into Raw before any quality function runs."""
    resolved_path = str(Path(path).resolve())
    result = RawLoadResult()
    batch: list[dict[str, Any]] = []
    for source_row_number, raw_record in stream_csv_rows(path):
        result.rows_read += 1
        batch.append(
            build_raw_document(
                raw_record,
                run_id,
                resolved_path,
                source_row_number,
                engine_used,
            )
        )
        if len(batch) >= settings.batch_size:
            _flush_batch(repository, batch, result)
            batch = []
    if batch:
        _flush_batch(repository, batch, result)
    return result


def _flush_batch(
    repository: OrdersRepository,
    batch: list[dict[str, Any]],
    result: RawLoadResult,
) -> None:
    batch_number = result.batches + 1
    started = time.perf_counter()
    first_row = batch[0]["source_row_number"]
    last_row = batch[-1]["source_row_number"]
    request_key = (
        f"raw:{batch[0]['run_id']}:{first_row}:{last_row}"
    )
    try:
        accepted = repository.insert_raw_batch(batch, request_key=request_key)
    except Exception as error:
        detail = {
            "batch_number": str(batch_number),
            "error": f"{type(error).__name__}: {error}",
        }
        result.batch_errors.append(detail)
        logger.exception("Raw batch %s failed", batch_number)
        raise
    elapsed = time.perf_counter() - started
    result.batches += 1
    if accepted:
        result.raw_loaded += len(batch)
    result.batch_timings.append(
        {
            "batch_number": batch_number,
            "records": len(batch),
            "elapsed_seconds": round(elapsed, 6),
            "records_per_second": round(len(batch) / max(elapsed, 0.000001), 3),
        }
    )
    logger.info(
        "Raw batch %s loaded: %s records in %.3fs (%.1f records/s)",
        batch_number,
        len(batch),
        elapsed,
        len(batch) / max(elapsed, 0.000001),
    )
