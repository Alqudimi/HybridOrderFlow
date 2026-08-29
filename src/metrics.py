from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class RunMetrics:
    run_id: str
    file_name: str
    file_size_mb: float
    engine_used: str
    rows_read: int = 0
    raw_loaded: int = 0
    valid_count: int = 0
    corrected_count: int = 0
    quarantine_count: int = 0
    elapsed_seconds: float = 0.0
    throughput: float = 0.0
    batch_size: int | None = None
    partitions: int | None = None
    batches: int = 0
    batch_timings: list[dict[str, Any]] = field(default_factory=list)
    batch_errors: list[dict[str, str]] = field(default_factory=list)
    error_case_counts: Counter[str] = field(default_factory=Counter)
    inserted_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    consistency_passed: bool = False
    consistency_equation: str = ""
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def finish(self, elapsed_seconds: float) -> None:
        self.elapsed_seconds = round(max(elapsed_seconds, 0.000001), 6)
        self.throughput = round(self.rows_read / self.elapsed_seconds, 3)
        self.error_case_counts = Counter(self.error_case_counts)
        self.consistency_passed = self.raw_loaded == (
            self.valid_count + self.corrected_count + self.quarantine_count
        )
        self.consistency_equation = (
            f"{self.raw_loaded} = {self.valid_count} + "
            f"{self.corrected_count} + {self.quarantine_count}"
        )

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["error_case_counts"] = dict(self.error_case_counts)
        result["config"] = {
            "batch_size": self.batch_size,
            "partitions": self.partitions,
        }
        result.pop("batch_size", None)
        result.pop("partitions", None)
        return result


def write_results(path: str | Path, metrics: RunMetrics) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    current = metrics.to_dict()
    history: list[dict[str, Any]] = []
    if output_path.exists():
        try:
            previous = json.loads(output_path.read_text(encoding="utf-8"))
            history = previous.get("runs", []) if isinstance(previous, dict) else []
        except json.JSONDecodeError:
            history = []
    history = [*history, current]
    output_path.write_text(
        json.dumps(
            {
                "last_run": current,
                "runs": history,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
