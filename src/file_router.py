from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from config.settings import Settings


@dataclass(frozen=True)
class FileInfo:
    path: Path
    size_bytes: int

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)


@dataclass(frozen=True)
class RouteDecision:
    file: FileInfo
    engine: str
    reason: str


def inspect_file(path: str | Path) -> FileInfo:
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {file_path}")
    if file_path.stat().st_size == 0:
        raise ValueError(f"Input CSV is empty: {file_path}")
    return FileInfo(path=file_path, size_bytes=file_path.stat().st_size)


def choose_engine(
    file_info: FileInfo,
    settings: Settings,
    force_engine: str | None = None,
) -> RouteDecision:
    if force_engine is not None:
        if force_engine not in {"python_batch", "pyspark"}:
            raise ValueError("force_engine must be python_batch or pyspark")
        reason = (
            f"engine forced to {force_engine} for demonstration/testing; "
            "normal routing still uses the configured size threshold"
        )
        return RouteDecision(file_info, force_engine, reason)

    if file_info.size_mb <= settings.small_file_threshold_mb:
        return RouteDecision(
            file_info,
            "python_batch",
            (
                f"{file_info.size_mb:.2f} MB <= "
                f"{settings.small_file_threshold_mb:.2f} MB threshold"
            ),
        )
    return RouteDecision(
        file_info,
        "pyspark",
        (
            f"{file_info.size_mb:.2f} MB > "
            f"{settings.small_file_threshold_mb:.2f} MB threshold"
        ),
    )
