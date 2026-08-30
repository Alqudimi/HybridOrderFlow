from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Runtime settings, with environment variables taking precedence."""

    small_file_threshold_mb: float = 200.0
    batch_size: int = 1_000
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_database: str = "orders_pipeline"
    spark_master: str = "local[*]"
    spark_app_name: str = "hybrid-orders-pipeline"
    spark_partitions: int | None = None
    spark_jars: str | None = None
    smart_poll_lease_seconds: int = 300
    results_path: Path = Path("reports/results.json")

    @classmethod
    def from_env(cls) -> "Settings":
        partitions = os.getenv("SPARK_PARTITIONS")
        return cls(
            small_file_threshold_mb=float(
                os.getenv("SMALL_FILE_THRESHOLD_MB", "200")
            ),
            batch_size=int(os.getenv("BATCH_SIZE", "1000")),
            mongo_uri=os.getenv("MONGO_URI", cls.mongo_uri),
            mongo_database=os.getenv("MONGO_DATABASE", cls.mongo_database),
            spark_master=os.getenv("SPARK_MASTER", cls.spark_master),
            spark_app_name=os.getenv("SPARK_APP_NAME", cls.spark_app_name),
            spark_partitions=int(partitions) if partitions else None,
            spark_jars=os.getenv("SPARK_JARS"),
            smart_poll_lease_seconds=int(
                os.getenv("SMART_POLL_LEASE_SECONDS", "300")
            ),
            results_path=Path(
                os.getenv("RESULTS_PATH", "reports/results.json")
            ),
        )

    def validate(self) -> None:
        if self.small_file_threshold_mb <= 0:
            raise ValueError("SMALL_FILE_THRESHOLD_MB must be positive")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")
        if self.spark_partitions is not None and self.spark_partitions <= 0:
            raise ValueError("SPARK_PARTITIONS must be positive")
        if self.smart_poll_lease_seconds <= 0:
            raise ValueError("SMART_POLL_LEASE_SECONDS must be positive")
