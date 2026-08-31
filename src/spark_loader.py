"""Large-file Spark path — fully integrated with the project pipeline.

The module is import-safe without Spark installed. A clear runtime error is
raised only when the user explicitly routes a file to PySpark.

Safety guarantees (shared with MongoOrdersRepository):
* Unique Index on ``order_id`` in ``orders_validated`` is ensured before any
  write, so duplicate business records are structurally impossible.
* Version Protection prevents an older record from overwriting a newer one
  using the same atomic ``$cond`` / ``$replaceWith`` pattern as the Python
  Batch path.
* Idempotency keys in ``pipeline_idempotency_keys`` ensure that replaying
  the same ``run_id`` does not produce duplicate side-effects.
* The Stable Business Key is ``order_id``.

Integration points:
* Uses ``OrdersRepository`` for schema setup and dry-run mode.
* Reuses ``as_int``, ``incoming_version_is_not_newer``, and
  ``only_duplicate_key_errors`` from ``repositories`` — no duplicated logic.
* Supports Smart Polling (progress_callback / checkpoint).
* Supports Incremental/Delta loading via ``run_spark_incremental_pipeline``.
* Supports ``--dry-run`` via ``collect()`` + Repository writes on the driver.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config.settings import Settings
from src.batch_loader import RawLoadResult
from src.file_router import RouteDecision
from src.quality_rules import classify_record
from src.repositories import (
    VALIDATED_JSON_SCHEMA,
    OrdersRepository,
    UpsertStats,
)

logger = logging.getLogger(__name__)

CSV_COLUMNS = (
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
)


@dataclass
class SparkRunResult:
    raw_result: RawLoadResult
    valid_count: int
    corrected_count: int
    quarantine_count: int
    partitions: int
    inserted_count: int
    updated_count: int
    unchanged_count: int
    elapsed_seconds: float
    error_case_counts: dict[str, int] = field(default_factory=dict)


def build_spark_session(settings: Settings) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "PySpark is not installed. Install requirements.txt and Java 11+ "
            "before running the large-file route."
        ) from error

    from pathlib import Path

    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config(
            "spark.mongodb.read.connection.uri",
            f"{settings.mongo_uri}/{settings.mongo_database}.orders_raw",
        )
        .config(
            "spark.mongodb.write.connection.uri",
            f"{settings.mongo_uri}/{settings.mongo_database}",
        )
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.driver.memory", settings.spark_driver_memory)
    )
    if settings.spark_jars:
        builder = builder.config("spark.jars", settings.spark_jars)
    else:
        project_root = Path(__file__).resolve().parent.parent
        local_jars = [str(p) for p in (project_root / "jars").glob("*.jar")]
        if local_jars:
            builder = builder.config("spark.jars", ",".join(local_jars))
        else:
            builder = builder.config(
                "spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0",
            )
    spark = builder.getOrCreate()

    # Log cluster connection details for verification.
    sc = spark.sparkContext
    master_url = sc.master
    app_id = sc.applicationId
    logger.info(
        "SparkSession created — master=%s, appId=%s, "
        "executor_memory=%s, driver_memory=%s",
        master_url,
        app_id,
        settings.spark_executor_memory,
        settings.spark_driver_memory,
    )
    if not master_url.startswith("local"):
        try:
            # In cluster mode, log executor count for verification.
            executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
            logger.info("Cluster executors detected: %s", executors)
        except Exception:  # noqa: BLE001
            logger.info("Cluster mode active but executor count unavailable yet")
    return spark


# ---------------------------------------------------------------------------
# Idempotency helpers (mirrors MongoOrdersRepository._claim_request)
# ---------------------------------------------------------------------------


def _payload_hash_from_rdd(rdd: Any) -> str:
    """Deterministic hash over the order_ids in an RDD without driver OOM.

    Computes SHA-256 hashes per partition inside Spark executors, then collects
    only the lightweight partition hashes to the driver and combines them.
    """
    def _hash_partition(rows: Iterator[Any]) -> Iterator[str]:
        order_ids = sorted(
            str(row.get("order_id", ""))
            for row in rows
            if row.get("order_id") is not None
        )
        if order_ids:
            chunk = "\n".join(order_ids).encode("utf-8")
            yield hashlib.sha256(chunk).hexdigest()

    try:
        partition_hashes = rdd.mapPartitions(_hash_partition).collect()
    except Exception:  # noqa: BLE001
        # Fallback if mapPartitions is not supported (e.g. test mock objects)
        order_ids = sorted(
            str(row.get("order_id", ""))
            for row in rdd.collect()
            if row.get("order_id") is not None
        )
        return hashlib.sha256("\n".join(order_ids).encode("utf-8")).hexdigest()

    if not partition_hashes:
        return hashlib.sha256(b"").hexdigest()
    partition_hashes.sort()
    combined = "\n".join(partition_hashes).encode("utf-8")
    return hashlib.sha256(combined).hexdigest()


def _claim_spark_request(
    settings: Settings, request_key: str, payload_hash: str
) -> bool:
    """Claim an idempotency slot. Returns True if this is a new request."""
    from pymongo import MongoClient
    from pymongo.errors import DuplicateKeyError

    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5_000)
    try:
        collection = client[settings.mongo_database].pipeline_idempotency_keys
        try:
            collection.insert_one(
                {
                    "_id": request_key,
                    "payload_hash": payload_hash,
                    "status": "processing",
                    "created_at": datetime.now(UTC),
                }
            )
            return True
        except DuplicateKeyError:
            existing = collection.find_one({"_id": request_key})
            if existing and existing.get("payload_hash") != payload_hash:
                raise ValueError(
                    f"Idempotency key {request_key!r} reused with a "
                    f"different payload"
                )
            return bool(existing and existing.get("status") != "completed")
    finally:
        client.close()


def _complete_spark_request(
    settings: Settings, request_key: str
) -> None:
    """Mark an idempotency slot as completed."""
    from pymongo import MongoClient

    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5_000)
    try:
        client[settings.mongo_database].pipeline_idempotency_keys.update_one(
            {"_id": request_key},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": datetime.now(UTC),
                }
            },
        )
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Version-protected upsert via foreachPartition + PyMongo bulk_write
# ---------------------------------------------------------------------------


def _spark_upsert_validated(
    valid_rdd: Any,
    settings: Settings,
    request_key: str,
    version_field: str = "version",
) -> tuple[int, int, int]:
    """Upsert valid/corrected documents with version protection.

    Uses ``foreachPartition`` so that each Spark partition opens its own
    PyMongo connection and performs an atomic ``bulk_write`` with the same
    ``$cond`` / ``$replaceWith`` guard used by ``MongoOrdersRepository``.

    Returns ``(inserted, updated, unchanged)``.
    """
    payload_hash = _payload_hash_from_rdd(valid_rdd)
    if not _claim_spark_request(settings, request_key, payload_hash):
        count = valid_rdd.count()
        logger.info(
            "Spark validated upsert skipped (idempotency key %s already "
            "completed): %s documents unchanged.",
            request_key,
            count,
        )
        return 0, 0, count

    # Broadcast the settings values needed inside the partition closure.
    mongo_uri = settings.mongo_uri
    mongo_db_name = settings.mongo_database
    vf = version_field

    # Accumulators to gather metrics from each partition.
    sc = valid_rdd.context
    acc_inserted = sc.accumulator(0)
    acc_updated = sc.accumulator(0)
    acc_unchanged = sc.accumulator(0)

    def _upsert_partition(partition: Iterator[dict[str, Any]]) -> None:
        """Run inside each Spark executor — uses shared logic from repositories."""
        from pymongo import MongoClient as _MC
        from pymongo import UpdateOne

        from src import repositories as _repos

        documents = list(partition)
        if not documents:
            return
        client = _MC(mongo_uri, serverSelectionTimeoutMS=5_000)
        try:
            db = client[mongo_db_name]
            coll = db["orders_validated"]

            # Batch query existing documents in chunks to eliminate N+1 query latency.
            order_ids = [
                doc["order_id"] for doc in documents if doc.get("order_id")
            ]
            existing_map: dict[str, dict[str, Any]] = {}
            chunk_size = 1_000
            for i in range(0, len(order_ids), chunk_size):
                sub_keys = order_ids[i : i + chunk_size]
                cursor = coll.find({"order_id": {"$in": sub_keys}})
                for existing_doc in cursor:
                    existing_map[existing_doc["order_id"]] = existing_doc

            operations: list[Any] = []
            local_inserted = 0
            local_updated = 0
            local_unchanged = 0
            for document in documents:
                key = document["order_id"]
                previous = existing_map.get(key)
                if previous is None:
                    local_inserted += 1
                elif _repos.incoming_version_is_not_newer(
                    previous, document, vf
                ) or _docs_equal_ignoring_id(previous, document):
                    local_unchanged += 1
                else:
                    local_updated += 1

                incoming_version = _repos.as_int(document.get(vf))
                if incoming_version is None:
                    version_condition: dict[str, Any] = {
                        "$eq": [{"$ifNull": [f"${vf}", None]}, None]
                    }
                else:
                    version_condition = {
                        "$lte": [
                            {"$ifNull": [f"${vf}", -1]},
                            incoming_version,
                        ]
                    }
                operations.append(
                    UpdateOne(
                        {"order_id": key},
                        [
                            {
                                "$replaceWith": {
                                    "$cond": [
                                        version_condition,
                                        {"$literal": document},
                                        "$$ROOT",
                                    ]
                                }
                            }
                        ],
                        upsert=True,
                    )
                )
            if operations:
                coll.bulk_write(operations, ordered=False)
            acc_inserted.add(local_inserted)
            acc_updated.add(local_updated)
            acc_unchanged.add(local_unchanged)
        finally:
            client.close()

    valid_rdd.foreachPartition(_upsert_partition)
    _complete_spark_request(settings, request_key)

    inserted = acc_inserted.value
    updated = acc_updated.value
    unchanged = acc_unchanged.value
    logger.info(
        "Spark validated upsert complete: inserted=%s updated=%s "
        "unchanged=%s",
        inserted,
        updated,
        unchanged,
    )
    return inserted, updated, unchanged


def _spark_insert_quarantine(
    quarantine_rdd: Any,
    settings: Settings,
    request_key: str,
) -> None:
    """Insert quarantine documents with idempotency protection."""
    payload_hash = _payload_hash_from_rdd(quarantine_rdd)
    if not _claim_spark_request(settings, request_key, payload_hash):
        logger.info(
            "Spark quarantine insert skipped (idempotency key %s already "
            "completed).",
            request_key,
        )
        return

    mongo_uri = settings.mongo_uri
    mongo_db_name = settings.mongo_database
    rkey = request_key

    def _insert_partition(partition: Iterator[dict[str, Any]]) -> None:
        from pymongo import MongoClient as _MC

        from src import repositories as _repos

        documents = list(partition)
        if not documents:
            return
        # Build stable _id values so replays are safe.
        prepared = []
        for index, document in enumerate(documents):
            identity = (
                f"{rkey}|{index}|"
                f"{document.get('run_id', '')}|"
                f"{document.get('source', {}).get('source_row_number', '')}"
            )
            doc = dict(document)
            doc["_id"] = hashlib.sha256(identity.encode("utf-8")).hexdigest()
            prepared.append(doc)
        client = _MC(mongo_uri, serverSelectionTimeoutMS=5_000)
        try:
            db = client[mongo_db_name]
            try:
                db.orders_quarantine.insert_many(prepared, ordered=False)
            except Exception as error:
                if not _repos.only_duplicate_key_errors(error):
                    raise
                logger.info(
                    "Quarantine partition replay contained existing documents"
                )
        finally:
            client.close()

    quarantine_rdd.foreachPartition(_insert_partition)
    _complete_spark_request(settings, request_key)
    logger.info("Spark quarantine insert complete for key %s.", request_key)


# ---------------------------------------------------------------------------
# Dry-run support: write via Repository on the driver (collect-based)
# ---------------------------------------------------------------------------


def _dry_run_upsert_validated(
    valid_rdd: Any,
    repository: OrdersRepository,
    run_id: str,
    version_field: str = "version",
) -> tuple[int, int, int]:
    """Collect valid documents to the driver and upsert via Repository.

    Used for ``--dry-run`` mode where no MongoDB Spark Connector is needed.
    """
    documents = valid_rdd.collect()
    if not documents:
        return 0, 0, 0
    stats: UpsertStats = repository.upsert_validated(
        documents,
        request_key=f"spark:validated:{run_id}",
        version_field=version_field,
    )
    return stats.inserted_count, stats.updated_count, stats.unchanged_count


def _dry_run_insert_quarantine(
    quarantine_rdd: Any,
    repository: OrdersRepository,
    run_id: str,
) -> None:
    """Collect quarantine documents to the driver and insert via Repository."""
    documents = quarantine_rdd.collect()
    if not documents:
        return
    for doc in documents:
        doc["run_id"] = run_id
    repository.insert_quarantine(
        documents,
        request_key=f"spark:quarantine:{run_id}",
    )


# ---------------------------------------------------------------------------
# Partition-level helpers (must be serialisable by Spark)
# ---------------------------------------------------------------------------


def _docs_equal_ignoring_id(
    existing: dict[str, Any], incoming: dict[str, Any]
) -> bool:
    return {
        k: v for k, v in existing.items() if k != "_id"
    } == {
        k: v for k, v in incoming.items() if k != "_id"
    }


# ---------------------------------------------------------------------------
# Main pipeline entry-point (Full Load)
# ---------------------------------------------------------------------------


def run_spark_pipeline(
    decision: RouteDecision,
    run_id: str,
    settings: Settings,
    repository: OrdersRepository | None = None,
    dry_run: bool = False,
    progress_callback: Callable[[int, dict[str, Any]], None] | None = None,
) -> SparkRunResult:
    """Read raw CSV with a fixed all-string schema and process by partitions.

    Parameters
    ----------
    repository
        An ``OrdersRepository`` instance. Required for ``dry_run=True`` and
        used for schema setup in all modes. When ``None`` in non-dry-run
        mode, schema setup falls back to direct PyMongo calls.
    dry_run
        When ``True``, data is collected to the driver and written via
        ``repository`` instead of the MongoDB Spark Connector. Useful for
        tests and demonstrations.
    progress_callback
        Optional callback invoked after processing completes, receiving the
        last source row number and a counters dict. Enables Smart Polling
        integration.

    Safety guarantees:
    * Schema and unique index are ensured before any write occurs.
    * ``_spark_upsert_validated`` performs version-protected upserts via
      ``foreachPartition`` + PyMongo ``bulk_write`` using the same
      ``$cond`` / ``$replaceWith`` guard as the Python Batch path.
    * Idempotency keys prevent duplicate side-effects when the same
      ``run_id`` is replayed.
    """
    started = time.perf_counter()

    # 1. Ensure indexes and schema BEFORE creating SparkSession.
    if repository is not None:
        repository.ensure_schema()
    else:
        _ensure_validated_schema(settings)

    spark = build_spark_session(settings)
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [StructField(column, StringType(), True) for column in CSV_COLUMNS]
        )
        raw_df = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", "\"")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(decision.file.path))
        )
        resolved_source_path = str(Path(decision.file.path).resolve())
        raw_df = (
            raw_df.withColumn("run_id", F.lit(run_id))
            .withColumn("source_file", F.lit(resolved_source_path))
            .withColumn(
                "source_row_number", F.monotonically_increasing_id() + F.lit(2)
            )
            .withColumn("engine_used", F.lit("pyspark"))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn(
                "raw_record",
                F.struct(*[F.col(column) for column in CSV_COLUMNS]),
            )
        )
        partitions = raw_df.rdd.getNumPartitions()
        if settings.spark_partitions:
            logger.info(
                "Using Spark's existing input partitioning (%s); no unreasoned "
                "repartition was added. Configured partitions=%s is advisory.",
                partitions,
                settings.spark_partitions,
            )
        raw_count = raw_df.count()

        # Raw ELT boundary — write raw records before quality classification.
        if dry_run:
            # In dry-run mode, collect and write via repository.
            _dry_run_write_raw(raw_df, repository, run_id)
        else:
            raw_df.write.format("mongodb").mode("append").option(
                "spark.mongodb.write.collection", "orders_raw"
            ).save()

        transformed = raw_df.rdd.mapPartitions(
            lambda rows: _clean_partition(rows, incremental=False)
        ).persist()

        valid_rdd = transformed.filter(
            lambda row: row["quality_status"] != "quarantined"
        )
        quarantine_rdd = transformed.filter(
            lambda row: row["quality_status"] == "quarantined"
        )
        valid_count = valid_rdd.filter(
            lambda row: row["quality_status"] == "valid"
        ).count()
        corrected_count = valid_rdd.filter(
            lambda row: row["quality_status"] == "corrected"
        ).count()
        quarantine_count = quarantine_rdd.count()
        candidate_count = valid_count + corrected_count
        inserted_count = 0
        updated_count = 0
        unchanged_count = 0
        error_case_counts: dict[str, int] = {}
        if quarantine_count:
            error_codes_rdd = quarantine_rdd.flatMap(
                lambda row: row.get("error_codes", [])
            )
            for code, count in error_codes_rdd.countByValue().items():
                error_case_counts[code] = int(count)

        # -- Version-protected upsert into orders_validated ----------------
        if candidate_count:
            if dry_run and repository is not None:
                inserted_count, updated_count, unchanged_count = (
                    _dry_run_upsert_validated(
                        valid_rdd,
                        repository,
                        run_id,
                        version_field="version",
                    )
                )
            else:
                inserted_count, updated_count, unchanged_count = (
                    _spark_upsert_validated(
                        valid_rdd,
                        settings,
                        request_key=f"spark:validated:{run_id}",
                        version_field="version",
                    )
                )

        # -- Idempotent quarantine insert ----------------------------------
        if quarantine_count:
            if dry_run and repository is not None:
                _dry_run_insert_quarantine(quarantine_rdd, repository, run_id)
            else:
                _spark_insert_quarantine(
                    quarantine_rdd,
                    settings,
                    request_key=f"spark:quarantine:{run_id}",
                )

        transformed.unpersist()

        # Report progress for Smart Polling integration.
        if progress_callback is not None:
            progress_callback(
                raw_count,
                {
                    "raw_loaded": raw_count,
                    "valid_count": valid_count,
                    "corrected_count": corrected_count,
                    "quarantine_count": quarantine_count,
                    "inserted_count": inserted_count,
                    "updated_count": updated_count,
                    "unchanged_count": unchanged_count,
                    "error_case_counts": error_case_counts,
                },
            )

        return SparkRunResult(
            raw_result=RawLoadResult(
                rows_read=raw_count,
                raw_loaded=raw_count,
                batches=0,
            ),
            valid_count=valid_count,
            corrected_count=corrected_count,
            quarantine_count=quarantine_count,
            partitions=partitions,
            inserted_count=inserted_count,
            updated_count=updated_count,
            unchanged_count=unchanged_count,
            elapsed_seconds=time.perf_counter() - started,
            error_case_counts=error_case_counts,
        )
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Incremental / Delta pipeline entry-point
# ---------------------------------------------------------------------------


def run_spark_incremental_pipeline(
    decision: RouteDecision,
    run_id: str,
    settings: Settings,
    repository: OrdersRepository | None = None,
    version_field: str = "version",
    dry_run: bool = False,
    progress_callback: Callable[[int, dict[str, Any]], None] | None = None,
) -> SparkRunResult:
    """Incremental delta loading via Spark with version-protected upserts.

    Reads a CSV that includes a ``version`` column and applies
    version-protected upserts, identical to ``incremental_loader.load_delta``
    but distributed across Spark partitions.
    """
    started = time.perf_counter()

    if repository is not None:
        repository.ensure_schema()
    else:
        _ensure_validated_schema(settings)

    spark = build_spark_session(settings)
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        # For incremental, the CSV includes the version column.
        incremental_columns = list(CSV_COLUMNS) + [version_field]
        schema = StructType(
            [StructField(column, StringType(), True) for column in incremental_columns]
        )
        raw_df = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", "\"")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(decision.file.path))
        )
        resolved_source_path = str(Path(decision.file.path).resolve())
        raw_df = (
            raw_df.withColumn("run_id", F.lit(run_id))
            .withColumn("source_file", F.lit(resolved_source_path))
            .withColumn(
                "source_row_number", F.monotonically_increasing_id() + F.lit(2)
            )
            .withColumn("engine_used", F.lit("pyspark"))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn(
                "raw_record",
                F.struct(*[F.col(column) for column in CSV_COLUMNS]),
            )
        )
        partitions = raw_df.rdd.getNumPartitions()
        raw_count = raw_df.count()

        vf = version_field
        transformed = raw_df.rdd.mapPartitions(
            lambda rows: _clean_partition(
                rows,
                incremental=True,
                version_field=vf,
            )
        ).persist()

        valid_rdd = transformed.filter(
            lambda row: row["quality_status"] != "quarantined"
        )
        quarantine_rdd = transformed.filter(
            lambda row: row["quality_status"] == "quarantined"
        )
        valid_count = valid_rdd.filter(
            lambda row: row["quality_status"] == "valid"
        ).count()
        corrected_count = valid_rdd.filter(
            lambda row: row["quality_status"] == "corrected"
        ).count()
        quarantine_count = quarantine_rdd.count()
        candidate_count = valid_count + corrected_count
        inserted_count = 0
        updated_count = 0
        unchanged_count = 0
        error_case_counts: dict[str, int] = {}
        if quarantine_count:
            error_codes_rdd = quarantine_rdd.flatMap(
                lambda row: row.get("error_codes", [])
            )
            for code, count in error_codes_rdd.countByValue().items():
                error_case_counts[code] = int(count)

        if candidate_count:
            if dry_run and repository is not None:
                inserted_count, updated_count, unchanged_count = (
                    _dry_run_upsert_validated(
                        valid_rdd,
                        repository,
                        run_id,
                        version_field=version_field,
                    )
                )
            else:
                inserted_count, updated_count, unchanged_count = (
                    _spark_upsert_validated(
                        valid_rdd,
                        settings,
                        request_key=f"spark:validated:{run_id}",
                        version_field=version_field,
                    )
                )

        if quarantine_count:
            if dry_run and repository is not None:
                _dry_run_insert_quarantine(quarantine_rdd, repository, run_id)
            else:
                _spark_insert_quarantine(
                    quarantine_rdd,
                    settings,
                    request_key=f"spark:quarantine:{run_id}",
                )

        transformed.unpersist()

        if progress_callback is not None:
            progress_callback(
                raw_count,
                {
                    "raw_loaded": 0,
                    "valid_count": valid_count,
                    "corrected_count": corrected_count,
                    "quarantine_count": quarantine_count,
                    "inserted_count": inserted_count,
                    "updated_count": updated_count,
                    "unchanged_count": unchanged_count,
                    "error_case_counts": error_case_counts,
                },
            )

        return SparkRunResult(
            raw_result=RawLoadResult(
                rows_read=raw_count,
                raw_loaded=0,
                batches=0,
            ),
            valid_count=valid_count,
            corrected_count=corrected_count,
            quarantine_count=quarantine_count,
            partitions=partitions,
            inserted_count=inserted_count,
            updated_count=updated_count,
            unchanged_count=unchanged_count,
            elapsed_seconds=time.perf_counter() - started,
            error_case_counts=error_case_counts,
        )
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Dry-run raw write helper
# ---------------------------------------------------------------------------


def _dry_run_write_raw(
    raw_df: Any,
    repository: OrdersRepository | None,
    run_id: str,
) -> None:
    """Collect raw rows to the driver and insert via Repository."""
    if repository is None:
        logger.warning("No repository in dry-run mode; skipping raw write.")
        return
    rows = raw_df.rdd.map(lambda row: row.asDict(recursive=True)).collect()
    batch: list[dict[str, Any]] = []
    for row in rows:
        raw_record = {col: row.get(col) for col in CSV_COLUMNS}
        batch.append(
            {
                "run_id": run_id,
                "source_file": row.get("source_file", ""),
                "source_row_number": row.get("source_row_number", 0),
                "ingested_at": datetime.now(UTC).isoformat(),
                "engine_used": "pyspark",
                "raw_record": raw_record,
            }
        )
    if batch:
        first_row = batch[0]["source_row_number"]
        last_row = batch[-1]["source_row_number"]
        repository.insert_raw_batch(
            batch,
            request_key=f"raw:{run_id}:{first_row}:{last_row}",
        )


# ---------------------------------------------------------------------------
# MongoDB schema & index setup (fallback when no Repository is provided)
# ---------------------------------------------------------------------------


def _ensure_validated_schema(settings: Settings) -> None:
    """Create the Unique Index on order_id and apply Schema Validation.

    This is a fallback for when no ``OrdersRepository`` is provided.
    Prefer ``repository.ensure_schema()`` when a repository is available.
    """
    from pymongo import ASCENDING, MongoClient

    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5_000)
    try:
        db = client[settings.mongo_database]
        existing = set(db.list_collection_names())
        for name in ("orders_raw", "orders_validated", "orders_quarantine"):
            if name not in existing:
                db.create_collection(name)
        db.orders_raw.create_index(
            [("run_id", ASCENDING), ("source_row_number", ASCENDING)]
        )
        db.orders_validated.create_index(
            [("order_id", ASCENDING)], unique=True, name="unique_order_id"
        )
        db.orders_quarantine.create_index(
            [("run_id", ASCENDING), ("source_row_number", ASCENDING)]
        )
        db.pipeline_idempotency_keys.create_index(
            [("created_at", ASCENDING)]
        )
        db.command(
            "collMod",
            "orders_validated",
            validator={"$jsonSchema": VALIDATED_JSON_SCHEMA},
            validationLevel="strict",
            validationAction="error",
        )
        logger.info(
            "Spark path: validated schema, unique index, and idempotency "
            "collection are ready."
        )
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Quality classification (runs inside Spark executors)
# ---------------------------------------------------------------------------


def _clean_partition(
    rows: Iterator[Any],
    incremental: bool = False,
    version_field: str = "version",
) -> Iterator[dict[str, Any]]:
    """Classify each row through quality rules.

    Parameters
    ----------
    rows
        Iterator of Spark Row objects in this partition.
    incremental
        When ``True``, the version column is extracted and included in the
        output document, matching the ``incremental_loader`` behaviour.
    version_field
        The column name used for version tracking.

    Note
    ----
    Duplicate detection via ``seen_order_ids`` is partition-local to avoid an
    expensive distributed shuffle across the cluster. Cross-partition duplicates
    are structurally prevented from creating duplicate records in
    ``orders_validated`` by MongoDB's unique index on ``order_id``.
    """
    seen_order_ids: set[str] = set()
    for row in rows:
        values = row.asDict(recursive=True)
        raw_record = {
            column: values.get(column) for column in CSV_COLUMNS
        }
        # Duplicate detection within the same file/partition.
        order_id = str(raw_record.get("order_id", "") or "").strip()
        is_duplicate = bool(order_id and order_id in seen_order_ids)
        if order_id:
            seen_order_ids.add(order_id)

        result = classify_record(raw_record, is_duplicate)
        result["run_id"] = values.get("run_id")
        result["source"] = {
            "source_file": values.get("source_file"),
            "source_row_number": values.get("source_row_number"),
            "engine_used": "pyspark",
            "incremental": incremental,
            "version_field": version_field,
        }
        # Version handling.
        if result["quality_status"] != "quarantined":
            if incremental:
                # Extract version from the row for incremental delta.
                raw_version = values.get(version_field)
                try:
                    result[version_field] = int(raw_version or 0)
                except (TypeError, ValueError):
                    result[version_field] = 0
            else:
                # Full-load path: version=1 is the baseline.
                result.setdefault("version", 1)
        yield result
