"""Large-file Spark path.

The module is import-safe without Spark installed. A clear runtime error is
raised only when the user explicitly routes a file to PySpark.

Safety guarantees (aligned with MongoOrdersRepository):
* Unique Index on ``order_id`` in ``orders_validated`` is ensured before any
  write, so duplicate business records are structurally impossible.
* Version Protection prevents an older record from overwriting a newer one
  using the same atomic ``$cond`` / ``$replaceWith`` pattern as the Python
  Batch path.
* Idempotency keys in ``pipeline_idempotency_keys`` ensure that replaying
  the same ``run_id`` does not produce duplicate side-effects.
* The Stable Business Key is ``order_id``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from config.settings import Settings
from src.batch_loader import RawLoadResult
from src.file_router import RouteDecision
from src.quality_rules import classify_record
from src.repositories import VALIDATED_JSON_SCHEMA, _as_int

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
    error_case_counts: dict[str, int] = None


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
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# MongoDB schema & index setup (called once before any Spark write)
# ---------------------------------------------------------------------------


def _ensure_validated_schema(settings: Settings) -> None:
    """Create the Unique Index on order_id and apply Schema Validation.

    This mirrors ``MongoOrdersRepository.ensure_schema`` but is callable
    without a full repository instance, keeping the Spark path self-contained.
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
# Idempotency helpers (mirrors MongoOrdersRepository._claim_request)
# ---------------------------------------------------------------------------


def _payload_hash_from_rdd(rdd: Any) -> str:
    """Deterministic hash over the sorted order_ids in an RDD.

    A full document hash is infeasible at Spark scale, so the hash is built
    from the stable business keys only. This is sufficient to detect a
    replayed run_id (same keys → same hash).
    """
    order_ids = sorted(
        rdd.map(lambda row: str(row.get("order_id", ""))).distinct().collect()
    )
    encoded = json.dumps(order_ids, sort_keys=True, ensure_ascii=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


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
                    "created_at": datetime.now(timezone.utc),
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
                    "completed_at": datetime.now(timezone.utc),
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
    # NOTE: We cannot import SparkContext at module level (import-safe), so
    # we obtain it from the RDD's context.
    sc = valid_rdd.context
    acc_inserted = sc.accumulator(0)
    acc_updated = sc.accumulator(0)
    acc_unchanged = sc.accumulator(0)

    def _upsert_partition(partition: Iterator[dict[str, Any]]) -> None:
        """Run inside each Spark executor."""
        from pymongo import MongoClient as _MC
        from pymongo import UpdateOne

        documents = list(partition)
        if not documents:
            return
        client = _MC(mongo_uri, serverSelectionTimeoutMS=5_000)
        try:
            db = client[mongo_db_name]
            coll = db["orders_validated"]
            operations: list[Any] = []
            local_inserted = 0
            local_updated = 0
            local_unchanged = 0
            for document in documents:
                key = document["order_id"]
                previous = coll.find_one({"order_id": key})
                if previous is None:
                    local_inserted += 1
                elif _version_is_not_newer(previous, document, vf):
                    local_unchanged += 1
                elif _docs_equal_ignoring_id(previous, document):
                    local_unchanged += 1
                else:
                    local_updated += 1

                incoming_version = _safe_int(document.get(vf))
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
                if not _only_dup_key(error):
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
# Partition-level helpers (must be serialisable by Spark)
# ---------------------------------------------------------------------------


def _version_is_not_newer(
    existing: dict[str, Any], incoming: dict[str, Any], version_field: str
) -> bool:
    """Same logic as ``repositories._incoming_version_is_not_newer``."""
    incoming_version = _safe_int(incoming.get(version_field))
    existing_version = _safe_int(existing.get(version_field))
    if incoming_version is None and existing_version is not None:
        return True
    return (
        incoming_version is not None
        and existing_version is not None
        and incoming_version <= existing_version
    )


def _safe_int(value: Any) -> int | None:
    """Same logic as ``repositories._as_int``."""
    try:
        return int(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _docs_equal_ignoring_id(
    existing: dict[str, Any], incoming: dict[str, Any]
) -> bool:
    return {
        k: v for k, v in existing.items() if k != "_id"
    } == {
        k: v for k, v in incoming.items() if k != "_id"
    }


def _only_dup_key(error: Exception) -> bool:
    details = getattr(error, "details", None)
    write_errors = (
        details.get("writeErrors", []) if isinstance(details, dict) else []
    )
    return bool(write_errors) and all(
        entry.get("code") == 11000 for entry in write_errors
    )


# ---------------------------------------------------------------------------
# Main pipeline entry-point
# ---------------------------------------------------------------------------


def run_spark_pipeline(
    decision: RouteDecision,
    run_id: str,
    settings: Settings,
) -> SparkRunResult:
    """Read raw CSV with a fixed all-string schema and process by partitions.

    Safety guarantees:
    * ``_ensure_validated_schema`` creates the Unique Index on ``order_id``
      and applies JSON Schema Validation before any write occurs.
    * ``_spark_upsert_validated`` performs version-protected upserts via
      ``foreachPartition`` + PyMongo ``bulk_write`` using the same
      ``$cond`` / ``$replaceWith`` guard as the Python Batch path.
    * Idempotency keys prevent duplicate side-effects when the same
      ``run_id`` is replayed.
    """
    started = time.perf_counter()

    # 1. Ensure indexes and schema BEFORE creating SparkSession, so that the
    #    Unique Index is in place even if Spark fails mid-flight.
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

        # Connector write is the raw ELT boundary and happens before quality.
        raw_df.write.format("mongodb").mode("append").option(
            "spark.mongodb.write.collection", "orders_raw"
        ).save()

        transformed = raw_df.rdd.mapPartitions(_clean_partition).persist()
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
            _spark_insert_quarantine(
                quarantine_rdd,
                settings,
                request_key=f"spark:quarantine:{run_id}",
            )

        transformed.unpersist()
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
# Quality classification (runs inside Spark executors)
# ---------------------------------------------------------------------------


def _clean_partition(rows: Iterator[Any]) -> Iterator[dict[str, Any]]:
    for row in rows:
        values = row.asDict(recursive=True)
        raw_record = {
            column: values.get(column) for column in CSV_COLUMNS
        }
        result = classify_record(raw_record)
        result["run_id"] = values.get("run_id")
        result["source"] = {
            "source_file": values.get("source_file"),
            "source_row_number": values.get("source_row_number"),
            "engine_used": "pyspark",
        }
        # Add a default version for version-protected upsert. The Python
        # Batch path injects this via the incremental_loader; for the Spark
        # full-load path version=1 is the baseline.
        if result["quality_status"] != "quarantined":
            result.setdefault("version", 1)
        yield result
