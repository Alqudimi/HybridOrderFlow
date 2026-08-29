"""Large-file Spark path.

The module is import-safe without Spark installed. A clear runtime error is
raised only when the user explicitly routes a file to PySpark.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Iterator

from config.settings import Settings
from src.batch_loader import RawLoadResult
from src.file_router import RouteDecision
from src.quality_rules import classify_record

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


def build_spark_session(settings: Settings) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "PySpark is not installed. Install requirements.txt and Java 11+ "
            "before running the large-file route."
        ) from error
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
    return builder.getOrCreate()


def run_spark_pipeline(
    decision: RouteDecision,
    run_id: str,
    settings: Settings,
) -> SparkRunResult:
    """Read raw CSV with a fixed all-string schema and process by partitions."""
    started = time.perf_counter()
    spark = build_spark_session(settings)
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [StructField(column, StringType(), True) for column in CSV_COLUMNS]
        )
        raw_df = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(decision.file.path))
        )
        raw_df = (
            raw_df.withColumn("run_id", F.lit(run_id))
            .withColumn("source_file", F.lit(str(decision.file.path)))
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
        valid_rdd = transformed.filter(lambda row: row["quality_status"] != "quarantined")
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
        inserted_count = candidate_count
        updated_count = 0
        unchanged_count = 0
        # Empty branches are valid outcomes; Spark cannot infer a schema from
        # an empty RDD, so only create/write a DataFrame when it has rows.
        if candidate_count:
            valid_df = spark.createDataFrame(valid_rdd)
            (
                inserted_count,
                updated_count,
                unchanged_count,
            ) = _estimate_upsert_stats(spark, valid_df, settings)
            valid_df.write.format("mongodb").mode("append").option(
                "spark.mongodb.write.collection", "orders_validated"
            ).option("spark.mongodb.write.operationType", "replace").option(
                "spark.mongodb.write.idField", "order_id"
            ).option(
                "spark.mongodb.write.upsert", "true"
            ).option(
                "spark.mongodb.write.replaceDocument", "true"
            ).save()
        if quarantine_count:
            quarantine_df = spark.createDataFrame(quarantine_rdd)
            quarantine_df.write.format("mongodb").mode("append").option(
                "spark.mongodb.write.collection", "orders_quarantine"
            ).save()
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
        )
    finally:
        spark.stop()


def _estimate_upsert_stats(
    spark: Any, candidate_df: Any, settings: Settings
) -> tuple[int, int, int]:
    """Compare stable candidate documents with Mongo before connector upsert.

    This read is solely for metrics. The connector's atomic replace/upsert
    remains the operation that changes the business state.
    """
    from pyspark.sql import functions as F

    try:
        existing_df = (
            spark.read.format("mongodb")
            .option(
                "spark.mongodb.read.connection.uri",
                f"{settings.mongo_uri}/{settings.mongo_database}",
            )
            .option("spark.mongodb.read.collection", "orders_validated")
            .load()
        )
    except Exception as error:
        logger.info("No readable validated collection for metrics: %s", error)
        return candidate_df.count(), 0, 0

    common_columns = [
        column
        for column in candidate_df.columns
        if column != "_id" and column in existing_df.columns
    ]
    if "order_id" not in common_columns:
        return candidate_df.count(), 0, 0
    candidate_signature = candidate_df.withColumn(
        "_candidate_signature",
        F.sha2(F.to_json(F.struct(*[F.col(column) for column in common_columns])), 256),
    ).select("order_id", "_candidate_signature")
    existing_signature = existing_df.withColumn(
        "_existing_signature",
        F.sha2(F.to_json(F.struct(*[F.col(column) for column in common_columns])), 256),
    ).select("order_id", "_existing_signature")
    matched = candidate_signature.join(existing_signature, "order_id", "inner")
    existing_count = matched.count()
    unchanged_count = matched.filter(
        F.col("_candidate_signature") == F.col("_existing_signature")
    ).count()
    candidate_count = candidate_df.count()
    return (
        candidate_count - existing_count,
        existing_count - unchanged_count,
        unchanged_count,
    )


def _clean_partition(rows: Iterator[Any]) -> Iterator[dict[str, Any]]:
    for row in rows:
        values = row.asDict(recursive=True)
        raw_record = {
            column: values.get(column) for column in CSV_COLUMNS
        }
        result = classify_record(raw_record)
        result["source"] = {
            "source_file": values.get("source_file"),
            "source_row_number": values.get("source_row_number"),
            "engine_used": "pyspark",
        }
        yield result
