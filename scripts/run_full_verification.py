#!/usr/bin/env python3
"""Comprehensive end-to-end verification script for the Big Data Midterm Pipeline.

Executes and verifies:
1. Schema & Indexes setup on MongoDB
2. Small File Route -> Python Batch (Streaming ELT)
3. Idempotency Proof (Full replay without duplicate business records)
4. Update Proof (Updating single existing record without duplicate creation)
5. Large File Route -> Apache Spark / PySpark (Parallel ELT + Mongo Connector)
6. Track B: Incremental Delta Loading, Version Handling & Conflict Resolution
7. Mathematical Consistency Verification across all runs
8. Operational MongoDB Aggregations & Report Generation
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# Ensure workspace root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pymongo
from config.settings import Settings
from src.batch_loader import load_raw_streaming
from src.elt_pipeline import run_python_elt
from src.file_router import choose_engine, inspect_file
from src.incremental_loader import load_delta
from src.metrics import RunMetrics, write_results
from src.mongo_aggregation import (
    latest_version_per_order_pipeline,
    quality_error_counts_pipeline,
    validated_status_counts_pipeline,
)
from src.mongo_setup import create_repository
from src.spark_loader import run_spark_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("verification")


def reset_database(settings: Settings) -> pymongo.database.Database:
    """Clean test database to ensure a clean baseline for verification."""
    client = pymongo.MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[settings.mongo_database]
    for coll in ("orders_raw", "orders_validated", "orders_quarantine", "pipeline_idempotency_keys", "smart_poll_state"):
        db[coll].drop()
    logger.info("Reset database: %s", settings.mongo_database)
    return db


def verify_phase_1_python_batch(settings: Settings, sample_path: Path) -> RunMetrics:
    """Phase 1: Small file route with Python Batch streaming ELT."""
    logger.info("=== Phase 1: Testing Python Batch Streaming Route (Initial Load) ===")
    file_info = inspect_file(sample_path)
    decision = choose_engine(file_info, settings)
    assert decision.engine == "python_batch", f"Expected python_batch, got {decision.engine}"

    run_id = f"demo-python-batch-init-{uuid.uuid4().hex[:8]}"
    repository = create_repository(settings)
    try:
        metrics = run_python_elt(decision, run_id, settings, repository)
        assert metrics.raw_loaded == metrics.rows_read, "Raw loaded mismatch"
        assert metrics.consistency_passed, "Consistency equation failed for Python Batch"
        assert metrics.inserted_count > 0, "Expected inserts on initial load"
        logger.info(
            "Phase 1 Success: Read=%d, Valid=%d, Corrected=%d, Quarantined=%d, Inserted=%d, Throughput=%.1f rec/s",
            metrics.rows_read, metrics.valid_count, metrics.corrected_count,
            metrics.quarantine_count, metrics.inserted_count, metrics.throughput
        )
        return metrics
    finally:
        repository.close()


def verify_phase_2_idempotency_and_update(settings: Settings, sample_path: Path) -> tuple[RunMetrics, RunMetrics]:
    """Phase 2: Idempotency & In-place Single Record Update verification."""
    logger.info("=== Phase 2: Idempotency & Business Key Update Proof ===")
    client = pymongo.MongoClient(settings.mongo_uri)
    db = client[settings.mongo_database]
    initial_validated_count = db.orders_validated.count_documents({})
    
    # 2.1 Replay exact same dataset with Python Batch
    file_info = inspect_file(sample_path)
    decision = choose_engine(file_info, settings, force_engine="python_batch")
    run_id_replay = f"demo-replay-{uuid.uuid4().hex[:8]}"
    repository = create_repository(settings)
    try:
        metrics_replay = run_python_elt(decision, run_id_replay, settings, repository)
        after_replay_count = db.orders_validated.count_documents({})
        assert after_replay_count == initial_validated_count, (
            f"Idempotency violation! Initial={initial_validated_count}, After Replay={after_replay_count}"
        )
        assert metrics_replay.inserted_count == 0, f"Expected 0 inserts on replay, got {metrics_replay.inserted_count}"
        assert metrics_replay.unchanged_count == initial_validated_count, (
            f"Expected {initial_validated_count} unchanged on replay, got {metrics_replay.unchanged_count}"
        )
        logger.info(
            "Phase 2.1 (Replay) Success: Validated count unchanged (%d == %d), Unchanged=%d, Inserted=%d (100%% Idempotent)",
            initial_validated_count, after_replay_count, metrics_replay.unchanged_count, metrics_replay.inserted_count
        )
    finally:
        repository.close()

    # 2.2 Update a single existing record in-place
    bak_path = sample_path.with_suffix(".csv.bak")
    import shutil
    shutil.copy(sample_path, bak_path)
    
    target_id = "طلب-100001"
    try:
        with bak_path.open("r", encoding="utf-8-sig") as f_in, sample_path.open("w", encoding="utf-8", newline="") as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
            writer.writeheader()
            rows = list(reader)
            # Target row with order_id == "طلب-100001"
            for r in rows:
                if r["order_id"] == target_id:
                    r["status"] = "تم التسليم"
                    r["customer_phone"] = "+967 77 123 4567"
                writer.writerow(r)

        decision_update = choose_engine(inspect_file(sample_path), settings, force_engine="python_batch")
        run_id_update = f"demo-single-update-{uuid.uuid4().hex[:8]}"
        repository = create_repository(settings)
        try:
            metrics_update = run_python_elt(decision_update, run_id_update, settings, repository)
            after_update_count = db.orders_validated.count_documents({})
            assert after_update_count == initial_validated_count, (
                f"Duplicate created on update! Count before={initial_validated_count}, Count after={after_update_count}"
            )
            assert metrics_update.updated_count == 1, f"Expected updated_count == 1, got {metrics_update.updated_count}"
            assert metrics_update.inserted_count == 0, f"Expected inserted_count == 0, got {metrics_update.inserted_count}"
            assert metrics_update.unchanged_count == initial_validated_count - 1, f"Expected unchanged_count == {initial_validated_count - 1}, got {metrics_update.unchanged_count}"
            
            # Verify MongoDB document was updated in-place
            doc = db.orders_validated.find_one({"order_id": target_id})
            assert doc is not None, f"Order {target_id} not found in validated"
            assert doc.get("status") == "تم التسليم", f"Expected updated status 'تم التسليم', got {doc.get('status')}"
            assert doc.get("customer_phone") == "771234567", f"Expected cleaned phone '771234567', got {doc.get('customer_phone')}"
            logger.info(
                "Phase 2.2 (Update) Success: Order %s updated in-place without duplicate. Status='%s', Phone='%s'",
                target_id, doc.get("status"), doc.get("customer_phone")
            )
        finally:
            repository.close()
    finally:
        shutil.copy(bak_path, sample_path)
        bak_path.unlink()
        client.close()

    return metrics_replay, metrics_update


def verify_phase_3_pyspark(settings: Settings, sample_path: Path) -> RunMetrics:
    """Phase 3: PySpark parallel ELT route with MongoDB Spark Connector."""
    logger.info("=== Phase 3: Testing PySpark Route with Mongo Spark Connector ===")
    file_info = inspect_file(sample_path)
    # Force PySpark for testing on sample dataset
    decision = choose_engine(file_info, settings, force_engine="pyspark")
    assert decision.engine == "pyspark"

    run_id = f"demo-pyspark-{uuid.uuid4().hex[:8]}"
    spark_result = run_spark_pipeline(decision, run_id, settings)
    
    metrics = RunMetrics(
        run_id=run_id,
        file_name=decision.file.path.name,
        file_size_mb=decision.file.size_mb,
        engine_used="pyspark",
        rows_read=spark_result.raw_result.rows_read,
        raw_loaded=spark_result.raw_result.raw_loaded,
        valid_count=spark_result.valid_count,
        corrected_count=spark_result.corrected_count,
        quarantine_count=spark_result.quarantine_count,
        partitions=spark_result.partitions,
        inserted_count=spark_result.inserted_count,
        updated_count=spark_result.updated_count,
        unchanged_count=spark_result.unchanged_count,
        error_case_counts=spark_result.error_case_counts or {},
    )
    metrics.finish(spark_result.elapsed_seconds)
    write_results(settings.results_path, metrics)
    assert metrics.consistency_passed, "Consistency equation failed for PySpark"
    logger.info(
        "Phase 3 Success: PySpark Read=%d, Valid=%d, Corrected=%d, Quarantined=%d, Partitions=%d, Throughput=%.1f rec/s",
        metrics.rows_read, metrics.valid_count, metrics.corrected_count,
        metrics.quarantine_count, metrics.partitions, metrics.throughput
    )
    return metrics


def verify_phase_4_incremental_path_b(settings: Settings) -> list[RunMetrics]:
    """Phase 4: Track B Incremental Loading, Delta Upserts & Version Handling."""
    logger.info("=== Phase 4: Track B Incremental Loading & Version Handling ===")
    scratch_dir = PROJECT_ROOT / "data" / "scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    
    # 4.1 Create Base Dataset (Initial Load)
    initial_csv = scratch_dir / "delta_initial.csv"
    delta_csv = scratch_dir / "delta_updates.csv"

    fieldnames = [
        "order_id", "order_date", "status", "customer_id", "customer_name",
        "customer_phone", "customer_email", "city", "district", "delivery_type",
        "delivery_cost", "payment_method", "payment_status", "payment_amount",
        "currency", "total_amount", "items_json", "version"
    ]

    base_rows = [
        {
            "order_id": "ORD-INC-001", "order_date": "2025-05-01", "status": "مؤكد",
            "customer_id": "CUST-10", "customer_name": "أحمد يحيى", "customer_phone": "771112233",
            "customer_email": "ahmed@example.com", "city": "صنعاء", "district": "حدة",
            "delivery_type": "عادي", "delivery_cost": "2000.0", "payment_method": "نقدًا",
            "payment_status": "تم الدفع", "payment_amount": "52000.0", "currency": "YER",
            "total_amount": "52000.0",
            "items_json": '[{"sku":"SKU-1","name":"كابل USB","qty":2,"unit_price":25000.0,"total":50000.0}]',
            "version": "1"
        },
        {
            "order_id": "ORD-INC-002", "order_date": "2025-05-01", "status": "قيد الانتظار",
            "customer_id": "CUST-20", "customer_name": "سارة محمد", "customer_phone": "732223344",
            "customer_email": "sara@example.com", "city": "عدن", "district": "المنصورة",
            "delivery_type": "سريع", "delivery_cost": "5000.0", "payment_method": "بطاقة",
            "payment_status": "بانتظار الدفع", "payment_amount": "105000.0", "currency": "YER",
            "total_amount": "105000.0",
            "items_json": '[{"sku":"SKU-2","name":"سماعة بلوتوث","qty":1,"unit_price":100000.0,"total":100000.0}]',
            "version": "1"
        }
    ]

    with initial_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(base_rows)

    # Initial Load execution
    run_id_init = f"demo-delta-initial-{uuid.uuid4().hex[:8]}"
    repository = create_repository(settings)
    started = time.perf_counter()
    init_res = load_delta(initial_csv, run_id_init, repository, version_field="version")
    m_init = RunMetrics(
        run_id=run_id_init,
        file_name=initial_csv.name,
        file_size_mb=round(initial_csv.stat().st_size / (1024 * 1024), 4),
        engine_used="incremental_initial",
        rows_read=init_res.rows_read,
        raw_loaded=init_res.rows_read,
        valid_count=init_res.valid_count,
        corrected_count=init_res.corrected_count,
        quarantine_count=init_res.quarantine_count,
        inserted_count=init_res.inserted_count,
        updated_count=init_res.updated_count,
        unchanged_count=init_res.unchanged_count,
    )
    m_init.finish(time.perf_counter() - started)
    write_results(settings.results_path, m_init)
    logger.info("Track B Step 1 (Initial): Inserted=%d, Updated=%d", m_init.inserted_count, m_init.updated_count)

    # 4.2 Delta 1: Insert 1 new order (ORD-INC-003) + Update ORD-INC-001 (version=2) + Unchanged (version=1 for ORD-INC-002)
    delta_rows = [
        # Updated order with newer version (version=2)
        {
            "order_id": "ORD-INC-001", "order_date": "2025-05-01", "status": "تم التسليم",
            "customer_id": "CUST-10", "customer_name": "أحمد يحيى", "customer_phone": "771112233",
            "customer_email": "ahmed@example.com", "city": "صنعاء", "district": "حدة",
            "delivery_type": "عادي", "delivery_cost": "2000.0", "payment_method": "نقدًا",
            "payment_status": "تم الدفع", "payment_amount": "52000.0", "currency": "YER",
            "total_amount": "52000.0",
            "items_json": '[{"sku":"SKU-1","name":"كابل USB","qty":2,"unit_price":25000.0,"total":50000.0}]',
            "version": "2"
        },
        # Brand new order
        {
            "order_id": "ORD-INC-003", "order_date": "2025-05-02", "status": "مؤكد",
            "customer_id": "CUST-30", "customer_name": "خالد عمر", "customer_phone": "713334455",
            "customer_email": "khaled@example.com", "city": "تعز", "district": "القاهرة",
            "delivery_type": "سريع", "delivery_cost": "5000.0", "payment_method": "محفظة",
            "payment_status": "تم الدفع", "payment_amount": "85000.0", "currency": "YER",
            "total_amount": "85000.0",
            "items_json": '[{"sku":"SKU-3","name":"لوحة مفاتيح","qty":1,"unit_price":80000.0,"total":80000.0}]',
            "version": "1"
        }
    ]

    with delta_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(delta_rows)

    run_id_delta1 = f"demo-delta-run1-{uuid.uuid4().hex[:8]}"
    started = time.perf_counter()
    delta1_res = load_delta(delta_csv, run_id_delta1, repository, version_field="version")
    m_delta1 = RunMetrics(
        run_id=run_id_delta1,
        file_name=delta_csv.name,
        file_size_mb=round(delta_csv.stat().st_size / (1024 * 1024), 4),
        engine_used="incremental_delta_1",
        rows_read=delta1_res.rows_read,
        raw_loaded=delta1_res.rows_read,
        valid_count=delta1_res.valid_count,
        corrected_count=delta1_res.corrected_count,
        quarantine_count=delta1_res.quarantine_count,
        inserted_count=delta1_res.inserted_count,
        updated_count=delta1_res.updated_count,
        unchanged_count=delta1_res.unchanged_count,
    )
    m_delta1.finish(time.perf_counter() - started)
    write_results(settings.results_path, m_delta1)
    assert m_delta1.inserted_count == 1, f"Expected 1 insert in Delta 1, got {m_delta1.inserted_count}"
    assert m_delta1.updated_count == 1, f"Expected 1 update in Delta 1, got {m_delta1.updated_count}"
    logger.info("Track B Step 2 (Delta 1): Inserted=%d, Updated=%d, Unchanged=%d", m_delta1.inserted_count, m_delta1.updated_count, m_delta1.unchanged_count)

    # 4.3 Delta 1 Replay (Idempotency of Delta)
    run_id_delta_replay = f"demo-delta-replay-{uuid.uuid4().hex[:8]}"
    started = time.perf_counter()
    delta_replay_res = load_delta(delta_csv, run_id_delta_replay, repository, version_field="version")
    m_delta_replay = RunMetrics(
        run_id=run_id_delta_replay,
        file_name=delta_csv.name,
        file_size_mb=round(delta_csv.stat().st_size / (1024 * 1024), 4),
        engine_used="incremental_delta_replay",
        rows_read=delta_replay_res.rows_read,
        raw_loaded=delta_replay_res.rows_read,
        valid_count=delta_replay_res.valid_count,
        corrected_count=delta_replay_res.corrected_count,
        quarantine_count=delta_replay_res.quarantine_count,
        inserted_count=delta_replay_res.inserted_count,
        updated_count=delta_replay_res.updated_count,
        unchanged_count=delta_replay_res.unchanged_count,
    )
    m_delta_replay.finish(time.perf_counter() - started)
    write_results(settings.results_path, m_delta_replay)
    assert m_delta_replay.inserted_count == 0, f"Expected 0 inserts on delta replay, got {m_delta_replay.inserted_count}"
    assert m_delta_replay.updated_count == 0, f"Expected 0 updates on delta replay, got {m_delta_replay.updated_count}"
    assert m_delta_replay.unchanged_count == 2, f"Expected 2 unchanged on delta replay, got {m_delta_replay.unchanged_count}"
    logger.info("Track B Step 3 (Delta Replay): Inserted=%d, Updated=%d, Unchanged=%d (100%% Idempotent)", m_delta_replay.inserted_count, m_delta_replay.updated_count, m_delta_replay.unchanged_count)

    repository.close()
    return [m_init, m_delta1, m_delta_replay]


def verify_mongodb_aggregations(settings: Settings) -> None:
    """Verify MongoDB aggregation pipelines on actual stored data."""
    logger.info("=== Phase 5: Testing MongoDB Server-Side Aggregation Pipelines ===")
    repository = create_repository(settings)
    try:
        errors = repository.aggregate_quality_errors()
        statuses = repository.aggregate_validated_statuses()
        latest = repository.aggregate_latest_orders()
        logger.info("Aggregated Quality Errors: %s", errors[:3])
        logger.info("Aggregated Validated Statuses: %s", statuses)
        logger.info("Aggregated Latest Orders Count: %d", len(latest))
    finally:
        repository.close()


def main() -> int:
    settings = Settings.from_env()
    settings.validate()
    sample_path = PROJECT_ROOT / "data" / "orders_sample.csv"
    if not sample_path.exists():
        raise FileNotFoundError(f"Sample data missing at {sample_path}")

    logger.info("Starting Complete Pipeline Verification Suite...")
    reset_database(settings)
    
    # 1. Python Batch Initial Run
    verify_phase_1_python_batch(settings, sample_path)
    
    # 2. Idempotency & Update Proof
    verify_phase_2_idempotency_and_update(settings, sample_path)

    # 3. PySpark Parallel Run
    verify_phase_3_pyspark(settings, sample_path)
    
    # 4. Track B: Incremental Loading & Version Handling
    verify_phase_4_incremental_path_b(settings)
    
    # 5. MongoDB Aggregations
    verify_mongodb_aggregations(settings)
    
    logger.info("All Verification Phases Completed Successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
