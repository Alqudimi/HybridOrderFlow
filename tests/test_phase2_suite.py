"""Phase 2 Integration Test Suite.

Tests all Phase 2 requirements:
- Indexes (Single, Multikey, Compound ESR)
- 5 Catalog Queries
- 5 Analytics Aggregations & JSON serialization
- Materialized Views & Incremental Refresh
- Scheduled Jobs, Manual Execution, Audit Logging, and Failure Handling
- FastAPI REST API Endpoints and Error Handling
"""

import json
from datetime import UTC, datetime
from typing import Any
import pytest
import pymongo
from fastapi.testclient import TestClient

from config.settings import Settings
from src.database.indexes import create_database_indexes, get_collection_indexes
from src.queries.catalog import QUERY_CATALOG, execute_query
from src.analytics.catalog import AGGREGATION_CATALOG, execute_aggregation
from src.materialized_views.manager import (
    DAILY_SALES_MV,
    TOP_PRODUCTS_MV,
    refresh_materialized_views,
    get_materialized_view_data,
)
from src.jobs.tasks import JOB_REGISTRY, JobDefinition, run_job_by_name
from src.jobs.logger import get_job_execution_history
from src.api.app import app


@pytest.fixture(scope="module")
def db() -> pymongo.database.Database:
    settings = Settings.from_env()
    client = pymongo.MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    return client[settings.mongo_database]


@pytest.fixture(scope="module")
def api_client() -> TestClient:
    return TestClient(app)


# ── 1. Indexes Tests ──────────────────────────────────────────────────────────

def test_indexes_creation_and_structure(db: pymongo.database.Database) -> None:
    created = create_database_indexes(db)
    assert len(created) >= 3

    indexes = get_collection_indexes(db, "orders_validated")
    index_names = [ix["name"] for ix in indexes]
    assert "idx_customer_id" in index_names
    assert "idx_items_sku" in index_names
    assert "idx_city_status_order_date" in index_names

    # Verify compound ESR index structure
    compound = next(ix for ix in indexes if ix["name"] == "idx_city_status_order_date")
    keys = list(compound["key"].keys())
    assert len(keys) >= 3
    assert keys[0] == "city"
    assert "status" in keys
    assert "order_date" in keys


# ── 2. Queries Tests ──────────────────────────────────────────────────────────

def test_five_queries_execution(db: pymongo.database.Database) -> None:
    assert len(QUERY_CATALOG) >= 5

    for qname in QUERY_CATALOG:
        res = execute_query(db, qname)
        assert "query" in res
        assert "results" in res
        assert isinstance(res["results"], list)
        assert res["count"] >= 0

        # Verify JSON serializability
        encoded = json.dumps(res)
        assert isinstance(encoded, str)


def test_query_data_independence(db: pymongo.database.Database) -> None:
    cities = db["orders_validated"].distinct("city")
    if cities:
        test_city = cities[0]
        res = execute_query(db, "orders_by_city_and_status", {"city": test_city, "limit": 5})
        assert isinstance(res["results"], list)


# ── 3. Aggregations Tests ─────────────────────────────────────────────────────

def test_five_aggregations_execution(db: pymongo.database.Database) -> None:
    assert len(AGGREGATION_CATALOG) >= 5

    for aname in AGGREGATION_CATALOG:
        res = execute_aggregation(db, aname)
        assert "aggregation" in res
        assert "results" in res
        assert isinstance(res["results"], list)
        assert res["count"] > 0

        # Verify JSON serializability
        encoded = json.dumps(res)
        assert isinstance(encoded, str)


# ── 4. Materialized Views & Incremental Refresh ────────────────────────────────

def test_materialized_views_and_incremental_refresh(db: pymongo.database.Database) -> None:
    # 1. Ensure initial build
    r1 = refresh_materialized_views(db, force_rebuild=True)
    assert r1["status"] == "success"

    daily_data = get_materialized_view_data(db, DAILY_SALES_MV)
    assert len(daily_data) > 0

    products_data = get_materialized_view_data(db, TOP_PRODUCTS_MV)
    assert len(products_data) > 0

    # 2. Call with no new data -> must return up_to_date
    r2 = refresh_materialized_views(db, force_rebuild=False)
    assert r2["status"] == "up_to_date"
    assert r2.get("records_processed", 0) == 0

    # 3. Insert synthetic new record and test incremental detection
    test_id = f"TEST-INC-PYTEST-{datetime.now(UTC).timestamp()}"
    test_doc = {
        "order_id": test_id,
        "customer_id": "عميل-pytest",
        "city": "عدن",
        "district": "كريتر",
        "order_date": "2025-07-01T12:00:00",
        "status": "مؤكد",
        "quality_status": "valid",
        "total_amount": 5555.0,
        "items_json": [{"sku": "SKU-TEST-PYTEST", "name": "منتج اختبار", "qty": 1, "unit_price": 5555.0, "total": 5555.0}],
    }
    try:
        db["orders_validated"].insert_one(test_doc)
        r3 = refresh_materialized_views(db, force_rebuild=False)
        assert r3["mode"] == "incremental_refresh"
        assert r3.get("records_processed", 0) >= 1
    finally:
        db["orders_validated"].delete_one({"order_id": test_id})


# ── 5. Scheduled Jobs & Failure Test ──────────────────────────────────────────

def test_scheduled_jobs_execution_and_logging(db: pymongo.database.Database) -> None:
    assert len(JOB_REGISTRY) >= 2

    for jname in JOB_REGISTRY:
        res = run_job_by_name(db, jname, trigger_type="test")
        assert res["status"] == "SUCCESS"
        assert res["started_at"] is not None
        assert res["finished_at"] is not None
        assert res["log_id"] is not None

    logs = get_job_execution_history(db, limit=5)
    assert len(logs) >= 2
    assert "status" in logs[0]
    assert "duration_seconds" in logs[0]


def test_job_failure_safety_and_audit(db: pymongo.database.Database) -> None:
    def failing_handler(d: pymongo.database.Database) -> tuple[int, dict[str, Any]]:
        raise RuntimeError("Simulated failure for testing")

    JOB_REGISTRY["test_suite_failing_job"] = JobDefinition(
        name="test_suite_failing_job",
        description="Failing job test",
        schedule_interval_seconds=60,
        schedule_human="Every minute",
        handler=failing_handler,
    )
    try:
        res = run_job_by_name(db, "test_suite_failing_job", trigger_type="unit_test")
        assert res["status"] == "FAILED"
        assert "Simulated failure for testing" in (res["error"] or "")
        assert res["started_at"] is not None
        assert res["finished_at"] is not None

        # Verify DB audit entry
        entry = db["job_execution_logs"].find_one({"job_name": "test_suite_failing_job"})
        assert entry is not None
        assert entry["status"] == "FAILED"
        assert "Simulated failure for testing" in entry["error"]
    finally:
        db["job_execution_logs"].delete_many({"job_name": "test_suite_failing_job"})
        del JOB_REGISTRY["test_suite_failing_job"]


# ── 6. FastAPI REST API Endpoints ─────────────────────────────────────────────

def test_api_endpoints_health_and_ops(api_client: TestClient) -> None:
    # GET /health
    r = api_client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

    # POST /indexes
    r = api_client.post("/indexes")
    assert r.status_code == 200
    assert r.json()["total_indexes"] >= 3

    # GET /queries
    r = api_client.get("/queries")
    assert r.status_code == 200
    assert len(r.json()["queries"]) >= 5

    # GET /queries/{name}
    r = api_client.get("/queries/orders_by_customer?customer_id=عميل-1&limit=5")
    assert r.status_code == 200
    assert isinstance(r.json()["results"], list)

    # GET /aggregations
    r = api_client.get("/aggregations")
    assert r.status_code == 200
    assert len(r.json()["aggregations"]) >= 5

    # GET /aggregations/{name}
    r = api_client.get("/aggregations/sales_by_city?limit=5")
    assert r.status_code == 200
    assert len(r.json()["results"]) > 0

    # POST /refresh-mv
    r = api_client.post("/refresh-mv")
    assert r.status_code == 200

    # GET /jobs
    r = api_client.get("/jobs")
    assert r.status_code == 200
    assert r.json()["total_jobs"] >= 2

    # POST /jobs/{name}/run
    r = api_client.post("/jobs/daily_sales_report_job/run")
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"


def test_api_error_handling(api_client: TestClient) -> None:
    # Invalid query
    r = api_client.get("/queries/non_existent_query")
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()

    # Invalid aggregation
    r = api_client.get("/aggregations/non_existent_aggregation")
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()

    # Invalid job run
    r = api_client.post("/jobs/non_existent_job/run")
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()

    # Missing file ingest
    r = api_client.post("/ingest", json={"file_path": "non_existent_file.csv"})
    assert r.status_code == 404
