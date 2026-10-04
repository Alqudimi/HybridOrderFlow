"""Master End-to-End Live Execution and Verification Script.

Executes and verifies every component of Big Data Phase 1 & Phase 2 live:
1. Ingestion & ELT Pipeline (idempotency, quality rules, quarantine, math consistency)
2. Complete Pytest Suite (41 tests)
3. MongoDB Indexes & Compound ESR Verification
4. Explain Plan Benchmark with Real executionStats (COLLSCAN -> IXSCAN)
5. 5 Catalog Queries with Live MongoDB Execution
6. 5 Analytics Aggregations with Live MongoDB Execution
7. Materialized Views & Incremental Refresh Simulation
8. Scheduled Jobs, Manual Run & Durable Audit Logging
9. Job Failure Mode Testing & Error Logging
10. Live FastAPI REST API Endpoints over HTTP (Port 8000)
11. Error Handling & Edge Cases (404s, empty results)
12. Security Audit & Final Summary
"""

import sys
import json
import time
import urllib.request
import urllib.parse
import urllib.error
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pymongo
from config.settings import Settings
from src.database.indexes import create_database_indexes, get_collection_indexes, PHASE2_INDEXES
from src.database.explain import run_explain_benchmark, save_explain_report
from src.queries.catalog import QUERY_CATALOG, execute_query
from src.analytics.catalog import AGGREGATION_CATALOG, execute_aggregation
from src.materialized_views.manager import (
    DAILY_SALES_MV, TOP_PRODUCTS_MV,
    refresh_materialized_views, get_materialized_view_data
)
from src.jobs.tasks import JOB_REGISTRY, JobDefinition, run_job_by_name
from src.jobs.logger import get_job_execution_history

BASE_URL = "http://127.0.0.1:8000"

def banner(title: str) -> None:
    print("\n" + "=" * 75)
    print(f"  📌 {title}")
    print("=" * 75)

def check_mark(ok: bool, text: str, detail: str = "") -> None:
    symbol = "  ✅ PASS" if ok else "  ❌ FAIL"
    print(f"{symbol} | {text}")
    if detail:
        print(f"         └─ {detail}")
    if not ok:
        print(f"FATAL CHECK FAILED: {text}")
        sys.exit(1)

def http_get(path: str) -> tuple[int, dict | str]:
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except Exception:
                return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body

def http_post(path: str, data: dict | None = None) -> tuple[int, dict | str]:
    url = f"{BASE_URL}{path}"
    json_bytes = json.dumps(data or {}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=json_bytes,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except Exception:
                return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body


def main() -> None:
    print("*" * 75)
    print("     BIG DATA PHASE 1 & PHASE 2 — MASTER SYSTEM VERIFICATION")
    print("*" * 75)

    settings = Settings.from_env()
    client = pymongo.MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[settings.mongo_database]

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 1: DATABASE CONNECTIVITY & COLLECTIONS AUDIT")
    # ──────────────────────────────────────────────────────────────────────────
    info = client.server_info()
    check_mark(True, f"MongoDB Connected: v{info['version']} ({settings.mongo_uri})")
    cols = sorted(db.list_collection_names())
    print(f"         Active collections ({len(cols)}): {cols}")
    for col_name in ["orders_raw", "orders_validated", "orders_quarantine"]:
        check_mark(col_name in cols, f"Collection '{col_name}' exists in MongoDB",
                   f"{db[col_name].count_documents({})} documents")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 2: RUNNING MIDTERM PIPELINE & IDEMPOTENCY CHECK")
    # ──────────────────────────────────────────────────────────────────────────
    initial_valid = db["orders_validated"].count_documents({})
    cmd = [sys.executable, "src/main.py", "--input", "data/orders_sample.csv"]
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    check_mark(p.returncode == 0, "CLI main.py executed successfully", p.stdout.strip().split("\n")[-1])
    after_valid = db["orders_validated"].count_documents({})
    check_mark(initial_valid == after_valid, "Idempotency verified: re-running file produced ZERO duplicates",
               f"Valid count remained exactly {initial_valid}")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 3: RUNNING FULL AUTOMATED PYTEST SUITE (41 TESTS)")
    # ──────────────────────────────────────────────────────────────────────────
    cmd_pytest = [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=line"]
    res_pytest = subprocess.run(cmd_pytest, capture_output=True, text=True, cwd=ROOT)
    pytest_output = res_pytest.stdout.strip()
    check_mark(res_pytest.returncode == 0, "All 41 Unit and Integration Tests Passed", pytest_output.split("\n")[-1])

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 4: MONGODB INDEXES & COMPOUND ESR VERIFICATION")
    # ──────────────────────────────────────────────────────────────────────────
    idx_res = create_database_indexes(db)
    check_mark(idx_res["total_indexes"] >= 3, f"Created/verified {idx_res['total_indexes']} indexes")
    live_indexes = get_collection_indexes(db, "orders_validated")
    live_index_names = [ix["name"] for ix in live_indexes]

    check_mark("idx_customer_id" in live_index_names, "Single-field index: idx_customer_id")
    check_mark("idx_items_sku" in live_index_names, "Multikey index: idx_items_sku (on items_json.sku array)")
    check_mark("idx_city_status_order_date" in live_index_names, "Compound ESR index: idx_city_status_order_date")

    # Inspect compound ESR structure
    compound = next(ix for ix in live_indexes if ix["name"] == "idx_city_status_order_date")
    keys = list(compound["key"].keys())
    check_mark(keys == ["city", "status", "order_date"],
               "Compound index conforms strictly to ESR rule (Equality -> Sort -> Range)",
               f"Keys order: {keys}")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 5: REAL EXPLAIN PLAN BENCHMARK (executionStats BEFORE vs AFTER)")
    # ──────────────────────────────────────────────────────────────────────────
    benchmark_results = run_explain_benchmark(db)
    save_explain_report(benchmark_results)
    check_mark(len(benchmark_results) == 3, "Executed real before-and-after explain for 3 queries")

    for r in benchmark_results:
        b = r.before
        a = r.after
        check_mark(b.has_collscan and a.has_ixscan,
                   f"Query '{r.query_name}': Transitioned from COLLSCAN to IXSCAN",
                   f"Docs examined: {b.total_docs_examined} -> {a.total_docs_examined} ({r.docs_examined_ratio}x reduction)")
        if r.query_name == "orders_by_city_and_status":
            sort_eliminated = b.has_in_memory_sort and not a.has_in_memory_sort
            check_mark(sort_eliminated, "In-Memory SORT stage completely ELIMINATED by ESR index")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 6: LIVE EXECUTION OF ALL 5 CATALOG QUERIES")
    # ──────────────────────────────────────────────────────────────────────────
    check_mark(len(QUERY_CATALOG) >= 5, f"Catalog contains {len(QUERY_CATALOG)} queries")
    for qname in QUERY_CATALOG:
        res = execute_query(db, qname)
        check_mark(isinstance(res["results"], list) and res["count"] >= 0,
                   f"Query '{qname}' executed cleanly",
                   f"Returned {res['count']} documents (JSON serializable)")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 7: LIVE EXECUTION OF ALL 5 ANALYTICS AGGREGATIONS")
    # ──────────────────────────────────────────────────────────────────────────
    check_mark(len(AGGREGATION_CATALOG) >= 5, f"Analytics engine contains {len(AGGREGATION_CATALOG)} aggregations")
    for aname in AGGREGATION_CATALOG:
        res = execute_aggregation(db, aname)
        check_mark(isinstance(res["results"], list) and res["count"] > 0,
                   f"Aggregation '{aname}' executed cleanly",
                   f"Returned {res['count']} aggregated groups (JSON serializable)")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 8: MATERIALIZED VIEWS & LIVE INCREMENTAL REFRESH")
    # ──────────────────────────────────────────────────────────────────────────
    # Initial build
    r_build = refresh_materialized_views(db, force_rebuild=True)
    check_mark(r_build["status"] == "success", "Materialized Views initial build succeeded")

    daily_count = db[DAILY_SALES_MV].count_documents({})
    products_count = db[TOP_PRODUCTS_MV].count_documents({})
    check_mark(daily_count > 0, f"Daily Sales MV populated ({daily_count} dates)")
    check_mark(products_count > 0, f"Top Products MV populated ({products_count} SKUs)")

    # Re-run with no changes -> must be up_to_date
    r_idle = refresh_materialized_views(db, force_rebuild=False)
    check_mark(r_idle["status"] == "up_to_date" and r_idle["records_processed"] == 0,
               "Incremental update idle state: returned 'up_to_date' with 0 records processed")

    # Insert new delta record
    test_sku = "SKU-MASTER-VERIFY-1"
    test_date = "2029-12-31"
    test_id = f"ORDER-MASTER-VERIFY-{int(time.time())}"
    delta_doc = {
        "order_id": test_id,
        "customer_id": "عميل-تحقق-شامل",
        "city": "حضرموت",
        "district": "سيئون",
        "order_date": f"{test_date}T15:00:00",
        "status": "مؤكد",
        "quality_status": "valid",
        "total_amount": 150000.0,
        "items_json": [{"sku": test_sku, "name": "عسل سدر ملكي", "qty": 1, "unit_price": 150000.0, "total": 150000.0}],
    }
    try:
        db["orders_validated"].insert_one(delta_doc)
        r_inc = refresh_materialized_views(db, force_rebuild=False)
        check_mark(r_inc["mode"] == "incremental_refresh",
                   "Incremental update detected new records via watermark",
                   f"Mode: {r_inc['mode']}, Processed: {r_inc['records_processed']}")

        # Verify new record reflected in both MVs
        mv_day = db[DAILY_SALES_MV].find_one({"_id": test_date})
        check_mark(mv_day is not None and mv_day["total_revenue"] == 150000.0,
                   f"Daily Sales MV reflected new date {test_date}",
                   f"Revenue: {mv_day['total_revenue'] if mv_day else None}")

        mv_prod = db[TOP_PRODUCTS_MV].find_one({"_id": test_sku})
        check_mark(mv_prod is not None and mv_prod["total_units_sold"] == 1,
                   f"Top Products MV reflected new SKU {test_sku}",
                   f"Units: {mv_prod['total_units_sold'] if mv_prod else None}")
    finally:
        db["orders_validated"].delete_one({"order_id": test_id})
        db[DAILY_SALES_MV].delete_one({"_id": test_date})
        db[TOP_PRODUCTS_MV].delete_one({"_id": test_sku})

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 9: SCHEDULED JOBS, MANUAL RUN & FAILURE AUDIT LOGGING")
    # ──────────────────────────────────────────────────────────────────────────
    check_mark(len(JOB_REGISTRY) >= 2, f"Found {len(JOB_REGISTRY)} registered scheduled jobs")
    for jname, jdef in JOB_REGISTRY.items():
        check_mark(jdef.schedule_interval_seconds > 0,
                   f"Job '{jname}' has schedule: {jdef.schedule_human} ({jdef.schedule_interval_seconds}s)")
        res_job = run_job_by_name(db, jname, trigger_type="master_verify")
        check_mark(res_job["status"] == "SUCCESS",
                   f"Job '{jname}' ran manually",
                   f"Duration: {res_job['duration_seconds']}s, Log ID: {res_job['log_id']}")

    # Failure mode test
    def broken_handler(d: pymongo.database.Database):
        raise ConnectionResetError("Simulated socket drop for verification")

    JOB_REGISTRY["master_test_failing_job"] = JobDefinition(
        name="master_test_failing_job",
        description="Failing job simulation",
        schedule_interval_seconds=60,
        schedule_human="Every minute",
        handler=broken_handler,
    )
    try:
        f_res = run_job_by_name(db, "master_test_failing_job", trigger_type="fault_injection")
        check_mark(f_res["status"] == "FAILED",
                   "Fault injection handled safely without server crash",
                   f"Status: {f_res['status']}, Error: {f_res['error']}")
        # Verify durable log in MongoDB
        fail_log = db["job_execution_logs"].find_one({"job_name": "master_test_failing_job"})
        check_mark(fail_log is not None and fail_log["status"] == "FAILED",
                   "Failure audit record securely saved in MongoDB job_execution_logs")
    finally:
        db["job_execution_logs"].delete_many({"job_name": "master_test_failing_job"})
        del JOB_REGISTRY["master_test_failing_job"]

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 10: FASTAPI LIVE HTTP REST API (PORT 8000)")
    # ──────────────────────────────────────────────────────────────────────────
    code, docs_html = http_get("/docs")
    check_mark(code == 200 and "swagger-ui" in str(docs_html).lower(),
               "GET /docs: Interactive Swagger UI loaded successfully")

    code, health_data = http_get("/health")
    check_mark(code == 200 and health_data.get("status") == "ok",
               f"GET /health: 200 OK (MongoDB v{health_data['database']['mongodb_version']})")

    code, idx_http = http_post("/indexes")
    check_mark(code == 200 and idx_http.get("total_indexes") >= 3,
               f"POST /indexes: 200 OK (Created {idx_http.get('total_indexes')} indexes)")

    code, q_http = http_get("/queries")
    check_mark(code == 200 and len(q_http.get("queries", [])) >= 5,
               f"GET /queries: 200 OK ({len(q_http.get('queries', []))} queries)")

    for qname in q_http.get("queries", []):
        code, q_detail = http_get(f"/queries/{qname}")
        check_mark(code == 200 and isinstance(q_detail.get("results"), list),
                   f"GET /queries/{qname}: 200 OK (count={q_detail.get('count')})")

    code, a_http = http_get("/aggregations")
    check_mark(code == 200 and len(a_http.get("aggregations", [])) >= 5,
               f"GET /aggregations: 200 OK ({len(a_http.get('aggregations', []))} aggregations)")

    for aname in a_http.get("aggregations", []):
        code, a_detail = http_get(f"/aggregations/{aname}")
        check_mark(code == 200 and isinstance(a_detail.get("results"), list),
                   f"GET /aggregations/{aname}: 200 OK (count={a_detail.get('count')})")

    code, mv_http = http_post("/refresh-mv", {"force_rebuild": False})
    check_mark(code == 200 and "status" in mv_http,
               f"POST /refresh-mv: 200 OK (status={mv_http.get('status')}, mode={mv_http.get('mode')})")

    code, jobs_http = http_get("/jobs")
    check_mark(code == 200 and jobs_http.get("total_jobs", 0) >= 2,
               f"GET /jobs: 200 OK ({jobs_http.get('total_jobs')} jobs)")

    for j in jobs_http.get("jobs", []):
        code, run_http = http_post(f"/jobs/{j['name']}/run")
        check_mark(code == 200 and run_http.get("status") == "SUCCESS",
                   f"POST /jobs/{j['name']}/run: 200 OK (duration={run_http.get('duration_seconds')}s)")

    code, ing_http = http_post("/ingest", {"file_path": "data/orders_sample.csv"})
    check_mark(code == 200 and ing_http.get("status") == "success",
               f"POST /ingest: 200 OK (engine={ing_http.get('engine_used')}, raw_loaded={ing_http['metrics'].get('raw_loaded')})")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 11: ERROR HANDLING & EDGE CASES (404s & EMPTY FILTERS)")
    # ──────────────────────────────────────────────────────────────────────────
    code, _ = http_get("/queries/does_not_exist_xyz")
    check_mark(code == 404, "GET /queries/invalid -> 404 Not Found")

    code, _ = http_get("/aggregations/does_not_exist_xyz")
    check_mark(code == 404, "GET /aggregations/invalid -> 404 Not Found")

    code, _ = http_post("/jobs/does_not_exist_xyz/run")
    check_mark(code == 404, "POST /jobs/invalid/run -> 404 Not Found")

    code, _ = http_post("/ingest", {"file_path": "non_existent_file.csv"})
    check_mark(code == 404, "POST /ingest missing file -> 404 Not Found")

    code, empty_q = http_get("/queries/orders_by_customer?customer_id=NO_SUCH_CUSTOMER_XYZ")
    check_mark(code == 200 and empty_q.get("count") == 0 and empty_q.get("results") == [],
               "Empty query filter returned count=0 and empty list without crashing")

    # ──────────────────────────────────────────────────────────────────────────
    banner("STEP 12: SECURITY AUDIT & CREDENTIALS CHECK")
    # ──────────────────────────────────────────────────────────────────────────
    gitignore_text = Path(ROOT / ".gitignore").read_text()
    check_mark(".env" in gitignore_text, ".env is safely excluded in .gitignore")
    check_mark(".venv/" in gitignore_text, ".venv/ is safely excluded in .gitignore")
    check_mark(Path(ROOT / "env.example").exists(), "env.example exists and provides clean defaults")

    p_sec = subprocess.run(
        ["grep", "-r", "--include=*.py", "-i", "password.*=.*['\"][a-zA-Z0-9_]{8,}['\"]", "src/"],
        capture_output=True, text=True, cwd=ROOT
    )
    check_mark(p_sec.stdout.strip() == "", "Zero hardcoded passwords or secrets found in src/")

    print("\n" + "*" * 75)
    print("  🏆 ALL CHECKS, TESTS, QUERIES, AND ENDPOINTS FULLY VERIFIED (100% PASS)!")
    print("*" * 75 + "\n")


if __name__ == "__main__":
    main()
