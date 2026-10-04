"""
Master Audit Script — Big Data Phase 2 Final Verification
Runs all checks in sequence and produces a structured report.
"""
import sys
import json
import time
import traceback
from pathlib import Path

# ── Add project root to path ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pymongo
from config.settings import Settings

settings = Settings.from_env()
client = pymongo.MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
db = client[settings.mongo_database]

RESULTS = {}

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def check(name, ok, detail=""):
    status = "✅ PASS" if ok else "❌ FAIL"
    print(f"  {status}  {name}")
    if detail:
        print(f"         {detail}")
    RESULTS[name] = {"pass": ok, "detail": detail}
    return ok

# ══════════════════════════════════════════════════════════════════════════════
section("1. MONGODB CONNECTION & COLLECTIONS")
# ══════════════════════════════════════════════════════════════════════════════
try:
    info = client.server_info()
    check("MongoDB connected", True, f"v{info['version']}")
    cols = db.list_collection_names()
    check("orders_validated exists", "orders_validated" in cols,
          f"{db.orders_validated.count_documents({})} docs")
    check("orders_raw exists", "orders_raw" in cols,
          f"{db.orders_raw.count_documents({})} docs")
    check("orders_quarantine exists", "orders_quarantine" in cols,
          f"{db.orders_quarantine.count_documents({})} docs")
    check("mv_daily_sales_summary exists", "mv_daily_sales_summary" in cols,
          f"{db['mv_daily_sales_summary'].count_documents({})} docs")
    check("mv_top_products_summary exists", "mv_top_products_summary" in cols,
          f"{db['mv_top_products_summary'].count_documents({})} docs")
    check("job_execution_logs exists", "job_execution_logs" in cols,
          f"{db['job_execution_logs'].count_documents({})} docs")
except Exception as e:
    check("MongoDB connected", False, str(e))
    print("FATAL: Cannot continue without MongoDB. Exiting.")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════════════════════
section("2. INDEXES AUDIT")
# ══════════════════════════════════════════════════════════════════════════════
all_indexes = list(db.orders_validated.list_indexes())
index_names = [ix.get("name") for ix in all_indexes]
print(f"  Existing indexes on orders_validated: {index_names}")

check("idx_customer_id exists", "idx_customer_id" in index_names)
check("idx_items_sku exists", "idx_items_sku" in index_names)
check("idx_city_status_order_date exists", "idx_city_status_order_date" in index_names,
      "(compound ESR index)")

# Verify compound index fields
compound = next((ix for ix in all_indexes if ix.get("name") == "idx_city_status_order_date"), None)
if compound:
    keys = list(compound["key"].keys())
    check("Compound index has 3+ fields", len(keys) >= 3, f"Fields: {keys}")
    check("Compound index starts with city", keys[0] == "city", f"First field: {keys[0]}")
    check("Compound index includes status", "status" in keys)
    check("Compound index includes order_date", "order_date" in keys)
else:
    check("Compound index structure", False, "Index not found")

# ══════════════════════════════════════════════════════════════════════════════
section("3. QUERIES AUDIT — Live Execution")
# ══════════════════════════════════════════════════════════════════════════════
from src.queries.catalog import QUERY_CATALOG, execute_query

check("5 Queries defined", len(QUERY_CATALOG) >= 5, f"Found: {len(QUERY_CATALOG)}")
print(f"  Queries: {list(QUERY_CATALOG.keys())}")

for qname in QUERY_CATALOG:
    try:
        result = execute_query(db, qname)
        is_list = isinstance(result.get("results"), list)
        check(f"Query '{qname}' executes", is_list,
              f"Returned {result.get('count', 0)} docs")
    except Exception as e:
        check(f"Query '{qname}' executes", False, str(e))

# Test queries work with different parameter values (data independence)
try:
    # Get actual cities from DB instead of hardcoding
    cities = db.orders_validated.distinct("city")
    if cities:
        r = execute_query(db, "orders_by_city_and_status", {"city": cities[0], "limit": 10})
        check("Query works with dynamic city value", isinstance(r["results"], list),
              f"City: {cities[0]}, found {r['count']} docs")
    statuses = db.orders_validated.distinct("status")
    if statuses:
        r = execute_query(db, "orders_by_payment_status",
                          {"payment_status": db.orders_validated.distinct("payment_status")[0]})
        check("Query works with dynamic payment_status", isinstance(r["results"], list),
              f"Found {r['count']} docs")
except Exception as e:
    check("Query data-independence test", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
section("4. EXPLAIN — Before & After Index Comparison")
# ══════════════════════════════════════════════════════════════════════════════
from src.database.explain import run_explain_benchmark, save_explain_report

try:
    results = run_explain_benchmark(db)
    check("Explain benchmark ran", len(results) == 3, f"Got {len(results)} comparisons")
    save_explain_report(results)

    for r in results:
        before_scan = r.before.has_collscan
        after_ixscan = r.after.has_ixscan
        check(f"Explain '{r.query_name}': Before was COLLSCAN", before_scan,
              f"before.has_collscan={before_scan}")
        check(f"Explain '{r.query_name}': After is IXSCAN", after_ixscan,
              f"after.has_ixscan={after_ixscan}")
        reduction = r.docs_examined_ratio
        check(f"Explain '{r.query_name}': Docs examined reduced",
              r.after.total_docs_examined <= r.before.total_docs_examined,
              f"Before={r.before.total_docs_examined}, After={r.after.total_docs_examined}, ratio={reduction}x")
except Exception as e:
    check("Explain benchmark", False, traceback.format_exc())

# ══════════════════════════════════════════════════════════════════════════════
section("5. AGGREGATION REPORTS — Live Execution")
# ══════════════════════════════════════════════════════════════════════════════
from src.analytics.catalog import AGGREGATION_CATALOG, execute_aggregation

check("5 Aggregations defined", len(AGGREGATION_CATALOG) >= 5,
      f"Found: {len(AGGREGATION_CATALOG)}")
print(f"  Aggregations: {list(AGGREGATION_CATALOG.keys())}")

for aname in AGGREGATION_CATALOG:
    try:
        result = execute_aggregation(db, aname)
        is_list = isinstance(result.get("results"), list)
        has_data = result.get("count", 0) > 0
        check(f"Aggregation '{aname}' returns data", is_list and has_data,
              f"count={result.get('count', 0)}")
        # JSON serializable test
        try:
            json.dumps(result)
            check(f"Aggregation '{aname}' is JSON serializable", True)
        except Exception as je:
            check(f"Aggregation '{aname}' is JSON serializable", False, str(je))
    except Exception as e:
        check(f"Aggregation '{aname}'", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
section("6. MATERIALIZED VIEWS — Existence & Content")
# ══════════════════════════════════════════════════════════════════════════════
from src.materialized_views.manager import (
    DAILY_SALES_MV, TOP_PRODUCTS_MV,
    refresh_materialized_views, get_materialized_view_data
)

daily_count = db[DAILY_SALES_MV].count_documents({})
products_count = db[TOP_PRODUCTS_MV].count_documents({})

check("mv_daily_sales_summary has data", daily_count > 0, f"{daily_count} date-rows")
check("mv_top_products_summary has data", products_count > 0, f"{products_count} product-rows")

# Verify structure of daily MV
daily_doc = db[DAILY_SALES_MV].find_one()
if daily_doc:
    required_fields = {"date", "total_orders", "total_revenue", "avg_order_value"}
    has_fields = required_fields.issubset(set(daily_doc.keys()))
    check("Daily MV has required fields", has_fields,
          f"Fields: {sorted(daily_doc.keys())}")

products_doc = db[TOP_PRODUCTS_MV].find_one()
if products_doc:
    required_fields = {"sku", "total_revenue", "orders_count"}
    has_fields = required_fields.issubset(set(products_doc.keys()))
    check("Products MV has required fields", has_fields,
          f"Fields: {sorted(products_doc.keys())}")

# ══════════════════════════════════════════════════════════════════════════════
section("7. INCREMENTAL REFRESH — Full Scenario Test")
# ══════════════════════════════════════════════════════════════════════════════

# Step 1: Record current state
initial_count = db.orders_validated.count_documents({})
initial_daily = daily_count

# Step 2: Force rebuild to set clean watermark
r1 = refresh_materialized_views(db, force_rebuild=True)
check("Initial build succeeds", r1["status"] == "success",
      f"mode={r1.get('mode')}, dates={r1.get('updated_dates')}")

# Step 3: Call again with NO new data — must return up_to_date
r2 = refresh_materialized_views(db)
check("Second call with no new data returns up_to_date",
      r2["status"] == "up_to_date" and r2.get("records_processed", 0) == 0,
      f"status={r2['status']}, processed={r2.get('records_processed')}")

# Step 4: Insert a synthetic test order and verify incremental picks it up
import uuid
from datetime import datetime, UTC
test_order_id = f"TEST-AUDIT-{uuid.uuid4().hex[:8]}"
test_doc = {
    "order_id": test_order_id,
    "customer_id": "عميل-AUDIT",
    "customer_name": "مستخدم التدقيق",
    "customer_phone": "700000000",
    "customer_email": "audit@test.com",
    "city": "صنعاء",
    "district": "تدقيق",
    "order_date": "2025-06-01T10:00:00",
    "status": "مؤكد",
    "delivery_type": "عادي",
    "delivery_cost": 1000.0,
    "payment_method": "نقدًا عند التسليم",
    "payment_status": "بانتظار الدفع",
    "payment_amount": 99999.0,
    "currency": "YER",
    "total_amount": 99999.0,
    "items_json": [{"sku": "SKU-AUDIT", "name": "منتج تدقيق", "qty": 1, "unit_price": 99999.0, "total": 99999.0}],
    "quality_status": "valid",
    "corrections": [],
    "raw_record": {"order_id": test_order_id},
    "source": {"source_file": "audit_test", "source_row_number": 0, "engine_used": "audit"},
    "run_id": "audit-run",
    "version": 1,
}
try:
    db.orders_validated.insert_one(test_doc)
    new_count = db.orders_validated.count_documents({})
    check("Test audit doc inserted", new_count == initial_count + 1,
          f"count: {initial_count} -> {new_count}")

    r3 = refresh_materialized_views(db)
    check("Incremental refresh detects new record",
          r3.get("records_processed", 0) > 0,
          f"mode={r3.get('mode')}, processed={r3.get('records_processed')}")
    check("Incremental refresh does NOT rebuild all",
          r3.get("mode") == "incremental_refresh",
          f"mode={r3.get('mode')}")
finally:
    # Cleanup test record
    db.orders_validated.delete_one({"order_id": test_order_id})
    check("Test audit doc cleaned up",
          db.orders_validated.count_documents({"order_id": test_order_id}) == 0)

# ══════════════════════════════════════════════════════════════════════════════
section("8. SCHEDULED JOBS — Live Execution & Failure Test")
# ══════════════════════════════════════════════════════════════════════════════
from src.jobs.tasks import JOB_REGISTRY, run_job_by_name
from src.jobs.logger import get_job_execution_history

check("2 Jobs defined", len(JOB_REGISTRY) >= 2,
      f"Jobs: {list(JOB_REGISTRY.keys())}")

for jname, jdef in JOB_REGISTRY.items():
    check(f"Job '{jname}' has schedule", jdef.schedule_interval_seconds > 0,
          f"interval={jdef.schedule_interval_seconds}s ({jdef.schedule_human})")

# Run both jobs manually
for jname in JOB_REGISTRY:
    try:
        result = run_job_by_name(db, jname, trigger_type="audit_test")
        check(f"Job '{jname}' runs manually", result["status"] == "SUCCESS",
              f"duration={result['duration_seconds']}s")
        check(f"Job '{jname}' logs started_at", bool(result.get("started_at")))
        check(f"Job '{jname}' logs finished_at", bool(result.get("finished_at")))
        check(f"Job '{jname}' logs status", bool(result.get("status")))
        check(f"Job '{jname}' logs log_id", bool(result.get("log_id")))
    except Exception as e:
        check(f"Job '{jname}' runs manually", False, str(e))

# Verify execution logs in MongoDB
logs = get_job_execution_history(db, limit=10)
check("Job execution logs stored in MongoDB", len(logs) >= 2,
      f"Found {len(logs)} log entries")
if logs:
    required_log_fields = {"job_name", "started_at", "finished_at", "status", "duration_seconds"}
    check("Log entries have required fields",
          required_log_fields.issubset(set(logs[0].keys())),
          f"Fields: {sorted(logs[0].keys())}")

# Stage 13: Job Failure Test — simulate failure safely
from src.jobs.tasks import JobDefinition
def failing_job_handler(db):
    raise RuntimeError("Simulated transient network timeout")

JOB_REGISTRY["audit_failing_job"] = JobDefinition(
    name="audit_failing_job",
    description="Simulated failing job for audit verification",
    schedule_interval_seconds=60,
    schedule_human="Every minute",
    handler=failing_job_handler,
)
try:
    fail_res = run_job_by_name(db, "audit_failing_job", trigger_type="failure_test")
    check("Job failure handled safely without crash", fail_res["status"] == "FAILED",
          f"status={fail_res.get('status')}")
    check("Job failure logs started_at", bool(fail_res.get("started_at")))
    check("Job failure logs finished_at", bool(fail_res.get("finished_at")))
    check("Job failure logs error message", "Simulated transient network timeout" in (fail_res.get("error") or ""))
    persisted_fail_log = db["job_execution_logs"].find_one({"job_name": "audit_failing_job"})
    check("Job failure log persisted in MongoDB with status=FAILED",
          persisted_fail_log is not None and persisted_fail_log.get("status") == "FAILED")
finally:
    db["job_execution_logs"].delete_many({"job_name": "audit_failing_job"})
    del JOB_REGISTRY["audit_failing_job"]

# ══════════════════════════════════════════════════════════════════════════════
section("9. FASTAPI — All Endpoints Live Test")
# ══════════════════════════════════════════════════════════════════════════════
import warnings
warnings.filterwarnings("ignore")
from fastapi.testclient import TestClient
from src.api.app import app

with TestClient(app) as client_api:
    # GET /health
    r = client_api.get("/health")
    check("GET /health → 200", r.status_code == 200, r.text[:100])
    check("GET /health has status=ok", r.json().get("status") == "ok")

    # POST /indexes
    r = client_api.post("/indexes")
    check("POST /indexes → 200", r.status_code == 200, r.text[:100])
    check("POST /indexes returns 3 indexes", r.json().get("total_indexes") == 3)

    # GET /queries
    r = client_api.get("/queries")
    check("GET /queries → 200", r.status_code == 200)
    check("GET /queries returns 5+", len(r.json().get("queries", [])) >= 5,
          str(r.json().get("queries")))

    # GET /queries/{name} — all 5
    for qname in QUERY_CATALOG:
        r = client_api.get(f"/queries/{qname}")
        check(f"GET /queries/{qname} → 200", r.status_code == 200, r.text[:80])

    # GET /aggregations
    r = client_api.get("/aggregations")
    check("GET /aggregations → 200", r.status_code == 200)
    check("GET /aggregations returns 5+", len(r.json().get("aggregations", [])) >= 5)

    # GET /aggregations/{name} — all 5
    for aname in AGGREGATION_CATALOG:
        r = client_api.get(f"/aggregations/{aname}")
        check(f"GET /aggregations/{aname} → 200", r.status_code == 200, r.text[:80])

    # POST /refresh-mv
    r = client_api.post("/refresh-mv")
    check("POST /refresh-mv → 200", r.status_code == 200, r.text[:100])

    # GET /jobs
    r = client_api.get("/jobs")
    check("GET /jobs → 200", r.status_code == 200)
    check("GET /jobs returns 2+ jobs", r.json().get("total_jobs", 0) >= 2)

    # POST /jobs/{name}/run
    for jname in JOB_REGISTRY:
        r = client_api.post(f"/jobs/{jname}/run")
        check(f"POST /jobs/{jname}/run → 200", r.status_code == 200, r.text[:80])

    # POST /ingest
    r = client_api.post("/ingest", json={"file_path": "data/orders_sample.csv"})
    check("POST /ingest → 200", r.status_code == 200, r.text[:100])
    check("POST /ingest returns metrics", "metrics" in r.json())

    # ── Error cases ────────────────────────────────────────────────────────────
    section("10. API ERROR CASES")

    r = client_api.get("/queries/invalid_query_xyz")
    check("GET /queries/invalid → 404", r.status_code == 404, r.text[:80])

    r = client_api.get("/aggregations/invalid_agg_xyz")
    check("GET /aggregations/invalid → 404", r.status_code == 404, r.text[:80])

    r = client_api.post("/jobs/invalid_job_xyz/run")
    check("POST /jobs/invalid/run → 404", r.status_code == 404, r.text[:80])

    r = client_api.post("/ingest", json={"file_path": "data/does_not_exist.csv"})
    check("POST /ingest missing file → 404", r.status_code == 404, r.text[:80])

    # ── JSON Serialization Check ───────────────────────────────────────────────
    section("11. JSON SERIALIZATION AUDIT")

    for aname in AGGREGATION_CATALOG:
        r = client_api.get(f"/aggregations/{aname}")
        try:
            data = r.json()
            json.dumps(data)
            check(f"JSON serializable: /aggregations/{aname}", True)
        except Exception as e:
            check(f"JSON serializable: /aggregations/{aname}", False, str(e))

    for qname in QUERY_CATALOG:
        r = client_api.get(f"/queries/{qname}")
        try:
            data = r.json()
            json.dumps(data)
            check(f"JSON serializable: /queries/{qname}", True)
        except Exception as e:
            check(f"JSON serializable: /queries/{qname}", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
section("12. SECURITY AUDIT")
# ══════════════════════════════════════════════════════════════════════════════
import subprocess

# Check .env file not committed
gitignore = Path(ROOT / ".gitignore").read_text() if Path(ROOT / ".gitignore").exists() else ""
check(".env in .gitignore", ".env" in gitignore)

# Check env.example has no real secrets
env_example = Path(ROOT / "env.example").read_text() if Path(ROOT / "env.example").exists() else ""
has_real_secrets = any(kw in env_example.lower() for kw in
                        ["password=abc", "secret=xyz", "apikey=", "token=real"])
check("env.example has no real secrets", not has_real_secrets)
check("env.example exists", Path(ROOT / "env.example").exists())

# Search for hardcoded credentials
result = subprocess.run(
    ["grep", "-r", "--include=*.py", "-l", "password.*=.*[a-zA-Z0-9]{8,}", "src/"],
    capture_output=True, text=True, cwd=ROOT
)
check("No hardcoded passwords in src/", result.returncode != 0 or not result.stdout.strip(),
      result.stdout.strip() or "clean")

# ══════════════════════════════════════════════════════════════════════════════
section("13. DOCUMENTATION AUDIT")
# ══════════════════════════════════════════════════════════════════════════════
readme = Path(ROOT / "README.md")
check("README.md exists", readme.exists())
if readme.exists():
    content = readme.read_text()
    check("README has Phase 2 content", "Phase 2" in content or "FastAPI" in content or "queries" in content.lower())

check("requirements.txt exists", Path(ROOT / "requirements.txt").exists())
if Path(ROOT / "requirements.txt").exists():
    reqs = Path(ROOT / "requirements.txt").read_text()
    check("requirements.txt has fastapi", "fastapi" in reqs.lower())
    check("requirements.txt has pymongo", "pymongo" in reqs.lower())
    check("requirements.txt has uvicorn", "uvicorn" in reqs.lower())
    check("requirements.txt has apscheduler", "apscheduler" in reqs.lower())

# ══════════════════════════════════════════════════════════════════════════════
section("14. MIDTERM REGRESSION TEST")
# ══════════════════════════════════════════════════════════════════════════════
import subprocess
result = subprocess.run(
    [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "-q"],
    capture_output=True, text=True, cwd=ROOT
)
passed = "passed" in result.stdout
failed = "failed" in result.stdout
check("All existing tests pass", passed and not failed,
      result.stdout.split("\n")[-2] if result.stdout else result.stderr[:200])
if failed:
    print("  FAILURES:", result.stdout)

# ══════════════════════════════════════════════════════════════════════════════
section("=== FINAL REQUIREMENT MATRIX ===")
# ══════════════════════════════════════════════════════════════════════════════
print()
total = len(RESULTS)
passed_count = sum(1 for v in RESULTS.values() if v["pass"])
failed_count = total - passed_count

print(f"  TOTAL CHECKS : {total}")
print(f"  PASSED       : {passed_count}")
print(f"  FAILED       : {failed_count}")
print()
if failed_count > 0:
    print("  ❌ FAILED CHECKS:")
    for k, v in RESULTS.items():
        if not v["pass"]:
            print(f"     - {k}: {v['detail']}")
else:
    print("  🎉 ALL CHECKS PASSED!")

sys.exit(0 if failed_count == 0 else 1)
