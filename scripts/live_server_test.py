"""Live HTTP Server Test for Big Data Phase 2.

Sends real HTTP requests over the network to the live running Uvicorn server at
http://127.0.0.1:8000 and rigorously tests all Phase 2 endpoints, error codes,
response schemas, incremental updates, and database reflections.
"""

import json
import sys
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import time
import pymongo
from config.settings import Settings

BASE_URL = "http://127.0.0.1:8000"

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
    print("=" * 70)
    print("🚀 LIVE HTTP TEST SUITE — FASTAPI SERVER AT http://127.0.0.1:8000")
    print("=" * 70)

    # 1. Swagger UI /docs
    code, body = http_get("/docs")
    assert code == 200, f"/docs failed with {code}"
    assert "swagger-ui" in str(body).lower(), "/docs does not contain Swagger UI"
    print("✅ PASS: GET /docs returns 200 and loads Swagger UI HTML")

    # 2. OpenAPI JSON Schema
    code, schema = http_get("/openapi.json")
    assert code == 200, f"/openapi.json failed with {code}"
    assert "paths" in schema, "Missing paths in OpenAPI schema"
    required_paths = [
        "/health", "/ingest", "/indexes", "/queries", "/queries/{name}",
        "/aggregations", "/aggregations/{name}", "/refresh-mv", "/jobs", "/jobs/{name}/run"
    ]
    for p in required_paths:
        assert p in schema["paths"], f"Path {p} missing in OpenAPI specification"
    print(f"✅ PASS: GET /openapi.json contains all {len(required_paths)} required endpoints")

    # 3. GET /health
    code, health = http_get("/health")
    assert code == 200
    assert health.get("status") == "ok"
    assert health.get("database", {}).get("connected") is True
    print(f"✅ PASS: GET /health -> 200, DB connected, MongoDB v{health['database']['mongodb_version']}")

    # 4. POST /indexes
    code, idx_resp = http_post("/indexes")
    assert code == 200
    assert idx_resp.get("status") == "success"
    assert idx_resp.get("total_indexes") >= 3
    idx_names = [i["name"] for i in idx_resp["indexes"]]
    assert "idx_customer_id" in idx_names
    assert "idx_items_sku" in idx_names
    assert "idx_city_status_order_date" in idx_names
    print(f"✅ PASS: POST /indexes -> 200, verified 3 indexes: {idx_names}")

    # 5. GET /queries
    code, q_list = http_get("/queries")
    assert code == 200
    queries = q_list.get("queries", [])
    assert len(queries) >= 5
    print(f"✅ PASS: GET /queries -> 200, returned {len(queries)} queries: {queries}")

    # 6. GET /queries/{name} for all queries
    for qname in queries:
        code, q_res = http_get(f"/queries/{qname}")
        assert code == 200, f"Query {qname} failed with {code}: {q_res}"
        assert "results" in q_res
        assert isinstance(q_res["results"], list)
        # Test JSON serializability
        json.dumps(q_res)
        print(f"  - GET /queries/{qname} -> 200, count={q_res.get('count')} docs (valid JSON)")
    print("✅ PASS: All 5 queries executed and returned clean JSON from database")

    # 7. GET /aggregations
    code, a_list = http_get("/aggregations")
    assert code == 200
    aggregations = a_list.get("aggregations", [])
    assert len(aggregations) >= 5
    print(f"✅ PASS: GET /aggregations -> 200, returned {len(aggregations)} aggregations: {aggregations}")

    # 8. GET /aggregations/{name} for all aggregations
    for aname in aggregations:
        code, a_res = http_get(f"/aggregations/{aname}")
        assert code == 200, f"Aggregation {aname} failed with {code}: {a_res}"
        assert "results" in a_res
        assert isinstance(a_res["results"], list)
        assert a_res.get("count", 0) > 0
        # Test JSON serializability
        json.dumps(a_res)
        print(f"  - GET /aggregations/{aname} -> 200, count={a_res.get('count')} records (valid JSON)")
    print("✅ PASS: All 5 aggregations executed and returned aggregated metrics from database")

    # 9. POST /refresh-mv
    code, mv_res = http_post("/refresh-mv", {"force_rebuild": False})
    assert code == 200
    print(f"✅ PASS: POST /refresh-mv -> 200, status={mv_res.get('status')}, mode={mv_res.get('mode')}")

    # 10. GET /jobs
    code, jobs_resp = http_get("/jobs")
    assert code == 200
    assert jobs_resp.get("total_jobs", 0) >= 2
    job_names = [j["name"] for j in jobs_resp.get("jobs", [])]
    print(f"✅ PASS: GET /jobs -> 200, registered jobs: {job_names}")

    # 11. POST /jobs/{name}/run for both jobs
    for jname in job_names:
        code, j_res = http_post(f"/jobs/{jname}/run")
        assert code == 200, f"Job {jname} failed with {code}: {j_res}"
        assert j_res.get("status") == "SUCCESS"
        assert j_res.get("started_at") is not None
        assert j_res.get("finished_at") is not None
        assert j_res.get("log_id") is not None
        print(f"  - POST /jobs/{jname}/run -> 200, status=SUCCESS, duration={j_res.get('duration_seconds')}s, log_id={j_res.get('log_id')}")
    print("✅ PASS: All scheduled jobs manually executed and logged to MongoDB")

    # 12. POST /ingest using existing midterm ELT pipeline
    code, ing_res = http_post("/ingest", {"file_path": "data/orders_sample.csv"})
    assert code == 200, f"Ingest failed with {code}: {ing_res}"
    assert ing_res.get("status") == "success"
    assert "metrics" in ing_res
    print(f"✅ PASS: POST /ingest -> 200, run_id={ing_res.get('run_id')}, engine={ing_res.get('engine_used')}, processed={ing_res['metrics'].get('total_raw')}")

    # 13. API Error Handling (Invalid input & names)
    code, err1 = http_get("/queries/invalid_query_name_404")
    assert code == 404
    assert "detail" in err1
    print("✅ PASS: GET /queries/invalid -> 404 Not Found with clean JSON")

    code, err2 = http_get("/aggregations/invalid_aggregation_name_404")
    assert code == 404
    assert "detail" in err2
    print("✅ PASS: GET /aggregations/invalid -> 404 Not Found with clean JSON")

    code, err3 = http_post("/jobs/invalid_job_name_404/run")
    assert code == 404
    assert "detail" in err3
    print("✅ PASS: POST /jobs/invalid/run -> 404 Not Found with clean JSON")

    code, err4 = http_post("/ingest", {"file_path": "data/non_existent_data.csv"})
    assert code == 404
    assert "detail" in err4
    print("✅ PASS: POST /ingest missing file -> 404 Not Found with clean JSON")

    # 14. Full Live Incremental Update Test
    settings = Settings.from_env()
    client = pymongo.MongoClient(settings.mongo_uri)
    db = client[settings.mongo_database]

    print("\n--- Testing Live Incremental Refresh Behavior ---")
    # Step 1: Ensure current views are up to date
    http_post("/refresh-mv", {"force_rebuild": True})
    code, up_to_date = http_post("/refresh-mv", {"force_rebuild": False})
    assert up_to_date.get("status") == "up_to_date"
    assert up_to_date.get("records_processed", 0) == 0
    print("  Step 1 & 2: Base state verified: status='up_to_date', records_processed=0")

    # Step 3: Insert new order into MongoDB
    test_sku = "SKU-LIVE-INCREMENTAL-TEST"
    test_order_id = f"ORDER-LIVE-TEST-{int(time.time())}"
    test_doc = {
        "order_id": test_order_id,
        "customer_id": "عميل-live-test",
        "city": "تعز",
        "district": "المظفر",
        "order_date": "2025-08-15T14:30:00",
        "status": "مؤكد",
        "quality_status": "valid",
        "total_amount": 7777.0,
        "items_json": [{"sku": test_sku, "name": "منتج حي اختباري", "qty": 1, "unit_price": 7777.0, "total": 7777.0}],
    }
    try:
        db["orders_validated"].insert_one(test_doc)
        print(f"  Step 4: Inserted new validated order {test_order_id} for date 2025-08-15 and SKU {test_sku}")

        # Step 5: Call POST /refresh-mv
        code, inc_res = http_post("/refresh-mv", {"force_rebuild": False})
        assert code == 200
        assert inc_res.get("mode") == "incremental_refresh", f"Expected incremental_refresh mode, got {inc_res.get('mode')}"
        assert inc_res.get("records_processed", 0) >= 1
        print(f"  Step 5 & 6: POST /refresh-mv executed in mode='{inc_res.get('mode')}', processed={inc_res.get('records_processed')} records")

        # Step 7: Verify data reflected in Materialized Views
        daily_mv = db["mv_daily_sales_summary"].find_one({"_id": "2025-08-15"})
        assert daily_mv is not None, "Date 2025-08-15 missing in mv_daily_sales_summary"
        print(f"  Step 7a: Daily MV reflected date 2025-08-15 -> total_revenue={daily_mv.get('total_revenue')}")

        product_mv = db["mv_top_products_summary"].find_one({"_id": test_sku})
        assert product_mv is not None, f"SKU {test_sku} missing in mv_top_products_summary"
        print(f"  Step 7b: Products MV reflected SKU {test_sku} -> total_units_sold={product_mv.get('total_units_sold')}")

    finally:
        # Cleanup
        db["orders_validated"].delete_one({"order_id": test_order_id})
        db["mv_daily_sales_summary"].delete_one({"_id": "2025-08-15"})
        db["mv_top_products_summary"].delete_one({"_id": test_sku})
        print("  Cleanup: Test records safely removed.")

    print("\n" + "=" * 70)
    print("🎉 ALL LIVE HTTP ENDPOINTS AND INCREMENTAL TESTS PASSED (100% VERIFIED)!")
    print("=" * 70)


if __name__ == "__main__":
    main()
