import csv
from pathlib import Path

from config.settings import Settings
from src.elt_pipeline import run_python_elt
from src.file_router import choose_engine, inspect_file
from src.repositories import InMemoryOrdersRepository


def test_missing_business_keys_are_quarantined() -> None:
    record = {
        "order_id": "",
        "customer_id": "",
        "order_date": "2025-01-31",
        "items_json": "[]",
    }
    from src.quality_rules import classify_record

    result = classify_record(record)
    assert result["quality_status"] == "quarantined"
    assert "MISSING_ORDER_ID" in result["error_codes"]
    assert "MISSING_CUSTOMER_ID" in result["error_codes"]


def test_duplicate_order_id_is_quarantined_with_explicit_reason() -> None:
    from src.quality_rules import classify_record

    result = classify_record(
        {
            "order_id": "same",
            "customer_id": "customer",
            "order_date": "2025-01-31",
            "items_json": '[{"qty": 1, "unit_price": 10}]',
            "delivery_cost": "0",
            "total_amount": "10",
        },
        duplicate_order_id=True,
    )
    assert result["quality_status"] == "quarantined"
    assert "DUPLICATE_ORDER_ID" in result["error_codes"]


def test_full_elt_is_consistent_and_idempotent(tmp_path: Path) -> None:
    input_path = Path("data/orders_sample.csv")
    settings = Settings(
        small_file_threshold_mb=200,
        batch_size=17,
        results_path=tmp_path / "test-results.json",
    )
    repository = InMemoryOrdersRepository()
    decision = choose_engine(inspect_file(input_path), settings)

    first = run_python_elt(decision, "run-1", settings, repository)
    first_business_count = repository.count_business_records()
    second = run_python_elt(decision, "run-2", settings, repository)

    assert first.consistency_passed is True
    assert second.consistency_passed is True
    assert first.raw_loaded == first.valid_count + first.corrected_count + first.quarantine_count
    assert second.raw_loaded == second.valid_count + second.corrected_count + second.quarantine_count
    assert repository.count_business_records() == first_business_count
    assert second.unchanged_count == first.valid_count + first.corrected_count
    assert len(repository.orders_raw) == first.raw_loaded + second.raw_loaded
    assert len(repository.orders_quarantine) == (
        first.quarantine_count + second.quarantine_count
    )


def _delta_record(order_id: str, total: str, version: str) -> dict[str, str]:
    return {
        "order_id": order_id,
        "order_date": "2025-01-31",
        "status": "مؤكد",
        "customer_id": "customer-1",
        "customer_name": "Customer",
        "customer_phone": "712345678",
        "customer_email": "customer@example.com",
        "city": "صنعاء",
        "district": "التحرير",
        "delivery_type": "عادي",
        "delivery_cost": "0",
        "payment_method": "بطاقة",
        "payment_status": "تم الدفع",
        "payment_amount": total,
        "currency": "YER",
        "total_amount": total,
        "items_json": '[{"sku":"SKU-1","name":"Item","qty":1,"unit_price":' + total + "}]",
        "version": version,
    }


def test_incremental_delta_insert_update_and_replay(tmp_path: Path) -> None:
    from src.incremental_loader import load_delta

    repository = InMemoryOrdersRepository()
    delta_path = tmp_path / "test-delta.csv"
    row = _delta_record("delta-1", "100", "1")
    with delta_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    first = load_delta(delta_path, "delta-run-1", repository)
    replay = load_delta(delta_path, "delta-run-2", repository)
    assert first.inserted_count == 1
    assert replay.unchanged_count == 1
    assert repository.count_business_records() == 1
