from pathlib import Path

import pytest

from config.settings import Settings
from src.elt_pipeline import run_python_elt
from src.file_router import choose_engine, inspect_file
from src.mongo_aggregation import (
    latest_version_per_order_pipeline,
    quality_error_counts_pipeline,
    validated_status_counts_pipeline,
)
from src.repositories import InMemoryOrdersRepository
from src.smart_poller import InMemoryPollingStateStore, SmartPoller


def test_aggregation_pipelines_use_server_side_stages() -> None:
    errors = quality_error_counts_pipeline("run-1")
    assert errors[0] == {"$match": {"run_id": "run-1"}}
    assert "$unwind" in errors[1]
    assert "$group" in errors[2]
    assert "$sort" in errors[3]
    assert "$group" in validated_status_counts_pipeline()[0]
    latest = latest_version_per_order_pipeline()
    assert latest[0]["$sort"]["version"] == -1
    assert latest[1]["$group"]["latest"]["$first"] == "$$ROOT"


def test_smart_poller_skips_same_source_and_old_watermark(tmp_path: Path) -> None:
    source = tmp_path / "delta.csv"
    source.write_text("order_id,version\norder-1,4\n", encoding="utf-8")
    store = InMemoryPollingStateStore()
    poller = SmartPoller(store)

    first = poller.poll(source)
    assert first.should_process is True
    poller.mark_success(first)

    same = poller.poll(source)
    assert same.should_process is False
    assert "unchanged" in same.reason

    source.write_text(
        "order_id,version\norder-1,3\norder-2,4\n", encoding="utf-8"
    )
    old = poller.poll(source)
    assert old.should_process is False
    assert "not newer" in old.reason


def test_smart_poller_accepts_newer_version(tmp_path: Path) -> None:
    source = tmp_path / "delta.csv"
    source.write_text("order_id,version\norder-1,1\n", encoding="utf-8")
    store = InMemoryPollingStateStore()
    poller = SmartPoller(store)
    first = poller.poll(source)
    poller.mark_success(first)

    source.write_text(
        "order_id,version\norder-1,2\norder-2,2\n", encoding="utf-8"
    )
    newer = poller.poll(source)
    assert newer.should_process is True
    assert newer.snapshot.max_version == 2


def test_smart_poller_uses_configured_version_field(tmp_path: Path) -> None:
    source = tmp_path / "delta.csv"
    source.write_text("order_id,revision\norder-1,7\n", encoding="utf-8")
    decision = SmartPoller(
        InMemoryPollingStateStore(), version_field="revision"
    ).poll(source)
    assert decision.snapshot.max_version == 7


def test_latest_version_pipeline_accepts_custom_version_field() -> None:
    pipeline = latest_version_per_order_pipeline("revision")
    assert pipeline[0]["$sort"]["revision"] == -1


def test_idempotency_key_deduplicates_replayed_batches() -> None:
    repository = InMemoryOrdersRepository()
    document = {"run_id": "run-1", "source_row_number": 2}

    assert repository.insert_raw_batch([document], request_key="raw-request")
    assert not repository.insert_raw_batch([document], request_key="raw-request")
    assert len(repository.orders_raw) == 1

    business = {
        "order_id": "order-1",
        "customer_id": "customer-1",
        "quality_status": "valid",
    }
    first = repository.upsert_validated(
        [business], request_key="validated-request"
    )
    replay = repository.upsert_validated(
        [business], request_key="validated-request"
    )
    assert first.inserted_count == 1
    assert replay.unchanged_count == 1
    assert repository.count_business_records() == 1


def test_older_version_cannot_replace_newer_business_state() -> None:
    repository = InMemoryOrdersRepository()
    newer = {
        "order_id": "order-1",
        "customer_id": "customer-1",
        "quality_status": "corrected",
        "total_amount": 200,
        "version": 2,
    }
    older = {**newer, "total_amount": 100, "version": 1}

    assert repository.upsert_validated([newer]).inserted_count == 1
    result = repository.upsert_validated([older])
    assert result.unchanged_count == 1
    assert repository.get_validated("order-1")["total_amount"] == 200


def test_unversioned_record_cannot_replace_versioned_business_state() -> None:
    repository = InMemoryOrdersRepository()
    newer = {
        "order_id": "order-1",
        "customer_id": "customer-1",
        "quality_status": "corrected",
        "total_amount": 200,
        "version": 2,
    }
    unversioned = {
        "order_id": "order-1",
        "customer_id": "customer-1",
        "quality_status": "valid",
        "total_amount": 100,
    }

    repository.upsert_validated([newer])
    result = repository.upsert_validated([unversioned])

    assert result.unchanged_count == 1
    assert repository.get_validated("order-1")["total_amount"] == 200


def test_interrupted_run_resumes_after_checkpoint(tmp_path: Path) -> None:
    source = tmp_path / "orders.csv"
    headers = (
        "order_id,order_date,customer_id,customer_email,delivery_cost,"
        "total_amount,items_json\n"
    )
    rows = "".join(
        f"order-{index},2025-01-31,customer-{index},user{index}@example.com,0,"
        f"10,\"[{{\"\"qty\"\":1,\"\"unit_price\"\":10}}]\"\n"
        for index in range(1, 5)
    )
    source.write_text(headers + rows, encoding="utf-8")

    settings = Settings(batch_size=2, results_path=tmp_path / "results.json")
    repository = InMemoryOrdersRepository()
    state_store = InMemoryPollingStateStore()
    poller = SmartPoller(state_store, lease_seconds=0)
    first_decision = poller.poll(source)
    assert first_decision.should_process
    run_id = first_decision.claim_id
    decision = choose_engine(inspect_file(source), settings)

    def stop_after_first_checkpoint(
        row_number: int, counters: dict[str, object]
    ) -> None:
        poller.mark_progress(first_decision, row_number, counters)
        raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        run_python_elt(
            decision,
            run_id,
            settings,
            repository,
            progress_callback=stop_after_first_checkpoint,
        )

    resumed_decision = poller.poll(source)
    assert resumed_decision.should_process
    assert resumed_decision.checkpoint["last_source_row_number"] == 3
    resumed = run_python_elt(
        decision,
        resumed_decision.claim_id,
        settings,
        repository,
        resume_after_source_row=3,
        checkpoint=resumed_decision.checkpoint,
        progress_callback=lambda row, counters: poller.mark_progress(
            resumed_decision, row, counters
        ),
    )
    poller.mark_success(resumed_decision)

    assert resumed.consistency_passed
    assert repository.count_business_records() == 4
    assert len(repository.orders_raw) == 4