from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from pathlib import Path

if __package__ in (None, ""):
    # Support the README's direct command as well as `python -m src.main`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import Settings
from src.elt_pipeline import run_python_elt
from src.file_router import choose_engine, inspect_file
from src.mongo_setup import create_repository
from src.repositories import InMemoryOrdersRepository

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hybrid ELT orders pipeline: Python Batch or PySpark."
    )
    parser.add_argument("--input", required=True, help="Path to dirty CSV")
    parser.add_argument(
        "--force-engine",
        choices=("python_batch", "pyspark"),
        help="Demonstration override; omit for size-based routing",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--smart-poll",
        action="store_true",
        help="Skip unchanged or older sources using durable source state",
    )
    parser.add_argument(
        "--version-field",
        default="version",
        help="Delta watermark column used by --smart-poll",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Treat --input as a versioned Delta file (Path B)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Use an in-memory repository; useful for tests and demos",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    settings = Settings.from_env()
    settings.validate()
    file_info = inspect_file(args.input)
    decision = choose_engine(file_info, settings, args.force_engine)
    poller = None
    poll_decision = None
    run_id = args.run_id or str(uuid.uuid4())
    if args.smart_poll:
        from src.smart_poller import (
            InMemoryPollingStateStore,
            MongoPollingStateStore,
            SmartPoller,
        )

        poller_store = (
            InMemoryPollingStateStore()
            if args.dry_run
            else MongoPollingStateStore(
                settings.mongo_uri, settings.mongo_database
            )
        )
        poller = SmartPoller(
            poller_store,
            args.version_field,
            lease_seconds=settings.smart_poll_lease_seconds,
        )
        poll_decision = poller.poll(args.input)
        print(
            f"smart_poll={poll_decision.should_process} "
            f"reason={poll_decision.reason}"
        )
        if not poll_decision.should_process:
            poller_store.close()
            return 0
        # The claim id is stable for this source fingerprint/version. Reusing
        # it lets a restarted process find the same raw rows and checkpoint.
        run_id = poll_decision.claim_id or run_id

    print(
        f"file={decision.file.path} size_mb={decision.file.size_mb:.2f} "
        f"engine={decision.engine} reason={decision.reason} run_id={run_id}"
    )

    repository = InMemoryOrdersRepository() if args.dry_run else create_repository(settings)
    try:
        if args.incremental and decision.engine == "python_batch":
            from src.incremental_loader import load_delta
            from src.metrics import RunMetrics, write_results

            delta_started = time.perf_counter()
            delta_result = load_delta(
                args.input,
                run_id,
                repository,
                version_field=args.version_field,
                batch_size=settings.batch_size,
            )
            metrics = RunMetrics(
                run_id=run_id,
                file_name=decision.file.path.name,
                file_size_mb=decision.file.size_mb,
                engine_used="incremental_delta",
                rows_read=delta_result.rows_read,
                raw_loaded=delta_result.rows_read,
                valid_count=delta_result.valid_count,
                corrected_count=delta_result.corrected_count,
                quarantine_count=delta_result.quarantine_count,
                batch_size=settings.batch_size,
                batches=delta_result.batches,
                inserted_count=delta_result.inserted_count,
                updated_count=delta_result.updated_count,
                unchanged_count=delta_result.unchanged_count,
                error_case_counts=delta_result.error_case_counts,
            )
            metrics.finish(time.perf_counter() - delta_started)
            write_results(settings.results_path, metrics)
        elif decision.engine == "python_batch":
            progress_callback = None
            resume_after_source_row = None
            checkpoint = None
            if poller and poll_decision:
                checkpoint = poll_decision.checkpoint
                if checkpoint:
                    resume_after_source_row = int(
                        checkpoint["last_source_row_number"]
                    )
                progress_callback = (
                    lambda row, counters: poller.mark_progress(
                        poll_decision, row, counters
                    )
                )
            metrics = run_python_elt(
                decision,
                run_id,
                settings,
                repository,
                resume_after_source_row=resume_after_source_row,
                checkpoint=checkpoint,
                progress_callback=progress_callback,
            )
        else:
            from src.spark_loader import (
                run_spark_incremental_pipeline,
                run_spark_pipeline,
            )

            progress_callback = None
            if poller and poll_decision:
                progress_callback = (
                    lambda row, counters: poller.mark_progress(
                        poll_decision, row, counters
                    )
                )

            if args.incremental:
                spark_result = run_spark_incremental_pipeline(
                    decision,
                    run_id,
                    settings,
                    repository=repository,
                    version_field=args.version_field,
                    dry_run=args.dry_run,
                    progress_callback=progress_callback,
                )
            else:
                spark_result = run_spark_pipeline(
                    decision,
                    run_id,
                    settings,
                    repository=repository,
                    dry_run=args.dry_run,
                    progress_callback=progress_callback,
                )
            from src.metrics import RunMetrics, write_results

            metrics = RunMetrics(
                run_id=run_id,
                file_name=decision.file.path.name,
                file_size_mb=decision.file.size_mb,
                engine_used="pyspark" if not args.incremental else "pyspark_incremental",
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

            # Run aggregation pipelines for operational reporting when a
            # full MongoDB repository is available (not dry-run).
            if (
                not args.dry_run
                and hasattr(repository, "aggregate_quality_errors")
                and hasattr(repository, "aggregate_validated_statuses")
            ):
                try:
                    errors = repository.aggregate_quality_errors(run_id)
                    statuses = repository.aggregate_validated_statuses()
                    logger.info(
                        "Post-Spark aggregations: quality_errors=%s, statuses=%s",
                        errors[:3] if errors else [],
                        statuses,
                    )
                except Exception:
                    logger.warning(
                        "Post-Spark aggregation pipelines failed", exc_info=True
                    )
        if poller and poll_decision:
            poller.mark_success(poll_decision)
        print(metrics_summary(metrics))
        return 0
    finally:
        close = getattr(repository, "close", None)
        if close:
            close()
        if poller:
            poller.state_store.close()


def metrics_summary(metrics: object) -> str:
    values = metrics.to_dict()
    return (
        f"rows_read={values['rows_read']} raw_loaded={values['raw_loaded']} "
        f"valid={values['valid_count']} corrected={values['corrected_count']} "
        f"quarantine={values['quarantine_count']} "
        f"throughput={values['throughput']} records/s "
        f"consistency_passed={values['consistency_passed']}"
    )


if __name__ == "__main__":
    sys.exit(main())
