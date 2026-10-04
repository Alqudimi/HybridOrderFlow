from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from pymongo.database import Database

from src.database.indexes import (
    create_database_indexes,
    drop_database_indexes,
)

logger = logging.getLogger(__name__)


@dataclass
class ExplainMetrics:
    execution_time_millis: int
    total_docs_examined: int
    total_keys_examined: int
    n_returned: int
    winning_plan_stage: str
    has_collscan: bool
    has_ixscan: bool
    has_in_memory_sort: bool


@dataclass
class QueryComparisonResult:
    query_name: str
    index_name: str
    index_type: str
    filter_spec: dict[str, Any]
    sort_spec: dict[str, int]
    before: ExplainMetrics
    after: ExplainMetrics
    docs_examined_ratio: float
    analysis_notes: str


def _traverse_stages(plan: dict[str, Any], stages: list[str]) -> None:
    if not isinstance(plan, dict):
        return
    stage = plan.get("stage")
    if stage:
        stages.append(stage)
    if "inputStage" in plan:
        _traverse_stages(plan["inputStage"], stages)
    if "inputStages" in plan and isinstance(plan["inputStages"], list):
        for sub in plan["inputStages"]:
            _traverse_stages(sub, stages)


def _extract_metrics(raw_explain: dict[str, Any]) -> ExplainMetrics:
    execution_stats = raw_explain.get("executionStats", {})
    query_planner = raw_explain.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    stages: list[str] = []
    _traverse_stages(winning_plan, stages)

    exec_stages = execution_stats.get("executionStages", {})
    _traverse_stages(exec_stages, stages)

    return ExplainMetrics(
        execution_time_millis=int(execution_stats.get("executionTimeMillis", 0)),
        total_docs_examined=int(execution_stats.get("totalDocsExamined", 0)),
        total_keys_examined=int(execution_stats.get("totalKeysExamined", 0)),
        n_returned=int(execution_stats.get("nReturned", 0)),
        winning_plan_stage=winning_plan.get("stage", "UNKNOWN"),
        has_collscan="COLLSCAN" in stages,
        has_ixscan="IXSCAN" in stages,
        has_in_memory_sort="SORT" in stages,
    )


def run_explain_benchmark(db: Database) -> list[QueryComparisonResult]:
    """Executes a rigorous before-and-after explain('executionStats') benchmark for 3 core queries.

    Measures real execution stats:
    - totalDocsExamined
    - totalKeysExamined
    - executionTimeMillis
    - winningPlan stages (COLLSCAN vs IXSCAN)
    - in-memory SORT elimination
    """
    benchmarks_spec = [
        {
            "query_name": "orders_by_customer",
            "index_name": "idx_customer_id",
            "index_type": "single",
            "filter": {"customer_id": "عميل-1"},
            "sort": {"order_date": -1},
            "note": "Single-field index eliminates COLLSCAN, scanning only matching customer keys.",
        },
        {
            "query_name": "orders_by_city_and_status",
            "index_name": "idx_city_status_order_date",
            "index_type": "compound (ESR pattern)",
            "filter": {"city": "صنعاء", "status": "قيد الانتظار"},
            "sort": {"order_date": -1},
            "note": "Compound ESR index eliminates both COLLSCAN and in-memory SORT stage.",
        },
        {
            "query_name": "orders_containing_item_sku",
            "index_name": "idx_items_sku",
            "index_type": "multikey",
            "filter": {"items_json.sku": "SKU-1010"},
            "sort": {"order_date": -1},
            "note": "Multikey B-Tree index indexes nested array elements, avoiding full array traversals.",
        },
    ]

    # Step 1: Drop indexes to capture pure "before" stats
    drop_database_indexes(db)

    before_metrics: dict[str, ExplainMetrics] = {}
    for spec in benchmarks_spec:
        cmd = {
            "find": "orders_validated",
            "filter": spec["filter"],
            "sort": spec["sort"],
        }
        raw_explain = db.command("explain", cmd, verbosity="executionStats")
        before_metrics[spec["query_name"]] = _extract_metrics(raw_explain)

    # Step 2: Create indexes
    create_database_indexes(db)

    # Step 3: Capture pure "after" stats
    results: list[QueryComparisonResult] = []
    for spec in benchmarks_spec:
        q_name = spec["query_name"]
        cmd = {
            "find": "orders_validated",
            "filter": spec["filter"],
            "sort": spec["sort"],
        }
        raw_explain = db.command("explain", cmd, verbosity="executionStats")
        after = _extract_metrics(raw_explain)
        before = before_metrics[q_name]

        ratio = (
            round(before.total_docs_examined / after.total_docs_examined, 2)
            if after.total_docs_examined > 0
            else float("inf")
        )

        results.append(
            QueryComparisonResult(
                query_name=q_name,
                index_name=spec["index_name"],
                index_type=spec["index_type"],
                filter_spec=spec["filter"],
                sort_spec=spec["sort"],
                before=before,
                after=after,
                docs_examined_ratio=ratio,
                analysis_notes=spec["note"],
            )
        )

    return results


def save_explain_report(
    results: list[QueryComparisonResult],
    output_json_path: Path = Path("reports/index_explain_benchmark.json"),
    output_md_path: Path = Path("reports/index_explain_benchmark.md"),
) -> None:
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = [asdict(r) for r in results]
    with output_json_path.open("w", encoding="utf-8") as handle:
        json.dump(serialized, handle, indent=2, ensure_ascii=False)

    md_lines = [
        "# MongoDB Index Strategy & ExecutionStats Benchmark Report",
        "",
        "This report documents actual MongoDB execution metrics comparing performance before and after index creation.",
        "",
        "| Query Name | Index Name | Index Type | Docs Examined (Before -> After) | Keys Examined (Before -> After) | Plan Stage (Before -> After) | IXSCAN Used? | In-Memory Sort Eliminated? |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for r in results:
        b = r.before
        a = r.after
        sort_eliminated = b.has_in_memory_sort and not a.has_in_memory_sort
        md_lines.append(
            f"| `{r.query_name}` | `{r.index_name}` | {r.index_type} | "
            f"{b.total_docs_examined} -> **{a.total_docs_examined}** | "
            f"{b.total_keys_examined} -> **{a.total_keys_examined}** | "
            f"{b.winning_plan_stage} ({'COLLSCAN' if b.has_collscan else ''}) -> **{a.winning_plan_stage} ({'IXSCAN' if a.has_ixscan else ''})** | "
            f"{'✅ Yes' if a.has_ixscan else '❌ No'} | "
            f"{'✅ Yes' if sort_eliminated else ('N/A' if not b.has_in_memory_sort else 'No')} |"
        )

    md_lines.extend(
        [
            "",
            "## Deep Analysis & Architectural Rationales",
            "",
        ]
    )

    for r in results:
        md_lines.extend(
            [
                f"### Query: `{r.query_name}`",
                f"- **Index Created**: `{r.index_name}` ({r.index_type})",
                f"- **Filter Specification**: `{r.filter_spec}`",
                f"- **Sort Specification**: `{r.sort_spec}`",
                f"- **Total Docs Examined**: Before = `{r.before.total_docs_examined}`, After = `{r.after.total_docs_examined}` (Optimization: **{r.docs_examined_ratio}x** reduction)",
                f"- **Total Keys Examined**: Before = `{r.before.total_keys_examined}`, After = `{r.after.total_keys_examined}`",
                f"- **Execution Time (ms)**: Before = `{r.before.execution_time_millis} ms`, After = `{r.after.execution_time_millis} ms`",
                f"- **Stage Progression**: Before had `COLLSCAN={r.before.has_collscan}`, `SORT={r.before.has_in_memory_sort}` -> After has `IXSCAN={r.after.has_ixscan}`, `SORT={r.after.has_in_memory_sort}`",
                f"- **Engineering Assessment**: {r.analysis_notes}",
                "",
            ]
        )

    with output_md_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(md_lines))


if __name__ == "__main__":
    import pymongo
    from config.settings import Settings

    settings = Settings.from_env()
    client = pymongo.MongoClient(settings.mongo_uri)
    db = client[settings.mongo_database]
    print("Running Index Explain Benchmark on MongoDB...")
    benchmark_results = run_explain_benchmark(db)
    save_explain_report(benchmark_results)
    print("Benchmark complete! Saved to reports/index_explain_benchmark.json and .md")
