#!/usr/bin/env python3
"""Summarize repeated openFDA real-world benchmark runs.

This script validates that each run has a manifest, raw summary CSV, zero
semantic errors, and consistent warmup/repetition settings before producing a
small Markdown stability report.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


QUERY_ORDER = [
    "q1_recall_number_lookup",
    "q2_entity_uuid_lookup",
    "q3_activity_uuid_lookup",
    "q4_recalling_firm_lookup",
]


@dataclass(frozen=True)
class QueryRow:
    ok_count: int
    error_count: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float


@dataclass(frozen=True)
class RunEvidence:
    run_id: str
    manifest: dict[str, str]
    rows: dict[str, QueryRow]


def read_manifest(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def read_summary(path: Path) -> dict[str, QueryRow]:
    rows: dict[str, QueryRow] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rows[row["query"]] = QueryRow(
                ok_count=int(row["ok_count"]),
                error_count=int(row["error_count"]),
                mean_ms=float(row["mean_ms"]),
                p50_ms=float(row["p50_ms"]),
                p95_ms=float(row["p95_ms"]),
                p99_ms=float(row["p99_ms"]),
                min_ms=float(row["min_ms"]),
                max_ms=float(row["max_ms"]),
            )
    return rows


def load_run(results_dir: Path, run_id: str) -> RunEvidence:
    run_dir = results_dir / run_id
    manifest_path = run_dir / "manifest.txt"
    summary_path = run_dir / "openfda_food_latency_summary.csv"
    if not manifest_path.exists():
        raise FileNotFoundError(f"missing manifest: {manifest_path}")
    if not summary_path.exists():
        raise FileNotFoundError(f"missing summary: {summary_path}")
    return RunEvidence(
        run_id=run_id,
        manifest=read_manifest(manifest_path),
        rows=read_summary(summary_path),
    )


def validate_run(
    run: RunEvidence,
    required_warmup: int,
    required_reps: int,
    required_storage: str | None,
    require_clean: bool,
    min_usable_records: int,
) -> list[str]:
    errors: list[str] = []
    manifest_run_id = run.manifest.get("run_id", run.run_id)
    if manifest_run_id != run.run_id:
        errors.append(f"{run.run_id}: manifest run_id is {manifest_run_id}")
    if str(required_warmup) != run.manifest.get("warmup"):
        errors.append(f"{run.run_id}: warmup is {run.manifest.get('warmup')}, expected {required_warmup}")
    if str(required_reps) != run.manifest.get("reps"):
        errors.append(f"{run.run_id}: reps is {run.manifest.get('reps')}, expected {required_reps}")
    if run.manifest.get("total_errors") != "0":
        errors.append(f"{run.run_id}: total_errors is {run.manifest.get('total_errors')}, expected 0")
    if required_storage and run.manifest.get("datomic_storage") != required_storage:
        errors.append(
            f"{run.run_id}: datomic_storage is {run.manifest.get('datomic_storage')}, expected {required_storage}"
        )
    if require_clean and run.manifest.get("git_dirty") != "false":
        errors.append(f"{run.run_id}: git_dirty is {run.manifest.get('git_dirty')}, expected false")
    if run.manifest.get("source_endpoint") != "https://api.fda.gov/food/enforcement.json":
        errors.append(f"{run.run_id}: unexpected source endpoint {run.manifest.get('source_endpoint')}")

    try:
        usable_records = int(run.manifest.get("usable_records", "0"))
    except ValueError:
        usable_records = 0
    if usable_records < min_usable_records:
        errors.append(f"{run.run_id}: usable_records is {usable_records}, expected >= {min_usable_records}")

    missing = [query for query in QUERY_ORDER if query not in run.rows]
    if missing:
        errors.append(f"{run.run_id}: missing query rows: {', '.join(missing)}")

    for query, row in run.rows.items():
        if row.error_count != 0:
            errors.append(f"{run.run_id}: {query} error_count is {row.error_count}, expected 0")
        if row.ok_count != required_reps:
            errors.append(f"{run.run_id}: {query} ok_count is {row.ok_count}, expected {required_reps}")
    return errors


def fmt(value: float) -> str:
    return f"{value:.3f}"


def fmt_range(values: Iterable[float]) -> str:
    values = list(values)
    return f"{fmt(min(values))}--{fmt(max(values))}"


def optional_float_values(runs: list[RunEvidence], key: str) -> list[float]:
    values: list[float] = []
    for run in runs:
        value = run.manifest.get(key)
        if value:
            values.append(float(value))
    return values


def markdown_report(
    runs: list[RunEvidence],
    selected_run_id: str,
    required_warmup: int,
    required_reps: int,
    required_storage: str | None,
    require_clean: bool,
    min_usable_records: int,
) -> str:
    if selected_run_id not in {run.run_id for run in runs}:
        raise ValueError(f"selected run {selected_run_id} is not in summarized runs")

    commit_set = sorted({run.manifest.get("git_commit", "unknown") for run in runs})
    storage_set = sorted({run.manifest.get("datomic_storage", "unknown") for run in runs})
    ingest_values = optional_float_values(runs, "ingest_ms")
    source_updates = sorted({run.manifest.get("source_last_updated", "unknown") for run in runs})
    source_totals = sorted({run.manifest.get("source_total_available", "unknown") for run in runs})
    lines = [
        "# openFDA Real-World Benchmark Rerun Summary",
        "",
        f"- Included runs: {', '.join(run.run_id for run in runs)}",
        f"- Selected run for discussion: `{selected_run_id}`",
        f"- Inclusion policy: warmup={required_warmup}, reps={required_reps}, total_errors=0, query error_count=0",
        f"- Required Datomic storage: {required_storage or 'not enforced'}",
        f"- Observed Datomic storage value(s): {', '.join(storage_set)}",
        f"- Clean worktree required: {'yes' if require_clean else 'no'}",
        f"- Minimum usable source records per run: {min_usable_records}",
        f"- Source endpoint: `https://api.fda.gov/food/enforcement.json`",
        f"- Source last_updated value(s): {', '.join(source_updates)}",
        f"- Source total_available value(s): {', '.join(source_totals)}",
        f"- Source commit(s): {', '.join(f'`{commit}`' for commit in commit_set)}",
    ]
    if ingest_values:
        lines.append(f"- Ingest time range: {fmt_range(ingest_values)} ms")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This benchmark uses real openFDA Food Enforcement records and maps them",
            "into the Datomic PROV/traceability schema. It is a real-world recall/event",
            "lookup workload, not a complete farm-to-retail provenance-chain benchmark.",
            "",
            "## Per-Run Summary",
            "",
            "| Run | Q1 mean/p95 | Q2 mean/p95 | Q3 mean/p95 | Q4 mean/p95 | Max observed |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )

    for run in runs:
        cells = []
        max_observed = 0.0
        for query in QUERY_ORDER:
            row = run.rows[query]
            cells.append(f"{fmt(row.mean_ms)}/{fmt(row.p95_ms)} ms")
            max_observed = max(max_observed, row.max_ms)
        lines.append(f"| `{run.run_id}` | {' | '.join(cells)} | {fmt(max_observed)} ms |")

    lines.extend(
        [
            "",
            "## Across-Run Ranges",
            "",
            "| Query | Mean range (ms) | p95 range (ms) | p99 max (ms) | Max observed (ms) |",
            "|---|---:|---:|---:|---:|",
        ]
    )

    for query in QUERY_ORDER:
        rows = [run.rows[query] for run in runs]
        lines.append(
            "| {query} | {mean_range} | {p95_range} | {p99_max} | {max_observed} |".format(
                query=query,
                mean_range=fmt_range(row.mean_ms for row in rows),
                p95_range=fmt_range(row.p95_ms for row in rows),
                p99_max=fmt(max(row.p99_ms for row in rows)),
                max_observed=fmt(max(row.max_ms for row in rows)),
            )
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The results are suitable as external-validity evidence for real public",
            "recall/event lookup data after the raw run directories are archived with",
            "their manifests. They should not be used as product-comparison throughput",
            "evidence and should not be described as full supply-chain traversal.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=Path("benchmarks/real-world/results"))
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--selected-run", required=True)
    parser.add_argument("--required-warmup", type=int, default=30)
    parser.add_argument("--required-reps", type=int, default=100)
    parser.add_argument("--required-storage")
    parser.add_argument("--min-usable-records", type=int, default=1)
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runs = [load_run(args.results_dir, run_id) for run_id in args.runs]
    require_clean = not args.allow_dirty
    errors = [
        error
        for run in runs
        for error in validate_run(
            run=run,
            required_warmup=args.required_warmup,
            required_reps=args.required_reps,
            required_storage=args.required_storage,
            require_clean=require_clean,
            min_usable_records=args.min_usable_records,
        )
    ]
    if errors:
        for error in errors:
            print(f"error: {error}")
        return 1

    report = markdown_report(
        runs=runs,
        selected_run_id=args.selected_run,
        required_warmup=args.required_warmup,
        required_reps=args.required_reps,
        required_storage=args.required_storage,
        require_clean=require_clean,
        min_usable_records=args.min_usable_records,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
