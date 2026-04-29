#!/usr/bin/env python3
"""Summarize openFDA disk-backed scale benchmark runs."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path


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
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass(frozen=True)
class ScaleRun:
    run_id: str
    requested_limit: int
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
                p95_ms=float(row["p95_ms"]),
                p99_ms=float(row["p99_ms"]),
                max_ms=float(row["max_ms"]),
            )
    return rows


def load_run(results_dir: Path, run_id: str, requested_limit: int) -> ScaleRun:
    run_dir = results_dir / run_id
    return ScaleRun(
        run_id=run_id,
        requested_limit=requested_limit,
        manifest=read_manifest(run_dir / "manifest.txt"),
        rows=read_summary(run_dir / "openfda_food_latency_summary.csv"),
    )


def validate_run(
    run: ScaleRun,
    required_warmup: int,
    required_reps: int,
    required_storage: str,
    require_clean: bool,
) -> list[str]:
    errors: list[str] = []
    manifest = run.manifest
    if manifest.get("run_id") != run.run_id:
        errors.append(f"{run.run_id}: manifest run_id is {manifest.get('run_id')}")
    if manifest.get("limit") != str(run.requested_limit):
        errors.append(f"{run.run_id}: limit is {manifest.get('limit')}, expected {run.requested_limit}")
    if manifest.get("warmup") != str(required_warmup):
        errors.append(f"{run.run_id}: warmup is {manifest.get('warmup')}, expected {required_warmup}")
    if manifest.get("reps") != str(required_reps):
        errors.append(f"{run.run_id}: reps is {manifest.get('reps')}, expected {required_reps}")
    if manifest.get("datomic_storage") != required_storage:
        errors.append(f"{run.run_id}: datomic_storage is {manifest.get('datomic_storage')}, expected {required_storage}")
    if manifest.get("total_errors") != "0":
        errors.append(f"{run.run_id}: total_errors is {manifest.get('total_errors')}, expected 0")
    if require_clean and manifest.get("git_dirty") != "false":
        errors.append(f"{run.run_id}: git_dirty is {manifest.get('git_dirty')}, expected false")
    if manifest.get("source_endpoint") != "https://api.fda.gov/food/enforcement.json":
        errors.append(f"{run.run_id}: unexpected source endpoint {manifest.get('source_endpoint')}")
    for query in QUERY_ORDER:
        row = run.rows.get(query)
        if row is None:
            errors.append(f"{run.run_id}: missing query {query}")
            continue
        if row.ok_count != required_reps:
            errors.append(f"{run.run_id}: {query} ok_count is {row.ok_count}, expected {required_reps}")
        if row.error_count != 0:
            errors.append(f"{run.run_id}: {query} error_count is {row.error_count}, expected 0")
    return errors


def fmt(value: float) -> str:
    return f"{value:.3f}"


def int_manifest(manifest: dict[str, str], key: str) -> int:
    return int(manifest.get(key, "0"))


def float_manifest(manifest: dict[str, str], key: str) -> float:
    return float(manifest.get(key, "0"))


def markdown_report(runs: list[ScaleRun], required_warmup: int, required_reps: int, required_storage: str) -> str:
    commit_set = sorted({run.manifest.get("git_commit", "unknown") for run in runs})
    source_updates = sorted({run.manifest.get("source_last_updated", "unknown") for run in runs})
    source_totals = sorted({run.manifest.get("source_total_available", "unknown") for run in runs})

    lines = [
        "# openFDA Disk-Backed Scale Benchmark Summary",
        "",
        f"- Included runs: {', '.join(run.run_id for run in runs)}",
        f"- Required Datomic storage: {required_storage}",
        f"- Warmup/reps: {required_warmup}/{required_reps}",
        "- Gate: total_errors=0, query error_count=0, clean git state",
        "- Source endpoint: `https://api.fda.gov/food/enforcement.json`",
        f"- Source last_updated value(s): {', '.join(source_updates)}",
        f"- Source total_available value(s): {', '.join(source_totals)}",
        f"- Source commit(s): {', '.join(f'`{commit}`' for commit in commit_set)}",
        "",
        "## Boundary",
        "",
        "This is an external-validity scale check for real public FDA recall/event",
        "lookup data mapped into Datomic-backed PROV/traceability entities. It is",
        "not a product-comparison benchmark and not a complete supply-chain traversal",
        "claim.",
        "",
        "## Scale Summary",
        "",
        "| Requested limit | Usable records | Ingest ms | Q1 p95 | Q2 p95 | Q3 p95 | Q4 p95 | Max observed |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for run in sorted(runs, key=lambda item: item.requested_limit):
        max_observed = max(row.max_ms for row in run.rows.values())
        lines.append(
            "| {limit} | {usable} | {ingest} | {q1} | {q2} | {q3} | {q4} | {max_observed} |".format(
                limit=run.requested_limit,
                usable=int_manifest(run.manifest, "usable_records"),
                ingest=fmt(float_manifest(run.manifest, "ingest_ms")),
                q1=fmt(run.rows["q1_recall_number_lookup"].p95_ms),
                q2=fmt(run.rows["q2_entity_uuid_lookup"].p95_ms),
                q3=fmt(run.rows["q3_activity_uuid_lookup"].p95_ms),
                q4=fmt(run.rows["q4_recalling_firm_lookup"].p95_ms),
                max_observed=fmt(max_observed),
            )
        )

    lines.extend(
        [
            "",
            "## Per-Query Mean Latency",
            "",
            "| Requested limit | Q1 mean | Q2 mean | Q3 mean | Q4 mean |",
            "|---:|---:|---:|---:|---:|",
        ]
    )
    for run in sorted(runs, key=lambda item: item.requested_limit):
        lines.append(
            "| {limit} | {q1} | {q2} | {q3} | {q4} |".format(
                limit=run.requested_limit,
                q1=fmt(run.rows["q1_recall_number_lookup"].mean_ms),
                q2=fmt(run.rows["q2_entity_uuid_lookup"].mean_ms),
                q3=fmt(run.rows["q3_activity_uuid_lookup"].mean_ms),
                q4=fmt(run.rows["q4_recalling_firm_lookup"].mean_ms),
            )
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Use this report to decide whether larger real-world data volumes remain",
            "stable enough for an external-validity paper note. Archive raw run",
            "directories before promoting values into the paper.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=Path("benchmarks/real-world/results"))
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--limits", nargs="+", type=int, required=True)
    parser.add_argument("--required-warmup", type=int, default=30)
    parser.add_argument("--required-reps", type=int, default=100)
    parser.add_argument("--required-storage", default="dev-transactor")
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if len(args.runs) != len(args.limits):
        print("error: --runs and --limits must have the same length")
        return 2

    runs = [load_run(args.results_dir, run_id, limit) for run_id, limit in zip(args.runs, args.limits)]
    errors = [
        error
        for run in runs
        for error in validate_run(run, args.required_warmup, args.required_reps, args.required_storage, not args.allow_dirty)
    ]
    if errors:
        for error in errors:
            print(f"error: {error}")
        return 1

    report = markdown_report(runs, args.required_warmup, args.required_reps, args.required_storage)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
