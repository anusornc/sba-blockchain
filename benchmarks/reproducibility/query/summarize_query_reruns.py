#!/usr/bin/env python3
"""Summarize repeated query benchmark runs for paper evidence.

The report is intentionally small and deterministic so it can be checked into
the repository and exported to the public companion artifact.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_RUN_IDS = [
    "main_revised_query_20260428_009",
    "main_revised_query_20260428_010",
    "main_revised_query_20260428_011",
]
DEFAULT_SELECTED_RUN_ID = "main_revised_query_20260428_011"
QUERY_ORDER = [
    "q1_trace_qr",
    "q2_batch_lookup",
    "q3_prov_entities",
    "q4_prov_activities",
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


def default_results_dir() -> Path:
    private_dir = Path("benchmarks/main-revised/results")
    if private_dir.exists():
        return private_dir
    return Path("benchmarks/reproducibility/results/query")


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
    summary_path = run_dir / "query_latency_summary.csv"
    if not manifest_path.exists():
        raise FileNotFoundError(f"missing manifest: {manifest_path}")
    if not summary_path.exists():
        raise FileNotFoundError(f"missing summary: {summary_path}")
    return RunEvidence(
        run_id=run_id,
        manifest=read_manifest(manifest_path),
        rows=read_summary(summary_path),
    )


def validate_run(run: RunEvidence, required_warmup: int, require_clean: bool) -> list[str]:
    errors: list[str] = []
    manifest_run_id = run.manifest.get("run_id", run.run_id)
    if manifest_run_id != run.run_id:
        errors.append(f"{run.run_id}: manifest run_id is {manifest_run_id}")
    if str(required_warmup) != run.manifest.get("warmup"):
        errors.append(f"{run.run_id}: warmup is {run.manifest.get('warmup')}, expected {required_warmup}")
    if run.manifest.get("total_errors") != "0":
        errors.append(f"{run.run_id}: total_errors is {run.manifest.get('total_errors')}, expected 0")
    if require_clean and run.manifest.get("git_dirty") != "false":
        errors.append(f"{run.run_id}: git_dirty is {run.manifest.get('git_dirty')}, expected false")
    missing = [query for query in QUERY_ORDER if query not in run.rows]
    if missing:
        errors.append(f"{run.run_id}: missing query rows: {', '.join(missing)}")
    for query, row in run.rows.items():
        if row.error_count != 0:
            errors.append(f"{run.run_id}: {query} error_count is {row.error_count}, expected 0")
    return errors


def fmt(value: float) -> str:
    return f"{value:.2f}"


def fmt_range(values: Iterable[float]) -> str:
    values = list(values)
    return f"{fmt(min(values))}--{fmt(max(values))}"


def markdown_report(
    runs: list[RunEvidence],
    selected_run_id: str,
    required_warmup: int,
    require_clean: bool,
) -> str:
    if selected_run_id not in {run.run_id for run in runs}:
        raise ValueError(f"selected run {selected_run_id} is not in summarized runs")

    commit_set = sorted({run.manifest.get("git_commit", "unknown") for run in runs})
    lines = [
        "# Query Benchmark Rerun Stability Summary",
        "",
        f"- Included runs: {', '.join(run.run_id for run in runs)}",
        f"- Selected paper evidence run: `{selected_run_id}`",
        f"- Inclusion policy: warmup={required_warmup}, total_errors=0, query error_count=0",
        f"- Clean worktree required: {'yes' if require_clean else 'no'}",
        f"- Source commit(s): {', '.join(f'`{commit}`' for commit in commit_set)}",
        "",
        "## Per-Run Summary",
        "",
        "| Run | Q1 mean/p95 | Q2 mean/p95 | Q3 mean/p95 | Q4 mean/p95 | Max outlier |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for run in runs:
        cells = []
        max_outlier = 0.0
        for query in QUERY_ORDER:
            row = run.rows[query]
            cells.append(f"{fmt(row.mean_ms)}/{fmt(row.p95_ms)} ms")
            max_outlier = max(max_outlier, row.max_ms)
        lines.append(f"| `{run.run_id}` | {' | '.join(cells)} | {fmt(max_outlier)} ms |")

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
            "The selected paper run follows the latest-clean-run policy rather than manual",
            "per-query cherry-picking. The rerun set shows warmup/cache sensitivity across the",
            "sequence and occasional single-request runtime outliers. Paper claims should",
            "therefore cite the selected run's mean/p95/p99 values, disclose the rerun ranges,",
            "and avoid treating max latency as representative steady-state behavior.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=default_results_dir())
    parser.add_argument("--runs", nargs="+", default=DEFAULT_RUN_IDS)
    parser.add_argument("--selected-run", default=DEFAULT_SELECTED_RUN_ID)
    parser.add_argument("--required-warmup", type=int, default=30)
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
        for error in validate_run(run, args.required_warmup, require_clean)
    ]
    if errors:
        for error in errors:
            print(f"error: {error}")
        return 1

    report = markdown_report(
        runs=runs,
        selected_run_id=args.selected_run,
        required_warmup=args.required_warmup,
        require_clean=require_clean,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
