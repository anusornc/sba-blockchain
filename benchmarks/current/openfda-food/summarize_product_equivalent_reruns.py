#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import statistics as stats
from collections import defaultdict
from pathlib import Path


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def read_manifest(path: Path) -> dict[str, str]:
    values: dict[str, str] = {"manifest_path": str(path)}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def fmt(value: float) -> str:
    return f"{value:.3f}"


def aggregate_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["system"], row["query"])].append(row)

    summary: list[dict[str, str]] = []
    for (system, query), group in sorted(grouped.items()):
        means = [float(row["mean_ms"]) for row in group]
        p50s = [float(row["p50_ms"]) for row in group]
        p95s = [float(row["p95_ms"]) for row in group]
        p99s = [float(row["p99_ms"]) for row in group]
        mins = [float(row["min_ms"]) for row in group]
        maxs = [float(row["max_ms"]) for row in group]
        summary.append(
            {
                "system": system,
                "query": query,
                "runs": str(len(group)),
                "total_ok_count": str(sum(int(row["ok_count"]) for row in group)),
                "total_error_count": str(sum(int(row["error_count"]) for row in group)),
                "mean_of_mean_ms": fmt(stats.mean(means)),
                "median_p50_ms": fmt(stats.median(p50s)),
                "mean_p95_ms": fmt(stats.mean(p95s)),
                "min_p95_ms": fmt(min(p95s)),
                "max_p95_ms": fmt(max(p95s)),
                "median_p95_ms": fmt(stats.median(p95s)),
                "mean_p99_ms": fmt(stats.mean(p99s)),
                "min_observed_ms": fmt(min(mins)),
                "max_observed_ms": fmt(max(maxs)),
            }
        )
    return summary


def aggregate_ingest(manifests: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for manifest in manifests:
        grouped[manifest["system"]].append(float(manifest["ingest_ms"]))

    rows: list[dict[str, str]] = []
    for system, values in sorted(grouped.items()):
        rows.append(
            {
                "system": system,
                "runs": str(len(values)),
                "mean_ingest_ms": fmt(stats.mean(values)),
                "min_ingest_ms": fmt(min(values)),
                "max_ingest_ms": fmt(max(values)),
                "median_ingest_ms": fmt(stats.median(values)),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise SystemExit("no rows to write")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    path: Path,
    run_names: list[str],
    summary_rows: list[dict[str, str]],
    ingest_rows: list[dict[str, str]],
    manifests: list[dict[str, str]],
) -> None:
    validation_keys = ["source_raw_json_sha256", "limit", "usable_records", "warmup", "reps", "git_commit"]
    validation = {key: sorted({manifest.get(key, "") for manifest in manifests}) for key in validation_keys}
    dirty_values = sorted({manifest.get("git_dirty", "") for manifest in manifests})
    nonzero_errors = [manifest for manifest in manifests if manifest.get("total_errors", "") != "0"]

    lines = [
        "# Product-Equivalent openFDA Rerun Aggregate",
        "",
        f"- runs: `{', '.join(run_names)}`",
    ]
    for key in validation_keys:
        lines.append(f"- {key}: `{', '.join(validation[key])}`")
    lines.extend(
        [
            f"- git_dirty_values: `{', '.join(dirty_values)}`",
            f"- manifest_total_error_nonzero: `{len(nonzero_errors)}`",
            "",
            "## Query Latency Aggregate",
            "",
            "| system | query | runs | ok | errors | mean(ms) | median p50(ms) | mean p95(ms) | p95 min-max(ms) | max observed(ms) |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary_rows:
        lines.append(
            "| {system} | {query} | {runs} | {total_ok_count} | {total_error_count} | "
            "{mean_of_mean_ms} | {median_p50_ms} | {mean_p95_ms} | "
            "{min_p95_ms} - {max_p95_ms} | {max_observed_ms} |".format(**row)
        )

    lines.extend(
        [
            "",
            "## Ingest Aggregate",
            "",
            "| system | runs | mean ingest(ms) | median ingest(ms) | ingest min-max(ms) |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in ingest_rows:
        lines.append(
            "| {system} | {runs} | {mean_ingest_ms} | {median_ingest_ms} | "
            "{min_ingest_ms} - {max_ingest_ms} |".format(**row)
        )

    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- SBA uses the Datomic-backed PROV-O schema through the Clojure openFDA harness.",
            "- Neo4j uses indexed exact lookups through HTTP transactional Cypher.",
            "- Fabric uses peer CLI against openFDA chaincode with composite-key exact lookups.",
            "- Ethereum stores records as transaction input and answers semantic queries by scanning block transactions; this is real execution but not an indexed smart-contract design.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=Path("benchmarks/real-world/results"))
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    args = parser.parse_args()

    all_rows: list[dict[str, str]] = []
    manifests: list[dict[str, str]] = []
    for run in args.runs:
        run_dir = args.results_dir / run
        combined = run_dir / "product_equivalent_combined_summary.csv"
        if not combined.exists():
            raise SystemExit(f"missing combined summary: {combined}")
        for row in read_csv(combined):
            all_rows.append({"run": run, **row})
        for manifest_path in sorted(run_dir.glob("*/manifest.txt")):
            manifests.append(read_manifest(manifest_path))

    if not all_rows:
        raise SystemExit("no summary rows found")
    if not manifests:
        raise SystemExit("no manifests found")

    summary_rows = aggregate_rows(all_rows)
    ingest_rows = aggregate_ingest(manifests)
    write_csv(args.output_csv, summary_rows)
    write_markdown(args.output_md, args.runs, summary_rows, ingest_rows, manifests)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
