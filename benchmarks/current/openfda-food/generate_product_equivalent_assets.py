#!/usr/bin/env python3
"""Generate paper-ready assets for product-equivalent openFDA results."""

from __future__ import annotations

import argparse
import csv
import html
import statistics as stats
from pathlib import Path


SYSTEM_LABELS = {
    "sba": "SBA/Datomic",
    "neo4j": "Neo4j",
    "fabric": "Fabric",
    "ethereum": "Ethereum",
}
SYSTEM_ORDER = ["sba", "neo4j", "fabric", "ethereum"]


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def system_summary(rows: list[dict[str, str]]) -> list[dict[str, float | str]]:
    result = []
    for system in SYSTEM_ORDER:
        system_rows = [row for row in rows if row["system"] == system]
        if not system_rows:
            continue
        result.append(
            {
                "system": system,
                "label": SYSTEM_LABELS.get(system, system),
                "avg_mean_ms": stats.mean(float(row["mean_of_mean_ms"]) for row in system_rows),
                "avg_p95_ms": stats.mean(float(row["mean_p95_ms"]) for row in system_rows),
                "worst_p95_ms": max(float(row["max_p95_ms"]) for row in system_rows),
            }
        )
    return result


def fmt(value: float) -> str:
    return f"{value:.3f}"


def write_markdown(path: Path, rows: list[dict[str, float | str]]) -> None:
    lines = [
        "| system | avg mean latency (ms) | avg p95 (ms) | worst p95 (ms) | p95 vs SBA |",
        "|---|---:|---:|---:|---:|",
    ]
    sba_p95 = next(float(row["avg_p95_ms"]) for row in rows if row["system"] == "sba")
    for row in rows:
        ratio = float(row["avg_p95_ms"]) / sba_p95
        ratio_text = "1.0x" if row["system"] == "sba" else f"{ratio:.1f}x"
        lines.append(
            f"| {row['label']} | {fmt(float(row['avg_mean_ms']))} | "
            f"{fmt(float(row['avg_p95_ms']))} | {fmt(float(row['worst_p95_ms']))} | {ratio_text} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def latex_escape(value: str) -> str:
    return value.replace("&", "\\&")


def write_latex(path: Path, rows: list[dict[str, float | str]]) -> None:
    sba_p95 = next(float(row["avg_p95_ms"]) for row in rows if row["system"] == "sba")
    lines = [
        "\\begin{table}[t]",
        "\\centering",
        "\\caption{Product-equivalent openFDA benchmark summary (5 reruns, 100 records, 10 measured repetitions per query).}",
        "\\label{tab:openfda-product-equivalent}",
        "\\begin{tabular}{lrrrr}",
        "\\toprule",
        "System & Mean (ms) & p95 (ms) & Worst p95 (ms) & p95 vs. SBA \\\\",
        "\\midrule",
    ]
    for row in rows:
        ratio = float(row["avg_p95_ms"]) / sba_p95
        ratio_text = "1.0$\\times$" if row["system"] == "sba" else f"{ratio:.1f}$\\times$"
        lines.append(
            f"{latex_escape(str(row['label']))} & {fmt(float(row['avg_mean_ms']))} & "
            f"{fmt(float(row['avg_p95_ms']))} & {fmt(float(row['worst_p95_ms']))} & {ratio_text} \\\\"
        )
    lines.extend(
        [
            "\\bottomrule",
            "\\end{tabular}",
            "\\end{table}",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def write_svg(path: Path, rows: list[dict[str, float | str]]) -> None:
    width = 860
    height = 420
    margin_left = 145
    margin_right = 55
    margin_top = 42
    bar_height = 34
    row_gap = 54
    plot_width = width - margin_left - margin_right
    max_value = max(float(row["avg_p95_ms"]) for row in rows) * 1.10
    colors = {
        "sba": "#0f766e",
        "neo4j": "#2563eb",
        "fabric": "#b45309",
        "ethereum": "#6b7280",
    }

    def x(value: float) -> float:
        return margin_left + (value / max_value) * plot_width

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        '<title id="title">openFDA product-equivalent p95 latency</title>',
        '<desc id="desc">Average p95 latency across five reruns for SBA/Datomic, Neo4j, Fabric, and Ethereum.</desc>',
        '<rect width="100%" height="100%" fill="#fbfaf7"/>',
        '<text x="36" y="30" font-family="Georgia, serif" font-size="22" fill="#172018">openFDA product-equivalent benchmark: average p95 latency</text>',
        '<text x="36" y="54" font-family="Georgia, serif" font-size="13" fill="#5b625c">5 reruns, 100 records, 3 warmup, 10 measured repetitions per query; lower is better</text>',
    ]

    ticks = [0, 50, 100, 150]
    for tick in ticks:
        tx = x(tick)
        elements.append(f'<line x1="{tx:.1f}" y1="78" x2="{tx:.1f}" y2="{height - 58}" stroke="#ddd8cc" stroke-width="1"/>')
        elements.append(f'<text x="{tx:.1f}" y="{height - 32}" text-anchor="middle" font-family="Arial, sans-serif" font-size="12" fill="#697067">{tick}</text>')
    elements.append(f'<text x="{x(150):.1f}" y="{height - 12}" text-anchor="middle" font-family="Arial, sans-serif" font-size="12" fill="#697067">milliseconds</text>')

    sba_p95 = next(float(row["avg_p95_ms"]) for row in rows if row["system"] == "sba")
    for idx, row in enumerate(rows):
        y = margin_top + 55 + idx * row_gap
        value = float(row["avg_p95_ms"])
        bar_width = max(2, x(value) - margin_left)
        ratio = value / sba_p95
        ratio_text = "baseline" if row["system"] == "sba" else f"{ratio:.1f}x SBA"
        label = html.escape(str(row["label"]))
        color = colors.get(str(row["system"]), "#475569")
        label_x = min(margin_left + bar_width + 8, width - 230)
        elements.extend(
            [
                f'<text x="{margin_left - 18}" y="{y + 22}" text-anchor="end" font-family="Arial, sans-serif" font-size="15" fill="#1f2933">{label}</text>',
                f'<rect x="{margin_left}" y="{y}" width="{bar_width:.1f}" height="{bar_height}" rx="5" fill="{color}"/>',
                f'<text x="{label_x:.1f}" y="{y + 22}" font-family="Arial, sans-serif" font-size="14" fill="#1f2933">{value:.3f} ms, {ratio_text}</text>',
            ]
        )
    elements.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(elements) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregate-csv", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--latex", type=Path, required=True)
    parser.add_argument("--svg", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = system_summary(read_rows(args.aggregate_csv))
    write_markdown(args.markdown, rows)
    write_latex(args.latex, rows)
    write_svg(args.svg, rows)
    print(f"wrote {args.markdown}")
    print(f"wrote {args.latex}")
    print(f"wrote {args.svg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
