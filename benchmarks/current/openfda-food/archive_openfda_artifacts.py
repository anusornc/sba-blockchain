#!/usr/bin/env python3
"""Archive public-safe openFDA benchmark artifacts.

The archive intentionally keeps latency CSVs, summaries, and run manifests in
git-friendly form while hashing large source JSON captures instead of copying
them by default.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path


SMALL_FILES = [
    ("run-manifest", "manifest.txt"),
    ("latency-raw", "openfda_food_latency_raw.csv"),
    ("latency-summary", "openfda_food_latency_summary.csv"),
]
SOURCE_JSON = "openfda_food_raw.json"


@dataclass(frozen=True)
class ArtifactEntry:
    run_id: str
    role: str
    source_path: Path
    artifact_path: str
    byte_count: int
    sha256: str
    archived: bool


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_artifact(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def entry_for(run_id: str, role: str, src: Path, artifact_path: str, archived: bool) -> ArtifactEntry:
    return ArtifactEntry(
        run_id=run_id,
        role=role,
        source_path=src,
        artifact_path=artifact_path,
        byte_count=src.stat().st_size,
        sha256=sha256_file(src),
        archived=archived,
    )


def archive_run(results_dir: Path, archive_dir: Path, run_id: str, include_source_json: bool) -> list[ArtifactEntry]:
    run_dir = results_dir / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"missing run directory: {run_dir}")

    entries: list[ArtifactEntry] = []
    for role, filename in SMALL_FILES:
        src = run_dir / filename
        if not src.exists():
            raise FileNotFoundError(f"missing required artifact: {src}")
        relative = Path("runs") / run_id / filename
        copy_artifact(src, archive_dir / relative)
        entries.append(entry_for(run_id, role, src, str(relative), archived=True))

    source_json = run_dir / SOURCE_JSON
    if source_json.exists():
        relative = Path("runs") / run_id / SOURCE_JSON
        if include_source_json:
            copy_artifact(source_json, archive_dir / relative)
            entries.append(entry_for(run_id, "source-json", source_json, str(relative), archived=True))
        else:
            entries.append(entry_for(run_id, "source-json", source_json, "not-archived", archived=False))
    return entries


def write_manifest(path: Path, entries: list[ArtifactEntry]) -> None:
    lines = [
        "run_id\trole\tarchived\tbytes\tsha256\tsource_path\tartifact_path",
    ]
    for entry in entries:
        lines.append(
            "\t".join(
                [
                    entry.run_id,
                    entry.role,
                    "yes" if entry.archived else "no",
                    str(entry.byte_count),
                    entry.sha256,
                    str(entry.source_path),
                    entry.artifact_path,
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_readme(
    path: Path,
    artifact_id: str,
    run_ids: list[str],
    include_source_json: bool,
    summary_file: Path | None,
) -> None:
    source_json_policy = (
        "Raw source JSON files are copied into this artifact package."
        if include_source_json
        else "Raw source JSON files are not copied because they are large public API captures; their byte counts and SHA-256 hashes are recorded in `ARTIFACT_MANIFEST.tsv`."
    )
    summary_line = f"- Summary copied from `{summary_file}`\n" if summary_file else ""
    text = f"""# openFDA Benchmark Artifact Package

Artifact ID: `{artifact_id}`

This package preserves the public-safe benchmark evidence needed to trace the
openFDA disk-backed scale results without committing Datomic runtime storage.

## Included

- Run manifests for: {", ".join(f"`{run_id}`" for run_id in run_ids)}
- Raw latency CSV files
- Latency summary CSV files
{summary_line}- `ARTIFACT_MANIFEST.tsv` with byte counts and SHA-256 hashes

## Source JSON Policy

{source_json_policy}

## Excluded

- Datomic data directories
- Datomic logs and pid files
- Generated transactor config files

These excluded files are runtime state, not benchmark measurements.
"""
    path.write_text(text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=Path("benchmarks/real-world/results"))
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--include-source-json", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.archive_dir.exists():
        if not args.force:
            print(f"error: archive directory already exists: {args.archive_dir}")
            return 2
        shutil.rmtree(args.archive_dir)
    args.archive_dir.mkdir(parents=True)

    entries: list[ArtifactEntry] = []
    for run_id in args.runs:
        entries.extend(archive_run(args.results_dir, args.archive_dir, run_id, args.include_source_json))

    if args.summary:
        if not args.summary.exists():
            raise FileNotFoundError(f"missing summary: {args.summary}")
        copy_artifact(args.summary, args.archive_dir / "scale_summary.md")
        entries.append(entry_for("summary", "scale-summary", args.summary, "scale_summary.md", archived=True))

    write_manifest(args.archive_dir / "ARTIFACT_MANIFEST.tsv", entries)
    write_readme(
        path=args.archive_dir / "README.md",
        artifact_id=args.archive_dir.name,
        run_ids=args.runs,
        include_source_json=args.include_source_json,
        summary_file=args.summary,
    )
    print(f"archived openFDA evidence to {args.archive_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
