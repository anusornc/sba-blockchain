#!/usr/bin/env python3
"""Archive public-safe product-equivalent openFDA benchmark artifacts.

The archive keeps benchmark measurements, manifests, aggregate reports, and
paper-ready assets. It does not copy raw openFDA JSON captures or Datomic/Fabric
runtime state; source JSON paths are represented by hashes in run manifests.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path


RUN_FILES = [
    ("combined-summary", "product_equivalent_combined_summary.csv"),
]
SYSTEM_FILES = [
    ("system-manifest", "manifest.txt"),
    ("latency-raw", "product_equivalent_latency_raw.csv"),
    ("latency-summary", "product_equivalent_summary.csv"),
]
AGGREGATE_FILES = [
    ("aggregate-summary-md", "{artifact_id}_aggregate_summary.md"),
    ("aggregate-summary-csv", "{artifact_id}_aggregate_summary.csv"),
]


@dataclass(frozen=True)
class ArtifactEntry:
    run_id: str
    system: str
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
    if src.suffix == ".csv":
        dst.write_bytes(src.read_bytes().replace(b"\r\n", b"\n"))
        shutil.copystat(src, dst)
    else:
        shutil.copy2(src, dst)


def entry_for(
    run_id: str,
    system: str,
    role: str,
    src: Path,
    artifact_path: str,
    archived: bool,
    measured_path: Path | None = None,
) -> ArtifactEntry:
    hash_path = measured_path or src
    return ArtifactEntry(
        run_id=run_id,
        system=system,
        role=role,
        source_path=src,
        artifact_path=artifact_path,
        byte_count=hash_path.stat().st_size,
        sha256=sha256_file(hash_path),
        archived=archived,
    )


def archive_run(results_dir: Path, archive_dir: Path, run_id: str) -> list[ArtifactEntry]:
    run_dir = results_dir / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"missing run directory: {run_dir}")

    entries: list[ArtifactEntry] = []
    for role, filename in RUN_FILES:
        src = run_dir / filename
        if not src.exists():
            raise FileNotFoundError(f"missing required artifact: {src}")
        relative = Path("runs") / run_id / filename
        dst = archive_dir / relative
        copy_artifact(src, dst)
        entries.append(entry_for(run_id, "all", role, src, str(relative), archived=True, measured_path=dst))

    system_dirs = sorted(path for path in run_dir.iterdir() if path.is_dir())
    if not system_dirs:
        raise FileNotFoundError(f"no system directories found under: {run_dir}")
    for system_dir in system_dirs:
        system = system_dir.name
        for role, filename in SYSTEM_FILES:
            src = system_dir / filename
            if not src.exists():
                raise FileNotFoundError(f"missing required artifact: {src}")
            relative = Path("runs") / run_id / system / filename
            dst = archive_dir / relative
            copy_artifact(src, dst)
            entries.append(entry_for(run_id, system, role, src, str(relative), archived=True, measured_path=dst))

        raw_json = system_dir / "openfda_food_raw.json"
        if raw_json.exists():
            entries.append(entry_for(run_id, system, "source-json", raw_json, "not-archived", archived=False))

    return entries


def archive_aggregate(results_dir: Path, archive_dir: Path, artifact_id: str) -> list[ArtifactEntry]:
    entries: list[ArtifactEntry] = []
    for role, pattern in AGGREGATE_FILES:
        src = results_dir / pattern.format(artifact_id=artifact_id)
        if not src.exists():
            raise FileNotFoundError(f"missing aggregate artifact: {src}")
        relative = Path("aggregate") / src.name
        dst = archive_dir / relative
        copy_artifact(src, dst)
        entries.append(entry_for("aggregate", "all", role, src, str(relative), archived=True, measured_path=dst))
    return entries


def archive_asset_files(archive_dir: Path, paths: list[Path]) -> list[ArtifactEntry]:
    entries: list[ArtifactEntry] = []
    for src in paths:
        if not src.exists():
            raise FileNotFoundError(f"missing asset: {src}")
        relative = Path("paper-assets") / src.name
        dst = archive_dir / relative
        copy_artifact(src, dst)
        entries.append(entry_for("paper-assets", "all", "paper-asset", src, str(relative), archived=True, measured_path=dst))
    return entries


def write_manifest(path: Path, entries: list[ArtifactEntry]) -> None:
    lines = [
        "run_id\tsystem\trole\tarchived\tbytes\tsha256\tsource_path\tartifact_path",
    ]
    for entry in entries:
        lines.append(
            "\t".join(
                [
                    entry.run_id,
                    entry.system,
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


def write_readme(path: Path, artifact_id: str, run_ids: list[str], asset_paths: list[Path]) -> None:
    assets = "\n".join(f"- `{Path('paper-assets') / asset.name}`" for asset in asset_paths)
    text = f"""# Product-Equivalent openFDA Benchmark Artifact Package

Artifact ID: `{artifact_id}`

This package preserves public-safe evidence for the openFDA product-equivalent
benchmark comparing SBA/Datomic, Neo4j, Hyperledger Fabric, and Ethereum.

## Included

- Run manifests for: {", ".join(f"`{run_id}`" for run_id in run_ids)}
- Per-system raw latency CSV files
- Per-system summary CSV files
- Per-run combined summary CSV files
- Aggregate Markdown and CSV summaries
- `ARTIFACT_MANIFEST.tsv` with byte counts and SHA-256 hashes
{assets}

## Source JSON Policy

Raw openFDA JSON captures are not copied into this archive. They are public API
captures, but can be large; each run manifest records `source_raw_json_sha256`
and the source path used during execution.

## Excluded

- Raw openFDA JSON captures
- Datomic data directories
- Datomic logs, pid files, and transactor config
- Fabric runtime crypto/channel artifacts and Docker state

These excluded files are runtime state or source captures, not benchmark
measurements.
"""
    path.write_text(text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=Path("benchmarks/real-world/results"))
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--artifact-id", required=True)
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--assets", nargs="*", type=Path, default=[])
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
    entries.extend(archive_aggregate(args.results_dir, args.archive_dir, args.artifact_id))
    for run_id in args.runs:
        entries.extend(archive_run(args.results_dir, args.archive_dir, run_id))
    entries.extend(archive_asset_files(args.archive_dir, args.assets))

    write_manifest(args.archive_dir / "ARTIFACT_MANIFEST.tsv", entries)
    write_readme(args.archive_dir / "README.md", args.artifact_id, args.runs, args.assets)
    print(f"archived product-equivalent openFDA evidence to {args.archive_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
