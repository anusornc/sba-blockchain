# openFDA Benchmark Artifact Package

Artifact ID: `openfda_food_disk_scale_20260429_065523`

This package preserves the public-safe benchmark evidence needed to trace the
openFDA disk-backed scale results without committing Datomic runtime storage.

## Included

- Run manifests for: `openfda_food_disk_scale_20260429_065523_5000_001`, `openfda_food_disk_scale_20260429_065523_10000_001`, `openfda_food_disk_scale_20260429_065523_26000_001`
- Raw latency CSV files
- Latency summary CSV files
- Summary copied from `benchmarks/real-world/results/openfda_food_disk_scale_20260429_065523_summary.md`
- `ARTIFACT_MANIFEST.tsv` with byte counts and SHA-256 hashes

## Source JSON Policy

Raw source JSON files are not copied because they are large public API captures; their byte counts and SHA-256 hashes are recorded in `ARTIFACT_MANIFEST.tsv`.

## Excluded

- Datomic data directories
- Datomic logs and pid files
- Generated transactor config files

These excluded files are runtime state, not benchmark measurements.
