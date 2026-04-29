# openFDA Disk-Backed Scale Benchmark Summary

- Included runs: openfda_food_disk_scale_20260429_065523_5000_001, openfda_food_disk_scale_20260429_065523_10000_001, openfda_food_disk_scale_20260429_065523_26000_001
- Required Datomic storage: dev-transactor
- Warmup/reps: 30/100
- Gate: total_errors=0, query error_count=0, clean git state
- Source endpoint: `https://api.fda.gov/food/enforcement.json`
- Source last_updated value(s): 2026-04-22
- Source total_available value(s): 28774
- Source commit(s): `3b556992fcef08a7eacd64c7b72cc652f685e3af`

## Boundary

This is an external-validity scale check for real public FDA recall/event
lookup data mapped into Datomic-backed PROV/traceability entities. It is
not a product-comparison benchmark and not a complete supply-chain traversal
claim.

## Scale Summary

| Requested limit | Usable records | Ingest ms | Q1 p95 | Q2 p95 | Q3 p95 | Q4 p95 | Max observed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 5000 | 5000 | 7868.294 | 1.273 | 1.042 | 1.017 | 1.636 | 21.662 |
| 10000 | 9999 | 14680.833 | 0.760 | 0.710 | 0.661 | 1.374 | 2.533 |
| 26000 | 25999 | 32867.687 | 0.983 | 0.653 | 0.821 | 1.770 | 2.536 |

## Per-Query Mean Latency

| Requested limit | Q1 mean | Q2 mean | Q3 mean | Q4 mean |
|---:|---:|---:|---:|---:|
| 5000 | 0.968 | 1.069 | 0.774 | 1.313 |
| 10000 | 0.613 | 0.519 | 0.490 | 1.040 |
| 26000 | 0.756 | 0.562 | 0.556 | 1.415 |

## Interpretation

Use this report to decide whether larger real-world data volumes remain
stable enough for an external-validity paper note. Archive raw run
directories before promoting values into the paper.
