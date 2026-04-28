# Query Benchmark Rerun Stability Summary

- Included runs: main_revised_query_20260428_009, main_revised_query_20260428_010, main_revised_query_20260428_011
- Selected paper evidence run: `main_revised_query_20260428_011`
- Inclusion policy: warmup=30, total_errors=0, query error_count=0
- Clean worktree required: yes
- Source commit(s): `3207cf201662e9ef0bc0ebb04c4ef1d9251020dd`

## Per-Run Summary

| Run | Q1 mean/p95 | Q2 mean/p95 | Q3 mean/p95 | Q4 mean/p95 | Max outlier |
|---|---:|---:|---:|---:|---:|
| `main_revised_query_20260428_009` | 2.82/3.69 ms | 7.56/8.70 ms | 4.86/6.04 ms | 4.48/5.50 ms | 134.65 ms |
| `main_revised_query_20260428_010` | 2.01/2.40 ms | 4.16/5.12 ms | 3.38/3.76 ms | 3.88/4.24 ms | 13.78 ms |
| `main_revised_query_20260428_011` | 1.51/1.82 ms | 3.61/4.03 ms | 3.39/3.80 ms | 3.80/4.43 ms | 34.49 ms |

## Across-Run Ranges

| Query | Mean range (ms) | p95 range (ms) | p99 max (ms) | Max observed (ms) |
|---|---:|---:|---:|---:|
| q1_trace_qr | 1.51--2.82 | 1.82--3.69 | 4.28 | 4.32 |
| q2_batch_lookup | 3.61--7.56 | 4.03--8.70 | 28.43 | 134.65 |
| q3_prov_entities | 3.38--4.86 | 3.76--6.04 | 6.28 | 34.49 |
| q4_prov_activities | 3.80--4.48 | 4.24--5.50 | 6.01 | 13.78 |

## Interpretation

The selected paper run follows the latest-clean-run policy rather than manual
per-query cherry-picking. The rerun set shows warmup/cache sensitivity across the
sequence and occasional single-request runtime outliers. Paper claims should
therefore cite the selected run's mean/p95/p99 values, disclose the rerun ranges,
and avoid treating max latency as representative steady-state behavior.
