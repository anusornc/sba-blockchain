# Reproducibility

Benchmark harnesses and selected public-safe artifact snapshots are provided
under `benchmarks/reproducibility/`.

The public repository excludes private paper build artifacts and private run
notes. If you rerun benchmarks, keep raw CSV files, summary CSV files, and a
manifest recording command, environment, timestamp, and commit hash. The bundled
artifact snapshots correspond to the paper revision and are safe for public
distribution.

Bundled selected artifacts:

```text
benchmarks/reproducibility/results/query/main_revised_query_20260428_009/
benchmarks/reproducibility/results/query/main_revised_query_20260428_010/
benchmarks/reproducibility/results/query/main_revised_query_20260428_011/
benchmarks/reproducibility/results/query/query_rerun_summary_20260428.md
benchmarks/reproducibility/results/fabric/main_revised_fabric_20260428_002/
benchmarks/real-world/artifacts/openfda_food_disk_scale_20260429_065523/
```

For repo-local prerequisites, run `source scripts/dev-env.bash` before starting
services or Clojure commands. Start the SBA API and load the public sample data
before running the query harness. The `/api/query` calls require a valid JWT in
`API_TOKEN`; the public QR trace route does not require authentication.

Query harness:

```bash
API_TOKEN="<valid-jwt>" REPS=100 WARMUP=30 \
  bash benchmarks/reproducibility/query/run_query_harness.bash
```

Summarize the bundled repeated query runs:

```bash
python3 benchmarks/reproducibility/query/summarize_query_reruns.py \
  --results-dir benchmarks/reproducibility/results/query
```

Default output path:

```text
benchmarks/reproducibility/results/query/<RUN_ID>/
```

Fabric harness:

```bash
bash benchmarks/reproducibility/fabric/setup_fabric_ccaas.bash
CHAINCODE_MODE=ccaas REPS=50 WARMUP=5 \
  bash benchmarks/reproducibility/fabric/run_fabric_invoke_benchmark.bash
```

Default output path:

```text
benchmarks/reproducibility/results/fabric/<RUN_ID>/
```

The setup script downloads Fabric samples if missing, starts `benchmark-channel`,
and deploys the `benchmark` asset-transfer chaincode as chaincode-as-a-service
(CCAAS). Generated Fabric crypto material, channel artifacts, wallets,
peer/orderer state, and Docker volumes are intentionally excluded from this
public release.

openFDA real-world benchmark:

```bash
LIMIT=1000 WARMUP=30 REPS=100 \
  bash benchmarks/real-world/openfda-food/run_openfda_food_benchmark.bash
```

Disk-backed openFDA scale reruns require a Datomic Pro transactor executable
outside git. Set `DATOMIC_TRANSACTOR_BIN` if it is not installed at
`external/datomic-pro/bin/transactor`.

```bash
DATOMIC_TRANSACTOR_BIN=/path/to/datomic-pro/bin/transactor \
SCALE_LIMITS="5000 10000 26000" RUN_COUNT_PER_SCALE=1 \
  bash benchmarks/real-world/openfda-food/run_openfda_food_disk_scale.bash
```

Bundled openFDA artifact package:

```text
benchmarks/real-world/artifacts/openfda_food_disk_scale_20260429_065523/
```

The artifact package includes run manifests, raw latency CSVs, summary CSVs,
and SHA-256 hashes for raw source JSON captures. It intentionally excludes
large raw JSON captures and Datomic runtime storage/log files.
