# Reproducibility

Benchmark harnesses are provided under `benchmarks/reproducibility/`.

The public repository excludes private paper build artifacts and private run
notes. If you rerun benchmarks, keep raw CSV files, summary CSV files, and a
manifest recording command, environment, timestamp, and commit hash.

For repo-local prerequisites, run `source scripts/dev-env.bash` before starting
services or Clojure commands.

Query harness:

```bash
API_TOKEN="<valid-jwt>" REPS=100 WARMUP=10 \
  bash benchmarks/reproducibility/query/run_query_harness.bash
```

Fabric harness:

```bash
REPS=50 WARMUP=5 \
  bash benchmarks/reproducibility/fabric/run_fabric_invoke_benchmark.bash
```

The Fabric harness is not standalone in the public companion repository. It
expects an externally bootstrapped Hyperledger Fabric `test-network` under
`benchmarks/practical/fabric/fabric-samples/test-network`, or an equivalent
local path provided by the operator. Generated Fabric crypto material, channel
artifacts, wallets, and peer/orderer state are intentionally excluded from this
public release.
