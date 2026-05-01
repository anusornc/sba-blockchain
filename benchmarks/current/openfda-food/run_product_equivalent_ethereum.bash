#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

latest_openfda_input() {
  find benchmarks/real-world/results -name openfda_food_raw.json -type f -printf '%T@ %p\n' 2>/dev/null |
    sort -n |
    tail -n 1 |
    cut -d' ' -f2-
}

RUN_GROUP="${RUN_GROUP:-product_equivalent_openfda_$(date -u +%Y%m%d_%H%M%S)}"
RUN_ID="${RUN_ID:-${RUN_GROUP}_ethereum}"
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results/${RUN_GROUP}}"
OUT_DIR="${OUT_DIR:-${RESULTS_DIR}/ethereum}"
INPUT="${INPUT:-$(latest_openfda_input)}"
LIMIT="${LIMIT:-50}"
WARMUP="${WARMUP:-1}"
REPS="${REPS:-5}"
GETH_CONTAINER="${GETH_CONTAINER:-openfda-geth-bench}"
GETH_IMAGE="${GETH_IMAGE:-ethereum/client-go:latest}"
ETHEREUM_RPC_URL="${ETHEREUM_RPC_URL:-http://127.0.0.1:8545}"
START_GETH="${START_GETH:-1}"

if [[ -z "${INPUT}" || ! -f "${INPUT}" ]]; then
  echo "error: openFDA raw JSON input not found. Set INPUT=/path/to/openfda_food_raw.json" >&2
  exit 2
fi

if [[ "${START_GETH}" == "1" ]]; then
  docker rm -f "${GETH_CONTAINER}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${GETH_CONTAINER}" \
    -p 8545:8545 \
    "${GETH_IMAGE}" \
    --dev \
    --http \
    --http.addr=0.0.0.0 \
    --http.port=8545 \
    --http.api=eth,net,web3 \
    --http.vhosts="*" \
    --http.corsdomain="*" \
    --dev.period=0 >/dev/null
fi

echo "Waiting for Ethereum JSON-RPC endpoint..."
python3 - <<'PY'
import json
import os
import sys
import time
from urllib import request

url = os.environ.get("ETHEREUM_RPC_URL", "http://127.0.0.1:8545")
for _ in range(90):
    try:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_accounts", "params": []}).encode()
        req = request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with request.urlopen(req, timeout=3) as response:
            accounts = json.loads(response.read().decode()).get("result", [])
        if accounts:
            sys.exit(0)
    except Exception:
        pass
    time.sleep(2)
sys.exit("Ethereum JSON-RPC endpoint did not become ready")
PY

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null; then
  GIT_DIRTY=false
else
  GIT_DIRTY=true
fi

python3 "${SCRIPT_DIR}/product_equivalent_benchmark.py" \
  --system ethereum \
  --input "${INPUT}" \
  --out-dir "${OUT_DIR}" \
  --run-id "${RUN_ID}" \
  --limit "${LIMIT}" \
  --warmup "${WARMUP}" \
  --reps "${REPS}" \
  --git-commit "${GIT_COMMIT}" \
  --git-dirty "${GIT_DIRTY}" \
  --ethereum-rpc-url "${ETHEREUM_RPC_URL}"

echo "Ethereum product-equivalent result:"
echo "  ${OUT_DIR}/product_equivalent_summary.csv"
