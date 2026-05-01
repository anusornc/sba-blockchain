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
RUN_ID="${RUN_ID:-${RUN_GROUP}_fabric}"
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results/${RUN_GROUP}}"
OUT_DIR="${OUT_DIR:-${RESULTS_DIR}/fabric}"
INPUT="${INPUT:-$(latest_openfda_input)}"
LIMIT="${LIMIT:-25}"
WARMUP="${WARMUP:-1}"
REPS="${REPS:-5}"
TESTNET_DIR="${TESTNET_DIR:-${REPO_ROOT}/benchmarks/practical/fabric/fabric-samples/test-network}"
FABRIC_CHANNEL="${FABRIC_CHANNEL:-openfda-channel}"
FABRIC_CHAINCODE="${FABRIC_CHAINCODE:-openfda}"
START_FABRIC="${START_FABRIC:-0}"

if [[ -z "${INPUT}" || ! -f "${INPUT}" ]]; then
  echo "error: openFDA raw JSON input not found. Set INPUT=/path/to/openfda_food_raw.json" >&2
  exit 2
fi

if [[ "${START_FABRIC}" == "1" ]]; then
  TESTNET_DIR="${TESTNET_DIR}" \
  CHANNEL_NAME="${FABRIC_CHANNEL}" \
  CHAINCODE_NAME="${FABRIC_CHAINCODE}" \
  bash "${SCRIPT_DIR}/setup_product_equivalent_fabric.bash"
fi

if ! docker ps --format '{{.Names}}' | grep -q "peer0.org1.example.com"; then
  echo "error: Fabric network is not running. Use START_FABRIC=1 or run setup_product_equivalent_fabric.bash." >&2
  exit 2
fi

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null; then
  GIT_DIRTY=false
else
  GIT_DIRTY=true
fi

python3 "${SCRIPT_DIR}/product_equivalent_benchmark.py" \
  --system fabric \
  --input "${INPUT}" \
  --out-dir "${OUT_DIR}" \
  --run-id "${RUN_ID}" \
  --limit "${LIMIT}" \
  --warmup "${WARMUP}" \
  --reps "${REPS}" \
  --git-commit "${GIT_COMMIT}" \
  --git-dirty "${GIT_DIRTY}" \
  --fabric-testnet-dir "${TESTNET_DIR}" \
  --fabric-channel "${FABRIC_CHANNEL}" \
  --fabric-chaincode "${FABRIC_CHAINCODE}"

echo "Fabric product-equivalent result:"
echo "  ${OUT_DIR}/product_equivalent_summary.csv"
