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
RUN_ID="${RUN_ID:-${RUN_GROUP}_neo4j}"
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results/${RUN_GROUP}}"
OUT_DIR="${OUT_DIR:-${RESULTS_DIR}/neo4j}"
INPUT="${INPUT:-$(latest_openfda_input)}"
LIMIT="${LIMIT:-100}"
WARMUP="${WARMUP:-3}"
REPS="${REPS:-10}"
NEO4J_CONTAINER="${NEO4J_CONTAINER:-openfda-neo4j-bench}"
NEO4J_IMAGE="${NEO4J_IMAGE:-neo4j:5-community}"
NEO4J_URL="${NEO4J_URL:-http://127.0.0.1:7474}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-openfda-benchmark}"
START_NEO4J="${START_NEO4J:-1}"

if [[ -z "${INPUT}" || ! -f "${INPUT}" ]]; then
  echo "error: openFDA raw JSON input not found. Set INPUT=/path/to/openfda_food_raw.json" >&2
  exit 2
fi

if [[ "${START_NEO4J}" == "1" ]]; then
  docker rm -f "${NEO4J_CONTAINER}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${NEO4J_CONTAINER}" \
    -p 7474:7474 \
    -p 7687:7687 \
    -e "NEO4J_AUTH=${NEO4J_USER}/${NEO4J_PASSWORD}" \
    -e "NEO4J_dbms_memory_heap_initial__size=1G" \
    -e "NEO4J_dbms_memory_heap_max__size=1G" \
    "${NEO4J_IMAGE}" >/dev/null
fi

echo "Waiting for Neo4j HTTP endpoint..."
for _ in $(seq 1 90); do
  if curl -fsS -u "${NEO4J_USER}:${NEO4J_PASSWORD}" "${NEO4J_URL}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null; then
  GIT_DIRTY=false
else
  GIT_DIRTY=true
fi

python3 "${SCRIPT_DIR}/product_equivalent_benchmark.py" \
  --system neo4j \
  --input "${INPUT}" \
  --out-dir "${OUT_DIR}" \
  --run-id "${RUN_ID}" \
  --limit "${LIMIT}" \
  --warmup "${WARMUP}" \
  --reps "${REPS}" \
  --git-commit "${GIT_COMMIT}" \
  --git-dirty "${GIT_DIRTY}" \
  --neo4j-url "${NEO4J_URL}" \
  --neo4j-user "${NEO4J_USER}" \
  --neo4j-password "${NEO4J_PASSWORD}"

echo "Neo4j product-equivalent result:"
echo "  ${OUT_DIR}/product_equivalent_summary.csv"
