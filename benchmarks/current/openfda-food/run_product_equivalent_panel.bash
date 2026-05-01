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
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results/${RUN_GROUP}}"
INPUT="${INPUT:-$(latest_openfda_input)}"
SYSTEMS="${SYSTEMS:-sba neo4j ethereum fabric}"
LIMIT="${LIMIT:-25}"
WARMUP="${WARMUP:-1}"
REPS="${REPS:-5}"

mkdir -p "${RESULTS_DIR}"

for system in ${SYSTEMS}; do
  echo
  echo "=== Product-equivalent openFDA benchmark: ${system} ==="
  RUN_GROUP="${RUN_GROUP}" \
  RESULTS_DIR="${RESULTS_DIR}" \
  INPUT="${INPUT}" \
  LIMIT="${LIMIT}" \
  WARMUP="${WARMUP}" \
  REPS="${REPS}" \
  bash "${SCRIPT_DIR}/run_product_equivalent_${system}.bash"
done

COMBINED="${RESULTS_DIR}/product_equivalent_combined_summary.csv"
python3 "${SCRIPT_DIR}/summarize_product_equivalent.py" --results-dir "${RESULTS_DIR}" --output "${COMBINED}"

echo
echo "Combined product-equivalent summary:"
echo "  ${COMBINED}"
