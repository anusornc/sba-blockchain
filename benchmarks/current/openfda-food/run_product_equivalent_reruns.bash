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

RUN_COUNT="${RUN_COUNT:-5}"
RUN_PREFIX="${RUN_PREFIX:-product_equivalent_openfda_$(date -u +%Y%m%d_%H%M%S)}"
RESULTS_ROOT="${RESULTS_ROOT:-benchmarks/real-world/results}"
INPUT="${INPUT:-$(latest_openfda_input)}"
SYSTEMS="${SYSTEMS:-sba neo4j ethereum fabric}"
LIMIT="${LIMIT:-100}"
WARMUP="${WARMUP:-3}"
REPS="${REPS:-10}"
SUMMARY_CSV="${SUMMARY_CSV:-${RESULTS_ROOT}/${RUN_PREFIX}_aggregate_summary.csv}"
SUMMARY_MD="${SUMMARY_MD:-${RESULTS_ROOT}/${RUN_PREFIX}_aggregate_summary.md}"

if [[ "${RUN_COUNT}" -lt 1 ]]; then
  echo "error: RUN_COUNT must be >= 1" >&2
  exit 2
fi

if [[ -z "${INPUT}" || ! -f "${INPUT}" ]]; then
  echo "error: openFDA raw JSON input not found. Set INPUT=/path/to/openfda_food_raw.json" >&2
  exit 2
fi

run_names=()
for run in $(seq 1 "${RUN_COUNT}"); do
  suffix="$(printf "%03d" "${run}")"
  run_group="${RUN_PREFIX}_${suffix}"
  run_names+=("${run_group}")

  echo
  echo "=== Product-equivalent openFDA rerun ${suffix}/${RUN_COUNT}: ${run_group} ==="
  RUN_GROUP="${run_group}" \
  RESULTS_DIR="${RESULTS_ROOT}/${run_group}" \
  INPUT="${INPUT}" \
  SYSTEMS="${SYSTEMS}" \
  LIMIT="${LIMIT}" \
  WARMUP="${WARMUP}" \
  REPS="${REPS}" \
  bash "${SCRIPT_DIR}/run_product_equivalent_panel.bash"
done

python3 "${SCRIPT_DIR}/summarize_product_equivalent_reruns.py" \
  --results-dir "${RESULTS_ROOT}" \
  --runs "${run_names[@]}" \
  --output-csv "${SUMMARY_CSV}" \
  --output-md "${SUMMARY_MD}"

echo
echo "Product-equivalent rerun aggregate:"
echo "  ${SUMMARY_MD}"
echo "  ${SUMMARY_CSV}"
