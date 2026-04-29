#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

RUN_COUNT="${RUN_COUNT:-3}"
RUN_PREFIX="${RUN_PREFIX:-openfda_food_realworld_$(date -u +%Y%m%d_%H%M%S)}"
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results}"
LIMIT="${LIMIT:-1000}"
WARMUP="${WARMUP:-30}"
REPS="${REPS:-100}"
DATOMIC_STORAGE="${DATOMIC_STORAGE:-mem}"
DATOMIC_HOST="${DATOMIC_HOST:-localhost}"
DATOMIC_PORT="${DATOMIC_PORT:-4334}"
SUMMARY_OUTPUT="${SUMMARY_OUTPUT:-${RESULTS_DIR}/${RUN_PREFIX}_summary.md}"

if [[ "${RUN_COUNT}" -lt 1 ]]; then
  echo "error: RUN_COUNT must be >= 1" >&2
  exit 2
fi

run_ids=()
for i in $(seq 1 "${RUN_COUNT}"); do
  suffix="$(printf "%03d" "${i}")"
  run_id="${RUN_PREFIX}_${suffix}"
  out_dir="${RESULTS_DIR}/${run_id}"

  if [[ -e "${out_dir}" ]]; then
    echo "error: output directory already exists: ${out_dir}" >&2
    exit 2
  fi

  echo "[${i}/${RUN_COUNT}] Running ${run_id}"
  RUN_ID="${run_id}" \
    OUT_DIR="${out_dir}" \
    LIMIT="${LIMIT}" \
    WARMUP="${WARMUP}" \
    REPS="${REPS}" \
    DATOMIC_STORAGE="${DATOMIC_STORAGE}" \
    DATOMIC_HOST="${DATOMIC_HOST}" \
    DATOMIC_PORT="${DATOMIC_PORT}" \
    DATOMIC_DB_NAME="openfda-food-${run_id}" \
    bash "${SCRIPT_DIR}/run_openfda_food_benchmark.bash"
  run_ids+=("${run_id}")
done

selected_run="${run_ids[$((${#run_ids[@]} - 1))]}"

python3 "${SCRIPT_DIR}/summarize_openfda_food_reruns.py" \
  --results-dir "${RESULTS_DIR}" \
  --runs "${run_ids[@]}" \
  --selected-run "${selected_run}" \
  --required-warmup "${WARMUP}" \
  --required-reps "${REPS}" \
  --required-storage "${DATOMIC_STORAGE}" \
  --output "${SUMMARY_OUTPUT}"

echo
echo "openFDA rerun summary written:"
echo "  ${SUMMARY_OUTPUT}"
