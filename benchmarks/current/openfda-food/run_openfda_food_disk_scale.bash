#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results}"
SCALE_PREFIX="${SCALE_PREFIX:-openfda_food_disk_scale_$(date -u +%Y%m%d_%H%M%S)}"
SCALE_LIMITS="${SCALE_LIMITS:-5000 10000 26000}"
SCALE_SUMMARY_OUTPUT="${SCALE_SUMMARY_OUTPUT:-${RESULTS_DIR}/${SCALE_PREFIX}_summary.md}"
RUN_COUNT_PER_SCALE="${RUN_COUNT_PER_SCALE:-1}"
WARMUP="${WARMUP:-30}"
REPS="${REPS:-100}"
OPENFDA_SKIP_CAP="${OPENFDA_SKIP_CAP:-25000}"
OPENFDA_MAX_LIMIT_WITH_SKIP="$((OPENFDA_SKIP_CAP + 1000))"

run_ids=()
limits=()

for limit in ${SCALE_LIMITS}; do
  if [[ "${limit}" -gt "${OPENFDA_MAX_LIMIT_WITH_SKIP}" ]]; then
    echo "error: LIMIT=${limit} exceeds openFDA skip pagination cap." >&2
    echo "       openFDA currently requires skip <= ${OPENFDA_SKIP_CAP}; max supported limit is ${OPENFDA_MAX_LIMIT_WITH_SKIP}." >&2
    exit 2
  fi

  run_prefix="${SCALE_PREFIX}_${limit}"
  echo
  echo "=== Disk-backed openFDA scale run: LIMIT=${limit} ==="
  RUN_COUNT="${RUN_COUNT_PER_SCALE}" \
  RUN_PREFIX="${run_prefix}" \
  RESULTS_DIR="${RESULTS_DIR}" \
  LIMIT="${limit}" \
  WARMUP="${WARMUP}" \
  REPS="${REPS}" \
  SUMMARY_OUTPUT="${RESULTS_DIR}/${run_prefix}_summary.md" \
  bash "${SCRIPT_DIR}/run_openfda_food_disk_reruns.bash"

  for i in $(seq 1 "${RUN_COUNT_PER_SCALE}"); do
    suffix="$(printf "%03d" "${i}")"
    run_ids+=("${run_prefix}_${suffix}")
    limits+=("${limit}")
  done
done

python3 "${SCRIPT_DIR}/summarize_openfda_food_scale.py" \
  --results-dir "${RESULTS_DIR}" \
  --runs "${run_ids[@]}" \
  --limits "${limits[@]}" \
  --required-warmup "${WARMUP}" \
  --required-reps "${REPS}" \
  --required-storage dev-transactor \
  --output "${SCALE_SUMMARY_OUTPUT}"

echo
echo "Disk-backed openFDA scale summary written:"
echo "  ${SCALE_SUMMARY_OUTPUT}"
