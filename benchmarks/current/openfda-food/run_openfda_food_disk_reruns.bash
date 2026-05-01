#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

RUN_COUNT="${RUN_COUNT:-3}"
RUN_PREFIX="${RUN_PREFIX:-openfda_food_disk_$(date -u +%Y%m%d_%H%M%S)}"
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results}"
DATOMIC_PORT="${DATOMIC_PORT:-4434}"
DATOMIC_H2_PORT="${DATOMIC_H2_PORT:-4435}"
DATOMIC_HOST="${DATOMIC_HOST:-localhost}"
DATOMIC_DATA_DIR="${DATOMIC_DATA_DIR:-${RESULTS_DIR}/${RUN_PREFIX}_datomic_data}"
DATOMIC_LOG_DIR="${DATOMIC_LOG_DIR:-${RESULTS_DIR}/${RUN_PREFIX}_datomic_log}"
DATOMIC_TRANSACTOR_BIN="${DATOMIC_TRANSACTOR_BIN:-external/datomic-pro/bin/transactor}"
TX_CONFIG="${RESULTS_DIR}/${RUN_PREFIX}_transactor.properties"
TX_STDOUT="${RESULTS_DIR}/${RUN_PREFIX}_transactor.out"
SUMMARY_OUTPUT="${SUMMARY_OUTPUT:-${RESULTS_DIR}/${RUN_PREFIX}_summary.md}"
TX_CONFIG_ABS="${REPO_ROOT}/${TX_CONFIG}"
DATOMIC_DATA_DIR_ABS="${REPO_ROOT}/${DATOMIC_DATA_DIR}"
DATOMIC_LOG_DIR_ABS="${REPO_ROOT}/${DATOMIC_LOG_DIR}"
TX_PID_ABS="${REPO_ROOT}/${RESULTS_DIR}/${RUN_PREFIX}_transactor.pid"

mkdir -p "${RESULTS_DIR}" "${DATOMIC_DATA_DIR}" "${DATOMIC_LOG_DIR}"

if [[ ! -x "${DATOMIC_TRANSACTOR_BIN}" ]]; then
  echo "error: Datomic transactor executable not found: ${DATOMIC_TRANSACTOR_BIN}" >&2
  echo "       Set DATOMIC_TRANSACTOR_BIN to your Datomic Pro transactor path." >&2
  exit 1
fi

cat > "${TX_CONFIG}" <<EOF
protocol=dev
host=${DATOMIC_HOST}
port=${DATOMIC_PORT}
h2-port=${DATOMIC_H2_PORT}
data-dir=${DATOMIC_DATA_DIR_ABS}
log-dir=${DATOMIC_LOG_DIR_ABS}
pid-file=${TX_PID_ABS}
memory-index-threshold=32m
memory-index-max=256m
object-cache-max=128m
EOF

echo "Starting Datomic dev transactor"
echo "  config: ${TX_CONFIG}"
echo "  data:   ${DATOMIC_DATA_DIR}"
echo "  port:   ${DATOMIC_HOST}:${DATOMIC_PORT}"

"${DATOMIC_TRANSACTOR_BIN}" "${TX_CONFIG_ABS}" > "${TX_STDOUT}" 2>&1 &
tx_pid=$!

cleanup() {
  if kill -0 "${tx_pid}" >/dev/null 2>&1; then
    kill "${tx_pid}" >/dev/null 2>&1 || true
    wait "${tx_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if (echo > "/dev/tcp/${DATOMIC_HOST}/${DATOMIC_PORT}") >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${tx_pid}" >/dev/null 2>&1; then
    echo "error: transactor exited before opening port; see ${TX_STDOUT}" >&2
    exit 1
  fi
  sleep 1
done

if ! (echo > "/dev/tcp/${DATOMIC_HOST}/${DATOMIC_PORT}") >/dev/null 2>&1; then
  echo "error: timed out waiting for Datomic transactor on ${DATOMIC_HOST}:${DATOMIC_PORT}" >&2
  echo "see ${TX_STDOUT}" >&2
  exit 1
fi

RUN_COUNT="${RUN_COUNT}" \
RUN_PREFIX="${RUN_PREFIX}" \
RESULTS_DIR="${RESULTS_DIR}" \
SUMMARY_OUTPUT="${SUMMARY_OUTPUT}" \
DATOMIC_STORAGE="dev-transactor" \
DATOMIC_HOST="${DATOMIC_HOST}" \
DATOMIC_PORT="${DATOMIC_PORT}" \
bash "${SCRIPT_DIR}/run_openfda_food_reruns.bash"

echo
echo "Disk-backed openFDA reruns complete:"
echo "  summary: ${SUMMARY_OUTPUT}"
echo "  data:    ${DATOMIC_DATA_DIR}"
