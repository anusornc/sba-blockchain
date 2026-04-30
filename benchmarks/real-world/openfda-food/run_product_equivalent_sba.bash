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
RUN_ID="${RUN_ID:-${RUN_GROUP}_sba}"
RESULTS_DIR="${RESULTS_DIR:-benchmarks/real-world/results/${RUN_GROUP}}"
OUT_DIR="${OUT_DIR:-${RESULTS_DIR}/sba}"
INPUT="${INPUT:-$(latest_openfda_input)}"
LIMIT="${LIMIT:-100}"
WARMUP="${WARMUP:-3}"
REPS="${REPS:-10}"
DATOMIC_STORAGE="${DATOMIC_STORAGE:-dev-transactor}"
DATOMIC_HOST="${DATOMIC_HOST:-localhost}"
DATOMIC_PORT="${DATOMIC_PORT:-4434}"
DATOMIC_H2_PORT="${DATOMIC_H2_PORT:-4435}"
DATOMIC_TRANSACTOR_BIN="${DATOMIC_TRANSACTOR_BIN:-external/datomic-pro/bin/transactor}"
DATOMIC_DATA_DIR="${DATOMIC_DATA_DIR:-${OUT_DIR}/datomic_data}"
DATOMIC_LOG_DIR="${DATOMIC_LOG_DIR:-${OUT_DIR}/datomic_log}"
DATOMIC_DB_NAME="${DATOMIC_DB_NAME:-openfda-product-equivalent-${RUN_ID}}"
TX_CONFIG="${OUT_DIR}/transactor.properties"
TX_STDOUT="${OUT_DIR}/transactor.out"
TX_PID="${OUT_DIR}/transactor.pid"

if [[ -z "${INPUT}" || ! -f "${INPUT}" ]]; then
  echo "error: openFDA raw JSON input not found. Set INPUT=/path/to/openfda_food_raw.json" >&2
  exit 2
fi

mkdir -p "${OUT_DIR}" "${DATOMIC_DATA_DIR}" "${DATOMIC_LOG_DIR}"

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null; then
  GIT_DIRTY=false
else
  GIT_DIRTY=true
fi

tx_pid=""
cleanup() {
  if [[ -n "${tx_pid}" ]] && kill -0 "${tx_pid}" >/dev/null 2>&1; then
    kill "${tx_pid}" >/dev/null 2>&1 || true
    wait "${tx_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${DATOMIC_STORAGE}" == "dev-transactor" ]]; then
  if [[ ! -x "${DATOMIC_TRANSACTOR_BIN}" ]]; then
    echo "error: Datomic transactor executable not found: ${DATOMIC_TRANSACTOR_BIN}" >&2
    echo "       Set DATOMIC_TRANSACTOR_BIN or DATOMIC_STORAGE=mem for a local smoke run." >&2
    exit 2
  fi

  cat > "${TX_CONFIG}" <<EOF
protocol=dev
host=${DATOMIC_HOST}
port=${DATOMIC_PORT}
h2-port=${DATOMIC_H2_PORT}
data-dir=${REPO_ROOT}/${DATOMIC_DATA_DIR}
log-dir=${REPO_ROOT}/${DATOMIC_LOG_DIR}
pid-file=${REPO_ROOT}/${TX_PID}
memory-index-threshold=32m
memory-index-max=256m
object-cache-max=128m
EOF

  echo "Starting SBA Datomic dev transactor..."
  "${DATOMIC_TRANSACTOR_BIN}" "${REPO_ROOT}/${TX_CONFIG}" > "${TX_STDOUT}" 2>&1 &
  tx_pid=$!

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
fi

OPENFDA_INPUT="${INPUT}" \
RUN_ID="${RUN_ID}" \
OUT_DIR="${OUT_DIR}" \
LIMIT="${LIMIT}" \
WARMUP="${WARMUP}" \
REPS="${REPS}" \
GIT_COMMIT="${GIT_COMMIT}" \
GIT_DIRTY="${GIT_DIRTY}" \
DATOMIC_STORAGE="${DATOMIC_STORAGE}" \
DATOMIC_HOST="${DATOMIC_HOST}" \
DATOMIC_PORT="${DATOMIC_PORT}" \
DATOMIC_DB_NAME="${DATOMIC_DB_NAME}" \
clojure -M -m real-world.openfda-food-benchmark

python3 - "${OUT_DIR}/openfda_food_latency_summary.csv" "${OUT_DIR}/product_equivalent_summary.csv" <<'PY'
import csv
import sys

source, target = sys.argv[1], sys.argv[2]
with open(source, newline="", encoding="utf-8") as src:
    reader = csv.DictReader(src)
    rows = list(reader)
    fields = ["system"] + list(reader.fieldnames or [])

with open(target, "w", newline="", encoding="utf-8") as dst:
    writer = csv.DictWriter(dst, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({"system": "sba", **row})
PY

python3 - "${OUT_DIR}/openfda_food_latency_raw.csv" "${OUT_DIR}/product_equivalent_latency_raw.csv" <<'PY'
import csv
import sys

source, target = sys.argv[1], sys.argv[2]
with open(source, newline="", encoding="utf-8") as src:
    reader = csv.DictReader(src)
    rows = list(reader)
    fields = ["system"] + list(reader.fieldnames or [])

with open(target, "w", newline="", encoding="utf-8") as dst:
    writer = csv.DictWriter(dst, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({"system": "sba", **row})
PY

echo "SBA product-equivalent result:"
echo "  ${OUT_DIR}/product_equivalent_summary.csv"
