#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:3000}"
API_TOKEN="${API_TOKEN:-}"
REPS="${REPS:-100}"
WARMUP="${WARMUP:-10}"
QR_CODE="${QR_CODE:-UHT-CHOC-2024-001-QR}"
BATCH_ID="${BATCH_ID:-UHT-CHOC-CM-2024-001}"
ENTITY_ID="${ENTITY_ID:-}"
ACTIVITY_ID="${ACTIVITY_ID:-}"
RUN_ID="${RUN_ID:-query_$(date +%Y%m%d_%H%M%S)}"
OUT_DIR="${OUT_DIR:-benchmarks/main-revised/results/${RUN_ID}}"

RAW_CSV="${OUT_DIR}/query_latency_raw.csv"
SUMMARY_CSV="${OUT_DIR}/query_latency_summary.csv"
MANIFEST="${OUT_DIR}/manifest.txt"

mkdir -p "${OUT_DIR}"

echo "run_id=${RUN_ID}" > "${MANIFEST}"
echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "${MANIFEST}"
echo "api_base_url=${API_BASE_URL}" >> "${MANIFEST}"
echo "reps=${REPS}" >> "${MANIFEST}"
echo "warmup=${WARMUP}" >> "${MANIFEST}"
echo "qr_code=${QR_CODE}" >> "${MANIFEST}"
echo "batch_id=${BATCH_ID}" >> "${MANIFEST}"
echo "entity_id=${ENTITY_ID}" >> "${MANIFEST}"
echo "activity_id=${ACTIVITY_ID}" >> "${MANIFEST}"
echo "git_commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)" >> "${MANIFEST}"
echo "semantic_validation=enabled" >> "${MANIFEST}"

if [[ -z "${API_TOKEN}" ]]; then
  echo "API_TOKEN is required for /api/query benchmark calls." >&2
  echo "Set API_TOKEN to a valid JWT before running this harness." >&2
  exit 1
fi

echo "query,iteration,http_code,status,latency_ms" > "${RAW_CSV}"

request_timed() {
  local method="$1"
  local path="$2"
  local with_auth="$3"
  local body="${4:-}"
  local response_file="$5"
  local url="${API_BASE_URL}${path}"
  local auth_args=()
  if [[ "${with_auth}" == "1" ]]; then
    auth_args=(-H "Authorization: Bearer ${API_TOKEN}")
  fi

  if [[ "${method}" == "POST" ]]; then
    curl -sS -o "${response_file}" \
      -w "%{http_code},%{time_total}" \
      -X POST "${url}" \
      "${auth_args[@]}" \
      -H "Content-Type: application/json" \
      -d "${body}"
  else
    curl -sS -o "${response_file}" \
      -w "%{http_code},%{time_total}" \
      -X GET "${url}" \
      "${auth_args[@]}"
  fi
}

response_semantically_ok() {
  local query_name="$1"
  local response_file="$2"
  local expected_template="${3:-}"

  QUERY_NAME="${query_name}" \
  RESPONSE_FILE="${response_file}" \
  EXPECTED_TEMPLATE="${expected_template}" \
  EXPECTED_BATCH_ID="${BATCH_ID}" \
  EXPECTED_QR_CODE="${QR_CODE}" \
  python3 - <<'PY'
import json
import os
import sys

query_name = os.environ["QUERY_NAME"]
response_file = os.environ["RESPONSE_FILE"]
expected_template = os.environ.get("EXPECTED_TEMPLATE", "")
expected_batch_id = os.environ.get("EXPECTED_BATCH_ID", "")
expected_qr_code = os.environ.get("EXPECTED_QR_CODE", "")

try:
    payload = json.loads(open(response_file, encoding="utf-8").read() or "{}")
except Exception:
    print("0")
    sys.exit(0)

data = payload.get("data", {}) or {}
if payload.get("success") is not True:
    print("0")
    sys.exit(0)

if query_name == "q1_trace_qr":
    product = data.get("product", {}) or {}
    stages = ((data.get("journey", {}) or {}).get("stages", [])) or []
    ok = (
        product.get("batch") == expected_batch_id
        and product.get("qr-code") == expected_qr_code
        and len(stages) > 0
    )
    print("1" if ok else "0")
    sys.exit(0)

count = data.get("count")
template_used = data.get("template-used")
ok = bool(count and count > 0)
if expected_template:
    ok = ok and template_used == expected_template
print("1" if ok else "0")
PY
}

bench_one() {
  local query_name="$1"
  local method="$2"
  local path="$3"
  local with_auth="$4"
  local body="${5:-}"
  local expected_template="${6:-}"

  echo "Warmup: ${query_name} (${WARMUP} requests)"
  for _ in $(seq 1 "${WARMUP}"); do
    request_timed "${method}" "${path}" "${with_auth}" "${body}" "/dev/null" >/dev/null || true
  done

  echo "Measure: ${query_name} (${REPS} requests)"
  for i in $(seq 1 "${REPS}"); do
    local result
    local response_file
    response_file="$(mktemp)"
    result="$(request_timed "${method}" "${path}" "${with_auth}" "${body}" "${response_file}" || echo "000,0")"
    local code="${result%%,*}"
    local sec="${result##*,}"
    local status="ok"
    if [[ "${code}" -lt 200 || "${code}" -ge 300 ]]; then
      status="error"
    elif [[ "$(response_semantically_ok "${query_name}" "${response_file}" "${expected_template}")" != "1" ]]; then
      status="semantic-error"
    fi
    local ms
    ms="$(awk "BEGIN {printf \"%.3f\", ${sec} * 1000}")"
    echo "${query_name},${i},${code},${status},${ms}" >> "${RAW_CSV}"
    rm -f "${response_file}"
  done
}

echo "Checking API health..."
if ! curl -sS "${API_BASE_URL}/health" >/dev/null; then
  echo "API is not reachable at ${API_BASE_URL}" >&2
  exit 1
fi

echo "Seeding sample data (best effort)..."
seed_response="$(curl -sS -w $'\n%{http_code}' -X POST "${API_BASE_URL}/api/dev/load-sample-data" || true)"
seed_body="${seed_response%$'\n'*}"
seed_code="${seed_response##*$'\n'}"
if ! [[ "${seed_code}" =~ ^[0-9]{3}$ ]]; then
  seed_code="000"
fi
echo "seed_http_code=${seed_code}" >> "${MANIFEST}"

if [[ "${seed_code}" -ge 200 && "${seed_code}" -lt 300 ]]; then
  seed_meta="$(
    SEED_JSON="${seed_body}" python3 - <<'PY'
import json
import os

raw = os.environ.get("SEED_JSON", "")
try:
    payload = json.loads(raw).get("data", {})
except Exception:
    payload = {}

anchors = payload.get("benchmark-anchors", {}) or {}
counts = payload.get("counts", {}) or {}

for key, value in [
    ("seed_dataset_source", payload.get("dataset-source", "")),
    ("seed_agents", counts.get("agents", "")),
    ("seed_products", counts.get("products", "")),
    ("seed_activities", counts.get("activities", "")),
    ("seed_relationships", counts.get("relationships", "")),
    ("seed_qr_code", anchors.get("qr-code", "")),
    ("seed_batch_id", anchors.get("batch-id", "")),
    ("seed_entity_id", anchors.get("entity-id", "")),
    ("seed_activity_id", anchors.get("activity-id", "")),
]:
    print(f"{key}={value}")
PY
  )"
  while IFS='=' read -r key value; do
    [[ -n "${key}" ]] || continue
    echo "${key}=${value}" >> "${MANIFEST}"
    case "${key}" in
      seed_qr_code)
        if [[ -n "${value}" ]]; then QR_CODE="${value}"; fi
        ;;
      seed_batch_id)
        if [[ -n "${value}" ]]; then BATCH_ID="${value}"; fi
        ;;
      seed_entity_id)
        if [[ -n "${value}" ]]; then ENTITY_ID="${value}"; fi
        ;;
      seed_activity_id)
        if [[ -n "${value}" ]]; then ACTIVITY_ID="${value}"; fi
        ;;
    esac
  done <<< "${seed_meta}"
fi

if [[ -z "${ENTITY_ID}" || -z "${ACTIVITY_ID}" ]]; then
  echo "Missing ENTITY_ID/ACTIVITY_ID after seed. Seeding must return benchmark anchors." >&2
  exit 1
fi

echo "effective_qr_code=${QR_CODE}" >> "${MANIFEST}"
echo "effective_batch_id=${BATCH_ID}" >> "${MANIFEST}"
echo "effective_entity_id=${ENTITY_ID}" >> "${MANIFEST}"
echo "effective_activity_id=${ACTIVITY_ID}" >> "${MANIFEST}"

q2_body=$(cat <<EOF
{"query":{"find":"?p","where":[["?p",":traceability/batch","${BATCH_ID}"]]}}
EOF
)
q3_body=$(cat <<EOF
{"query":{"find":"?e","where":[["?e",":prov/entity","${ENTITY_ID}"]]}}
EOF
)
q4_body=$(cat <<EOF
{"query":{"find":"?a","where":[["?a",":prov/activity","${ACTIVITY_ID}"]]}}
EOF
)

# Q1: entity-style lookup by QR/batch trace endpoint (public route)
bench_one "q1_trace_qr" "GET" "/api/trace/${QR_CODE}" "0"

# Q2-Q4: authenticated query endpoint (whitelisted templates)
bench_one "q2_batch_lookup" "POST" "/api/query" "1" "${q2_body}" "get-traceability-products"
bench_one "q3_prov_entities" "POST" "/api/query" "1" "${q3_body}" "get-prov-entities"
bench_one "q4_prov_activities" "POST" "/api/query" "1" "${q4_body}" "get-prov-activities"

echo "query,ok_count,error_count,mean_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms" > "${SUMMARY_CSV}"

for q in $(tail -n +2 "${RAW_CSV}" | cut -d, -f1 | sort -u); do
  ok_count="$(awk -F, -v q="${q}" '$1==q && $4=="ok" {c++} END {print c+0}' "${RAW_CSV}")"
  err_count="$(awk -F, -v q="${q}" '$1==q && $4!="ok" {c++} END {print c+0}' "${RAW_CSV}")"

  if [[ "${ok_count}" -gt 0 ]]; then
    vals_file="$(mktemp)"
    awk -F, -v q="${q}" '$1==q && $4=="ok" {print $5}' "${RAW_CSV}" | sort -n > "${vals_file}"

    mean="$(awk '{s+=$1} END {printf "%.3f", s/NR}' "${vals_file}")"
    min="$(head -n 1 "${vals_file}")"
    max="$(tail -n 1 "${vals_file}")"

    p50_idx=$(( (ok_count + 1) / 2 ))
    p95_idx=$(( (ok_count * 95 + 99) / 100 ))
    p99_idx=$(( (ok_count * 99 + 99) / 100 ))

    p50="$(awk -v idx="${p50_idx}" 'NR==idx {print; exit}' "${vals_file}")"
    p95="$(awk -v idx="${p95_idx}" 'NR==idx {print; exit}' "${vals_file}")"
    p99="$(awk -v idx="${p99_idx}" 'NR==idx {print; exit}' "${vals_file}")"

    rm -f "${vals_file}"
  else
    mean="NA"
    p50="NA"
    p95="NA"
    p99="NA"
    min="NA"
    max="NA"
  fi

  echo "${q},${ok_count},${err_count},${mean},${p50},${p95},${p99},${min},${max}" >> "${SUMMARY_CSV}"
done

total_errors="$(awk -F, 'NR>1 {s+=$3} END {print s+0}' "${SUMMARY_CSV}")"
echo "total_errors=${total_errors}" >> "${MANIFEST}"

if [[ "${total_errors}" -gt 0 ]]; then
  echo "Query harness completed with errors (total_errors=${total_errors})." >&2
  exit 2
fi

echo "Query harness complete."
echo "Raw: ${RAW_CSV}"
echo "Summary: ${SUMMARY_CSV}"
echo "Manifest: ${MANIFEST}"
