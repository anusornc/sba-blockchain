#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TESTNET_DIR_DEFAULT="${ROOT_DIR}/benchmarks/practical/fabric/fabric-samples/test-network"

TESTNET_DIR="${TESTNET_DIR:-${TESTNET_DIR_DEFAULT}}"
CHANNEL_NAME="${CHANNEL_NAME:-benchmark-channel}"
CHAINCODE_NAME="${CHAINCODE_NAME:-benchmark}"
CHAINCODE_MODE="${CHAINCODE_MODE:-standard}"
REPS="${REPS:-50}"
WARMUP="${WARMUP:-5}"
WARMUP_SLEEP_SECS="${WARMUP_SLEEP_SECS:-0.5}"
MEASURE_SLEEP_SECS="${MEASURE_SLEEP_SECS:-0}"
RUN_ID="${RUN_ID:-fabric_$(date +%Y%m%d_%H%M%S)}"
ASSET_PREFIX="${ASSET_PREFIX:-${RUN_ID//[^[:alnum:]_]/_}}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/benchmarks/reproducibility/results/fabric/${RUN_ID}}"

WRITE_RAW="${OUT_DIR}/fabric_write_raw.csv"
READ_RAW="${OUT_DIR}/fabric_read_raw.csv"
SUMMARY="${OUT_DIR}/fabric_summary.csv"
MANIFEST="${OUT_DIR}/manifest.txt"

ORDERER_CA_REL="organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem"
ORG1_CA_REL="organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"
ORG2_CA_REL="organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt"

mkdir -p "${OUT_DIR}"

echo "run_id=${RUN_ID}" > "${MANIFEST}"
echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "${MANIFEST}"
echo "testnet_dir=${TESTNET_DIR}" >> "${MANIFEST}"
echo "channel_name=${CHANNEL_NAME}" >> "${MANIFEST}"
echo "chaincode_name=${CHAINCODE_NAME}" >> "${MANIFEST}"
echo "chaincode_mode=${CHAINCODE_MODE}" >> "${MANIFEST}"
echo "asset_prefix=${ASSET_PREFIX}" >> "${MANIFEST}"
echo "reps=${REPS}" >> "${MANIFEST}"
echo "warmup=${WARMUP}" >> "${MANIFEST}"
echo "warmup_sleep_secs=${WARMUP_SLEEP_SECS}" >> "${MANIFEST}"
echo "measure_sleep_secs=${MEASURE_SLEEP_SECS}" >> "${MANIFEST}"
echo "git_commit=$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)" >> "${MANIFEST}"
if git -C "${ROOT_DIR}" diff --quiet 2>/dev/null && git -C "${ROOT_DIR}" diff --cached --quiet 2>/dev/null; then
  echo "git_dirty=false" >> "${MANIFEST}"
else
  echo "git_dirty=true" >> "${MANIFEST}"
fi

if [[ ! -d "${TESTNET_DIR}" ]]; then
  echo "Fabric test-network not found: ${TESTNET_DIR}" >&2
  exit 1
fi

cd "${TESTNET_DIR}"

if ! docker ps --format '{{.Names}}' | grep -q "peer0.org1.example.com"; then
  echo "Fabric network is not running (peer0.org1.example.com not found)." >&2
  exit 1
fi

export PATH="${PWD}/../bin:${PATH}"
export FABRIC_CFG_PATH="${PWD}/../config/"
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE="${PWD}/${ORG1_CA_REL}"
export CORE_PEER_MSPCONFIGPATH="${PWD}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp"
export CORE_PEER_ADDRESS=localhost:7051

ORDERER_CA="${PWD}/${ORDERER_CA_REL}"
ORG1_CA="${PWD}/${ORG1_CA_REL}"
ORG2_CA="${PWD}/${ORG2_CA_REL}"

echo "iteration,success,latency_ms,tps" > "${WRITE_RAW}"
echo "iteration,success,latency_ms,qps" > "${READ_RAW}"

invoke_create_asset() {
  local asset_id="$1"
  local owner="$2"
  local value="$3"
  peer chaincode invoke \
    -o localhost:7050 \
    --ordererTLSHostnameOverride orderer.example.com \
    --tls \
    --cafile "${ORDERER_CA}" \
    -C "${CHANNEL_NAME}" \
    -n "${CHAINCODE_NAME}" \
    --peerAddresses localhost:7051 \
    --tlsRootCertFiles "${ORG1_CA}" \
    --peerAddresses localhost:9051 \
    --tlsRootCertFiles "${ORG2_CA}" \
    -c "{\"function\":\"CreateAsset\",\"Args\":[\"${asset_id}\",\"blue\",\"5\",\"${owner}\",\"${value}\"]}"
}

query_asset() {
  local asset_id="$1"
  peer chaincode query \
    -C "${CHANNEL_NAME}" \
    -n "${CHAINCODE_NAME}" \
    -c "{\"Args\":[\"ReadAsset\",\"${asset_id}\"]}"
}

echo "Warmup invoke (${WARMUP})..."
for i in $(seq 1 "${WARMUP}"); do
  invoke_create_asset "${ASSET_PREFIX}_warmup_${i}" "warmup" "$((RANDOM % 1000))" >/dev/null 2>&1 || true
  sleep "${WARMUP_SLEEP_SECS}"
done

echo "Measured write invoke (${REPS})..."
write_phase_start_ns="$(date +%s%N)"
for i in $(seq 1 "${REPS}"); do
  start_ns="$(date +%s%N)"
  if output="$(invoke_create_asset "${ASSET_PREFIX}_asset_${i}" "owner_${i}" "$((RANDOM % 1000))" 2>&1)"; then
    success=1
    if ! echo "${output}" | grep -q "Chaincode invoke successful"; then
      success=0
    fi
  else
    success=0
  fi
  end_ns="$(date +%s%N)"
  latency_ms="$(( (end_ns - start_ns) / 1000000 ))"
  if [[ "${latency_ms}" -le 0 ]]; then
    latency_ms=1
  fi
  tps="$(awk "BEGIN {printf \"%.4f\", 1000/${latency_ms}}")"
  echo "${i},${success},${latency_ms},${tps}" >> "${WRITE_RAW}"
  if [[ "${MEASURE_SLEEP_SECS}" != "0" && "${MEASURE_SLEEP_SECS}" != "0.0" ]]; then
    sleep "${MEASURE_SLEEP_SECS}"
  fi
done
write_phase_end_ns="$(date +%s%N)"
WRITE_TOTAL_MS="$(( (write_phase_end_ns - write_phase_start_ns) / 1000000 ))"
echo "write_total_ms=${WRITE_TOTAL_MS}" >> "${MANIFEST}"

echo "Measured read query (${REPS})..."
read_phase_start_ns="$(date +%s%N)"
for i in $(seq 1 "${REPS}"); do
  idx=$(( (i % REPS) + 1 ))
  start_ns="$(date +%s%N)"
  if query_asset "${ASSET_PREFIX}_asset_${idx}" >/dev/null 2>&1; then
    success=1
  else
    success=0
  fi
  end_ns="$(date +%s%N)"
  latency_ms="$(( (end_ns - start_ns) / 1000000 ))"
  if [[ "${latency_ms}" -le 0 ]]; then
    latency_ms=1
  fi
  qps="$(awk "BEGIN {printf \"%.4f\", 1000/${latency_ms}}")"
  echo "${i},${success},${latency_ms},${qps}" >> "${READ_RAW}"
  if [[ "${MEASURE_SLEEP_SECS}" != "0" && "${MEASURE_SLEEP_SECS}" != "0.0" ]]; then
    sleep "${MEASURE_SLEEP_SECS}"
  fi
done
read_phase_end_ns="$(date +%s%N)"
READ_TOTAL_MS="$(( (read_phase_end_ns - read_phase_start_ns) / 1000000 ))"
echo "read_total_ms=${READ_TOTAL_MS}" >> "${MANIFEST}"

echo "metric,ok_count,error_count,mean_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms,mean_rate" > "${SUMMARY}"

summarize_metric() {
  local metric_name="$1"
  local csv_file="$2"
  local rate_col="$3"
  local total_ms="$4"

  ok_count="$(awk -F, 'NR>1 && $2==1 {c++} END {print c+0}' "${csv_file}")"
  err_count="$(awk -F, 'NR>1 && $2!=1 {c++} END {print c+0}' "${csv_file}")"

  if [[ "${ok_count}" -gt 0 ]]; then
    vals_file="$(mktemp)"
    awk -F, 'NR>1 && $2==1 {print $3}' "${csv_file}" | sort -n > "${vals_file}"

    mean_ms="$(awk '{s+=$1} END {printf "%.3f", s/NR}' "${vals_file}")"
    min_ms="$(head -n 1 "${vals_file}")"
    max_ms="$(tail -n 1 "${vals_file}")"
    p50_idx=$(( (ok_count + 1) / 2 ))
    p95_idx=$(( (ok_count * 95 + 99) / 100 ))
    p99_idx=$(( (ok_count * 99 + 99) / 100 ))
    p50_ms="$(awk -v idx="${p50_idx}" 'NR==idx {print; exit}' "${vals_file}")"
    p95_ms="$(awk -v idx="${p95_idx}" 'NR==idx {print; exit}' "${vals_file}")"
    p99_ms="$(awk -v idx="${p99_idx}" 'NR==idx {print; exit}' "${vals_file}")"
    rm -f "${vals_file}"

    if [[ "${total_ms}" -gt 0 ]]; then
      mean_rate="$(awk -v ok="${ok_count}" -v total_ms="${total_ms}" 'BEGIN {printf "%.4f", (ok * 1000) / total_ms}')"
    else
      mean_rate="NA"
    fi
  else
    mean_ms="NA"
    min_ms="NA"
    max_ms="NA"
    p50_ms="NA"
    p95_ms="NA"
    p99_ms="NA"
    mean_rate="NA"
  fi

  echo "${metric_name},${ok_count},${err_count},${mean_ms},${p50_ms},${p95_ms},${p99_ms},${min_ms},${max_ms},${mean_rate}" >> "${SUMMARY}"
}

summarize_metric "fabric_write_invoke" "${WRITE_RAW}" 4 "${WRITE_TOTAL_MS}"
summarize_metric "fabric_read_query" "${READ_RAW}" 4 "${READ_TOTAL_MS}"

echo "Fabric invoke benchmark complete."
echo "Write raw: ${WRITE_RAW}"
echo "Read raw: ${READ_RAW}"
echo "Summary: ${SUMMARY}"
echo "Manifest: ${MANIFEST}"
