#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
TESTNET_DIR="${TESTNET_DIR:-${REPO_ROOT}/benchmarks/practical/fabric/fabric-samples/test-network}"
CHAINCODE_SRC="${CHAINCODE_SRC:-${SCRIPT_DIR}/fabric-chaincode}"
CHANNEL_NAME="${CHANNEL_NAME:-openfda-channel}"
CHAINCODE_NAME="${CHAINCODE_NAME:-openfda}"
FABRIC_IMAGE_TAG="${FABRIC_IMAGE_TAG:-2.5}"
PIN_FABRIC_COMPOSE_IMAGES="${PIN_FABRIC_COMPOSE_IMAGES:-1}"
PINNED_COMPOSE_BACKUPS=()

restore_pinned_compose_files() {
  for backup in "${PINNED_COMPOSE_BACKUPS[@]}"; do
    if [[ -f "${backup}" ]]; then
      mv "${backup}" "${backup%.openfda-benchmark.bak}"
    fi
  done
}

trap restore_pinned_compose_files EXIT

if [[ ! -d "${TESTNET_DIR}" ]]; then
  echo "error: Fabric test-network not found: ${TESTNET_DIR}" >&2
  echo "Run benchmarks/practical/fabric/setup-fabric.sh first, then rerun this setup." >&2
  exit 2
fi

if [[ ! -f "${CHAINCODE_SRC}/openfda_chaincode.go" ]]; then
  echo "error: openFDA Fabric chaincode not found: ${CHAINCODE_SRC}" >&2
  exit 2
fi

cd "${TESTNET_DIR}"

if [[ "${PIN_FABRIC_COMPOSE_IMAGES}" == "1" ]]; then
  echo "Pinning Fabric peer/orderer Docker images to ${FABRIC_IMAGE_TAG} for this setup run..."
  while IFS= read -r -d '' compose_file; do
    backup="${compose_file}.openfda-benchmark.bak"
    cp "${compose_file}" "${backup}"
    PINNED_COMPOSE_BACKUPS+=("${backup}")
    sed -i \
      -e "s#hyperledger/fabric-peer:latest#hyperledger/fabric-peer:${FABRIC_IMAGE_TAG}#g" \
      -e "s#hyperledger/fabric-orderer:latest#hyperledger/fabric-orderer:${FABRIC_IMAGE_TAG}#g" \
      -e "s#hyperledger/fabric-peer:[0-9][^[:space:]]*#hyperledger/fabric-peer:${FABRIC_IMAGE_TAG}#g" \
      -e "s#hyperledger/fabric-orderer:[0-9][^[:space:]]*#hyperledger/fabric-orderer:${FABRIC_IMAGE_TAG}#g" \
      "${compose_file}"
  done < <(find compose -type f \( -name '*.yaml' -o -name '*.yml' \) -print0)
fi

echo "Resetting Fabric test-network for openFDA product-equivalent benchmark..."
docker rm -f "peer0org1_${CHAINCODE_NAME}_ccaas" "peer0org2_${CHAINCODE_NAME}_ccaas" >/dev/null 2>&1 || true
./network.sh down
./network.sh up createChannel -c "${CHANNEL_NAME}" -s couchdb
./network.sh deployCCAAS -c "${CHANNEL_NAME}" -ccn "${CHAINCODE_NAME}" -ccp "${CHAINCODE_SRC}" -ccaasdocker true

echo "Fabric openFDA benchmark network ready:"
echo "  channel:   ${CHANNEL_NAME}"
echo "  chaincode: ${CHAINCODE_NAME}"
