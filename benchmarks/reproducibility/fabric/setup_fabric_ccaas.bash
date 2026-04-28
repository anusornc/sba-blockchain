#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FABRIC_DIR="${FABRIC_DIR:-${ROOT_DIR}/benchmarks/practical/fabric}"
FABRIC_SAMPLES_DIR="${FABRIC_SAMPLES_DIR:-${FABRIC_DIR}/fabric-samples}"
TESTNET_DIR="${TESTNET_DIR:-${FABRIC_SAMPLES_DIR}/test-network}"
CHANNEL_NAME="${CHANNEL_NAME:-benchmark-channel}"
CHAINCODE_NAME="${CHAINCODE_NAME:-benchmark}"
FABRIC_IMAGE_TAG="${FABRIC_IMAGE_TAG:-2.5}"
CA_IMAGE_TAG="${CA_IMAGE_TAG:-1.5}"
GOCACHE="${GOCACHE:-/tmp/go-build-fabric}"

export GOCACHE

if [[ ! -d "${TESTNET_DIR}" ]]; then
  mkdir -p "${FABRIC_DIR}"
  cd "${FABRIC_DIR}"
  curl -sSL https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/bootstrap.sh |
    bash -s -- "${FABRIC_IMAGE_TAG}.0" "${CA_IMAGE_TAG}.0"
fi

cd "${TESTNET_DIR}"

echo "Pinning Fabric peer/orderer compose images to ${FABRIC_IMAGE_TAG}"
find compose -type f \( -name '*.yaml' -o -name '*.yml' \) -print0 |
  xargs -0 sed -i \
    -e "s#hyperledger/fabric-peer:latest#hyperledger/fabric-peer:${FABRIC_IMAGE_TAG}#g" \
    -e "s#hyperledger/fabric-orderer:latest#hyperledger/fabric-orderer:${FABRIC_IMAGE_TAG}#g"

echo "Starting Fabric test-network channel ${CHANNEL_NAME}"
./network.sh down 2>/dev/null || true
./network.sh up createChannel -c "${CHANNEL_NAME}" -s couchdb

echo "Deploying ${CHAINCODE_NAME} as chaincode-as-a-service"
./network.sh deployCCAAS \
  -c "${CHANNEL_NAME}" \
  -ccn "${CHAINCODE_NAME}" \
  -ccp ../asset-transfer-basic/chaincode-external \
  -ccaasdocker true

echo "Fabric CCAAS benchmark network is ready"
