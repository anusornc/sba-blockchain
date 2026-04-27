#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export JAVA_HOME="${ROOT_DIR}/.tools/jdk"
export HOME="${ROOT_DIR}/.home"
export PATH="${ROOT_DIR}/.tools/jdk/bin:${ROOT_DIR}/.tools/clojure/bin:${ROOT_DIR}/.tools/rlwrap/bin:${PATH}"

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "JAVA_HOME=${JAVA_HOME}"
  java -version
  clojure -Sdescribe
fi
