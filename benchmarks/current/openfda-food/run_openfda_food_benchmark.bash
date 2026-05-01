#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

RUN_ID="${RUN_ID:-openfda_food_$(date +%Y%m%d_%H%M%S)}"
OUT_DIR="${OUT_DIR:-benchmarks/real-world/results/${RUN_ID}}"
LIMIT="${LIMIT:-1000}"
WARMUP="${WARMUP:-30}"
REPS="${REPS:-100}"
DATOMIC_STORAGE="${DATOMIC_STORAGE:-mem}"
DATOMIC_HOST="${DATOMIC_HOST:-localhost}"
DATOMIC_PORT="${DATOMIC_PORT:-4334}"
DATOMIC_DB_NAME="${DATOMIC_DB_NAME:-openfda-food-${RUN_ID}}"

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  GIT_COMMIT="${GIT_COMMIT:-$(git rev-parse HEAD)}"
  if [[ -z "${GIT_DIRTY:-}" ]]; then
    if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
      GIT_DIRTY="true"
    else
      GIT_DIRTY="false"
    fi
  fi
else
  GIT_COMMIT="${GIT_COMMIT:-unknown}"
  GIT_DIRTY="${GIT_DIRTY:-unknown}"
fi

export RUN_ID OUT_DIR LIMIT WARMUP REPS GIT_COMMIT GIT_DIRTY
export DATOMIC_STORAGE DATOMIC_HOST DATOMIC_PORT DATOMIC_DB_NAME

clojure -M -m real-world.openfda-food-benchmark
