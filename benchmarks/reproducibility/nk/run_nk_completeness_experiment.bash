#!/usr/bin/env bash
# NK completeness experiment: varies interdependency K, records the
# completeness score and rubric recall decision per generated provenance
# path. Writes raw rows, a per-K summary CSV, and a manifest to
# benchmarks/reproducibility/nk/out/.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "${ROOT_DIR}/scripts/dev-env.bash"
cd "${ROOT_DIR}"

exec clojure -M:nk-experiment
