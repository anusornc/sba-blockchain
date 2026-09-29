# Semantic Blockchain Architecture (SBA)

Public companion repository for the Semantic Blockchain Architecture (SBA)
implementation described in the associated paper.

This repository intentionally contains only public-safe source code,
tests, public sample resources, and reproducibility harnesses. It does not
include private notes, generated credentials, Fabric crypto material, or
private repository history.

## Contents

- `src/` - Clojure implementation
- `test/` - Kaocha test suite
- `examples/` - small usage examples
- `resources/ontologies/prov-o.rdf` - W3C PROV-O ontology resource
- `resources/datasets/uht-supply-chain/data.edn` - public sample dataset
- `resources/openapi/api.yaml` - API description
- `benchmarks/reproducibility/` - public benchmark harnesses
- `benchmarks/current/` - current benchmark harnesses
- `benchmarks/current/openfda-food/run_product_equivalent_reruns.bash` - product-equivalent openFDA benchmark panel
- `benchmarks/real-world/artifacts/` - public-safe openFDA artifact packages
- `frontend/ontology-traceability/` - read-only traceability visualization frontend
- `frontend/e2e/tests/ontology-traceability.spec.ts` - browser smoke test for the visualization frontend
- `docs/LIMITATIONS.md` - evidence boundaries and production-readiness gaps

The private work repository keeps broader project notes under `docs/current/`,
`docs/reproducibility/`, and `evidence/paper-current/`. This public release
does not export those trees wholesale because some files contain private
research workflow context. Public-safe setup, test, benchmark, limitation, and
security instructions are generated into this repository during export.

## Datomic Dependency Notice

This public companion resolves the Datomic Free peer from Clojars for
local tests and demo use. If you run a full Datomic Pro transactor or
external storage setup, follow Datomic's deployment documentation and
keep generated runtime state outside version control.

## Run Tests

```bash
source scripts/dev-env.bash
clojure -M:test
```

Use `clojure` for non-interactive commands. The `clj` wrapper also works when
`rlwrap` is installed.

## Ontology Traceability Frontend

The React frontend is a read-only companion for inspecting SBA trace graphs. It
uses `GET /api/ui/trace/{code}?kind=qr|batch` and `GET /api/ui/ontology`.

```bash
cd frontend/ontology-traceability
npm install
VITE_SBA_API_BASE_URL=http://127.0.0.1:3000 npm run build
```

This frontend is visualization/inspection evidence only. It is not a benchmark
harness and must not be cited as throughput or latency evidence.

## Reproducibility

See `docs/REPRODUCIBILITY.md`.

## Security

See `docs/SECURITY.md`.
