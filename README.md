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

## Datomic Dependency Notice

This public companion resolves `com.datomic/peer` from Maven Central for local
tests and demo use. If you run a full Datomic Pro transactor or external
storage setup, follow Datomic's deployment documentation and keep generated
runtime state outside version control.

## Run Tests

```bash
source scripts/dev-env.bash
clojure -M:test
```

Use `clojure` for non-interactive commands. The `clj` wrapper also works when
`rlwrap` is installed.

## Reproducibility

See `docs/REPRODUCIBILITY.md`.

## Security

See `docs/SECURITY.md`.
