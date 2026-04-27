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

The original research prototype uses Datomic Pro. Datomic Pro artifacts are
not redistributed in this public repository. To run the full system, provide
your own licensed Datomic dependency according to Datomic's terms and place it
outside version control.

## Run Tests

```bash
clj -M:test
```

If dependency resolution fails because Datomic Pro is unavailable, install or
configure Datomic locally first.

## Reproducibility

See `docs/REPRODUCIBILITY.md`.

## Security

See `docs/SECURITY.md`.
