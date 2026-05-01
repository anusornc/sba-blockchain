# openFDA Food Benchmarks

Canonical current harness for the openFDA food-enforcement benchmark family.

Use this directory for:

- disk-backed SBA/Datomic scale runs,
- product-equivalent SBA, Neo4j, Hyperledger Fabric, and Ethereum comparison
  runs,
- artifact archiving and paper-ready asset generation.

Compatibility wrappers remain under `benchmarks/real-world/openfda-food/` so
older registry commands and local notes continue to work during cleanup.

Raw local run output still defaults to `benchmarks/real-world/results/` and
must not be exported directly. Archive public-safe evidence packages into
`benchmarks/real-world/artifacts/`.
