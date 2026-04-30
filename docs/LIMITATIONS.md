# Evidence Boundaries and Limitations

This public companion artifact is intended to support reproducibility and
implementation review for a research prototype. It should not be interpreted as
a production-certified blockchain platform.

## Safe Evidence Scope

- Source-code feasibility for the SBA prototype.
- Deterministic public UHT supply-chain sample data.
- Unit/regression tests for public-safe modules.
- Local query benchmark harness and bundled repeated-run artifacts.
- Local Fabric invoke/query harness artifacts for the selected CCAAS run.
- openFDA public recall-data scale-check harness and selected artifact package.
- Product-equivalent openFDA exact-lookup benchmark artifacts for SBA/Datomic,
  Neo4j, Hyperledger Fabric, and Ethereum transaction-input scan baselines.

## Claims That Require Additional Evidence

- Production-grade distributed blockchain behavior.
- Demonstrated Byzantine fault tolerance in a deployed multi-node network.
- End-to-end cross-chain bridge latency with relayers, remote chain reads,
  transaction submission, and finality.
- Query performance at 10k, 100k, or million-entity scale.
- Product-to-product claims beyond the bundled exact-lookup openFDA workload.
- Security hardening against real adversarial API or infrastructure threats.

## Benchmark Interpretation

The bundled query evidence uses a single-machine local harness, warmed cache,
and the deterministic UHT dataset. The selected paper evidence run is
`main_revised_query_20260428_011`; repeated-run context is summarized in
`benchmarks/reproducibility/results/query/query_rerun_summary_20260428.md`.

The bundled openFDA evidence is an external-validity scale check over public
FDA food recall records. It is a recall/event lookup workload, not a complete
multi-party supply-chain traversal and not a product comparison benchmark.

The bundled product-equivalent openFDA panel uses the same public openFDA source
capture and exact-lookup query families across SBA/Datomic, Neo4j, Fabric, and
Ethereum adapters. It supports local product-equivalent latency comparison for
that workload only. The Ethereum adapter stores records as transaction input and
answers lookups by scanning block transactions; it is real execution but not an
indexed smart-contract design.

Use mean, p95, and p99 values for latency interpretation. Max latency captures
occasional runtime outliers and should not be treated as representative
steady-state behavior.

## Production Readiness Gap

Before production claims, the project would need multi-node deployment tests,
larger datasets, concurrency/load testing, security review, failure-recovery
tests, backup/restore validation, monitoring guidance, and operational runbooks.
