# Product-Equivalent openFDA Benchmark Artifact Package

Artifact ID: `product_equivalent_openfda_sba_limit1000_20260430_001`

This package preserves public-safe evidence for the openFDA product-equivalent
benchmark comparing SBA/Datomic, Neo4j, Hyperledger Fabric, and Ethereum.

## Included

- Run manifests for: `product_equivalent_openfda_sba_limit1000_20260430_001_001`, `product_equivalent_openfda_sba_limit1000_20260430_001_002`, `product_equivalent_openfda_sba_limit1000_20260430_001_003`, `product_equivalent_openfda_sba_limit1000_20260430_001_004`, `product_equivalent_openfda_sba_limit1000_20260430_001_005`
- Per-system raw latency CSV files
- Per-system summary CSV files
- Per-run combined summary CSV files
- Aggregate Markdown and CSV summaries
- `ARTIFACT_MANIFEST.tsv` with byte counts and SHA-256 hashes
- `paper-assets/openfda_product_equivalent_limit1000_table.md`
- `paper-assets/openfda_product_equivalent_limit1000_table.tex`
- `paper-assets/openfda_product_equivalent_limit1000_p95.svg`

## Source JSON Policy

Raw openFDA JSON captures are not copied into this archive. They are public API
captures, but can be large; each run manifest records `source_raw_json_sha256`
and the source path used during execution.

## Excluded

- Raw openFDA JSON captures
- Datomic data directories
- Datomic logs, pid files, and transactor config
- Fabric runtime crypto/channel artifacts and Docker state

These excluded files are runtime state or source captures, not benchmark
measurements.
