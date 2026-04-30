# Product-Equivalent openFDA Rerun Aggregate

- runs: `product_equivalent_openfda_sba_20260430_001_001, product_equivalent_openfda_sba_20260430_001_002, product_equivalent_openfda_sba_20260430_001_003, product_equivalent_openfda_sba_20260430_001_004, product_equivalent_openfda_sba_20260430_001_005`
- source_raw_json_sha256: `06cc50661e0f19fa8f8d97109de3234609e41cb4380561d7d97a893c9d213907`
- limit: `100`
- usable_records: `100`
- warmup: `3`
- reps: `10`
- git_commit: `0bd0e4f98528f787fb9bef98504c306ad656862c`
- git_dirty_values: `false`
- manifest_total_error_nonzero: `0`

## Query Latency Aggregate

| system | query | runs | ok | errors | mean(ms) | median p50(ms) | mean p95(ms) | p95 min-max(ms) | max observed(ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ethereum | q1_recall_number_lookup | 5 | 50 | 0 | 137.104 | 135.795 | 162.011 | 143.496 - 176.450 | 176.450 |
| ethereum | q2_entity_uuid_lookup | 5 | 50 | 0 | 146.054 | 150.099 | 157.043 | 143.315 - 170.132 | 170.132 |
| ethereum | q3_activity_uuid_lookup | 5 | 50 | 0 | 131.605 | 124.222 | 165.199 | 156.160 - 178.495 | 178.495 |
| ethereum | q4_recalling_firm_lookup | 5 | 50 | 0 | 139.062 | 141.724 | 163.228 | 153.035 - 170.674 | 170.674 |
| fabric | q1_recall_number_lookup | 5 | 50 | 0 | 69.458 | 69.397 | 74.331 | 70.555 - 80.408 | 80.408 |
| fabric | q2_entity_uuid_lookup | 5 | 50 | 0 | 69.488 | 69.024 | 74.246 | 71.910 - 77.817 | 77.817 |
| fabric | q3_activity_uuid_lookup | 5 | 50 | 0 | 69.832 | 69.750 | 74.698 | 71.961 - 79.830 | 79.830 |
| fabric | q4_recalling_firm_lookup | 5 | 50 | 0 | 73.103 | 73.572 | 76.329 | 74.737 - 78.815 | 78.815 |
| neo4j | q1_recall_number_lookup | 5 | 50 | 0 | 16.636 | 15.763 | 29.650 | 18.152 - 38.102 | 38.102 |
| neo4j | q2_entity_uuid_lookup | 5 | 50 | 0 | 16.035 | 15.489 | 26.887 | 18.232 - 32.134 | 32.134 |
| neo4j | q3_activity_uuid_lookup | 5 | 50 | 0 | 15.198 | 14.796 | 27.053 | 24.985 - 28.909 | 28.909 |
| neo4j | q4_recalling_firm_lookup | 5 | 50 | 0 | 14.562 | 13.740 | 25.532 | 20.998 - 31.211 | 31.211 |
| sba | q1_recall_number_lookup | 5 | 50 | 0 | 1.691 | 1.372 | 2.013 | 1.511 - 2.598 | 2.598 |
| sba | q2_entity_uuid_lookup | 5 | 50 | 0 | 1.019 | 0.795 | 1.103 | 0.856 - 1.583 | 1.583 |
| sba | q3_activity_uuid_lookup | 5 | 50 | 0 | 1.200 | 0.911 | 2.741 | 0.937 - 8.573 | 8.573 |
| sba | q4_recalling_firm_lookup | 5 | 50 | 0 | 1.904 | 1.721 | 2.164 | 1.526 - 2.758 | 2.758 |

## Ingest Aggregate

| system | runs | mean ingest(ms) | median ingest(ms) | ingest min-max(ms) |
|---|---:|---:|---:|---:|
| ethereum | 5 | 50684.212 | 50686.967 | 50677.844 - 50690.025 |
| fabric | 5 | 14269.221 | 16367.159 | 10770.632 - 16720.916 |
| neo4j | 5 | 1116.176 | 1118.675 | 1071.607 - 1175.717 |
| sba | 5 | 387.779 | 398.188 | 333.518 - 417.210 |

## Interpretation Notes

- SBA uses the Datomic-backed PROV-O schema through the Clojure openFDA harness.
- Neo4j uses indexed exact lookups through HTTP transactional Cypher.
- Fabric uses peer CLI against openFDA chaincode with composite-key exact lookups.
- Ethereum stores records as transaction input and answers semantic queries by scanning block transactions; this is real execution but not an indexed smart-contract design.
