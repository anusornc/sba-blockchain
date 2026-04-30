# Product-Equivalent openFDA Rerun Aggregate

- runs: `product_equivalent_openfda_sba_limit1000_20260430_001_001, product_equivalent_openfda_sba_limit1000_20260430_001_002, product_equivalent_openfda_sba_limit1000_20260430_001_003, product_equivalent_openfda_sba_limit1000_20260430_001_004, product_equivalent_openfda_sba_limit1000_20260430_001_005`
- source_raw_json_sha256: `06cc50661e0f19fa8f8d97109de3234609e41cb4380561d7d97a893c9d213907`
- limit: `1000`
- usable_records: `1000`
- warmup: `3`
- reps: `10`
- git_commit: `742bbda19af00dd00af43ace9c8c7d1596f938cb`
- git_dirty_values: `false`
- manifest_total_error_nonzero: `0`

## Query Latency Aggregate

| system | query | runs | ok | errors | mean(ms) | median p50(ms) | mean p95(ms) | p95 min-max(ms) | max observed(ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ethereum | q1_recall_number_lookup | 5 | 50 | 0 | 1749.262 | 1775.053 | 1921.305 | 1859.820 - 1974.026 | 1974.026 |
| ethereum | q2_entity_uuid_lookup | 5 | 50 | 0 | 1696.953 | 1682.019 | 1893.778 | 1829.179 - 1968.330 | 1968.330 |
| ethereum | q3_activity_uuid_lookup | 5 | 50 | 0 | 1684.017 | 1674.749 | 1946.597 | 1895.634 - 2023.976 | 2023.976 |
| ethereum | q4_recalling_firm_lookup | 5 | 50 | 0 | 1654.143 | 1693.126 | 1883.338 | 1835.265 - 1932.193 | 1932.193 |
| fabric | q1_recall_number_lookup | 5 | 50 | 0 | 70.640 | 70.409 | 77.443 | 73.020 - 88.326 | 88.326 |
| fabric | q2_entity_uuid_lookup | 5 | 50 | 0 | 70.064 | 69.450 | 75.013 | 72.961 - 76.790 | 76.790 |
| fabric | q3_activity_uuid_lookup | 5 | 50 | 0 | 72.429 | 71.807 | 81.275 | 72.469 - 99.656 | 99.656 |
| fabric | q4_recalling_firm_lookup | 5 | 50 | 0 | 73.885 | 73.593 | 78.665 | 75.613 - 84.328 | 84.328 |
| neo4j | q1_recall_number_lookup | 5 | 50 | 0 | 17.603 | 15.852 | 33.851 | 19.027 - 38.455 | 38.455 |
| neo4j | q2_entity_uuid_lookup | 5 | 50 | 0 | 14.317 | 14.181 | 23.268 | 16.344 - 28.800 | 28.800 |
| neo4j | q3_activity_uuid_lookup | 5 | 50 | 0 | 15.558 | 13.669 | 27.215 | 23.224 - 31.163 | 31.163 |
| neo4j | q4_recalling_firm_lookup | 5 | 50 | 0 | 15.543 | 14.135 | 29.948 | 25.209 - 38.272 | 38.272 |
| sba | q1_recall_number_lookup | 5 | 50 | 0 | 2.156 | 2.234 | 2.725 | 1.700 - 3.716 | 3.716 |
| sba | q2_entity_uuid_lookup | 5 | 50 | 0 | 1.273 | 1.201 | 1.443 | 1.127 - 1.640 | 1.640 |
| sba | q3_activity_uuid_lookup | 5 | 50 | 0 | 1.294 | 1.095 | 1.617 | 1.371 - 1.928 | 1.928 |
| sba | q4_recalling_firm_lookup | 5 | 50 | 0 | 2.110 | 1.944 | 2.516 | 1.950 - 3.626 | 3.626 |

## Ingest Aggregate

| system | runs | mean ingest(ms) | median ingest(ms) | ingest min-max(ms) |
|---|---:|---:|---:|---:|
| ethereum | 5 | 506794.664 | 506899.295 | 506350.488 - 506928.727 |
| fabric | 5 | 108809.353 | 108433.276 | 106330.289 - 111615.666 |
| neo4j | 5 | 2545.148 | 2581.635 | 2427.402 - 2651.233 |
| sba | 5 | 1952.121 | 1919.921 | 1838.872 - 2104.776 |

## Interpretation Notes

- SBA uses the Datomic-backed PROV-O schema through the Clojure openFDA harness.
- Neo4j uses indexed exact lookups through HTTP transactional Cypher.
- Fabric uses peer CLI against openFDA chaincode with composite-key exact lookups.
- Ethereum stores records as transaction input and answers semantic queries by scanning block transactions; this is real execution but not an indexed smart-contract design.
