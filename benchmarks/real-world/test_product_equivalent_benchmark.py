#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "current" / "openfda-food" / "product_equivalent_benchmark.py"
SPEC = importlib.util.spec_from_file_location("product_equivalent_benchmark", MODULE_PATH)
assert SPEC and SPEC.loader
bench = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bench
SPEC.loader.exec_module(bench)


class ProductEquivalentBenchmarkTest(unittest.TestCase):
    def test_java_name_uuid_from_bytes_matches_known_value(self) -> None:
        self.assertEqual(
            "87996074-8112-3534-95f7-729dfdc28a47",
            bench.java_name_uuid_from_bytes("openfda-food/entity/F-0001-2024"),
        )

    def test_normalize_record_uses_source_identifiers(self) -> None:
        record = bench.normalize_record(
            {
                "recall_number": "F-0001-2024",
                "event_id": "12345",
                "recalling_firm": "Example Foods Inc.",
                "product_description": "Example product",
                "city": "Bangkok",
                "state": "",
                "country": "Thailand",
                "distribution_pattern": "Nationwide",
            }
        )
        self.assertEqual("F-0001-2024", record.recall_number)
        self.assertEqual("12345", record.event_id)
        self.assertEqual("Bangkok, Thailand", record.location)
        self.assertEqual(
            bench.java_name_uuid_from_bytes("openfda-food/entity/F-0001-2024"),
            record.entity_id,
        )
        self.assertEqual(
            bench.java_name_uuid_from_bytes("openfda-food/activity/12345/F-0001-2024"),
            record.activity_id,
        )
        self.assertEqual(
            bench.java_name_uuid_from_bytes("openfda-food/agent/Example Foods Inc./Bangkok, Thailand"),
            record.agent_id,
        )

    def test_summary_reports_errors_per_query(self) -> None:
        rows = [
            bench.Measurement("neo4j", "q1_recall_number_lookup", 1, "ok", 1.0),
            bench.Measurement("neo4j", "q1_recall_number_lookup", 2, "error", 2.0),
            bench.Measurement("neo4j", "q2_entity_uuid_lookup", 1, "ok", 3.0),
        ]
        summary = {row["query"]: row for row in bench.summarize(rows, "neo4j")}
        self.assertEqual(1, summary["q1_recall_number_lookup"]["ok_count"])
        self.assertEqual(1, summary["q1_recall_number_lookup"]["error_count"])
        self.assertEqual(3.0, summary["q2_entity_uuid_lookup"]["p95_ms"])
        self.assertEqual(0, summary["q3_activity_uuid_lookup"]["ok_count"])


if __name__ == "__main__":
    unittest.main()
