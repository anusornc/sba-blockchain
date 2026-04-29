#!/usr/bin/env python3
"""Tests for openFDA scale summary generation."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).parent / "openfda-food" / "summarize_openfda_food_scale.py"
SPEC = importlib.util.spec_from_file_location("summarize_openfda_food_scale", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
summarizer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = summarizer
SPEC.loader.exec_module(summarizer)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_run(results_dir: Path, run_id: str, limit: int, *, dirty: bool = False) -> None:
    manifest = "\n".join(
        [
            f"run_id={run_id}",
            "source_endpoint=https://api.fda.gov/food/enforcement.json",
            "source_last_updated=2026-04-22",
            "source_total_available=28774",
            f"limit={limit}",
            f"fetched_records={limit}",
            f"usable_records={limit}",
            "warmup=30",
            "reps=100",
            "datomic_storage=dev-transactor",
            "ingest_ms=1234.500",
            "git_commit=test-commit",
            f"git_dirty={'true' if dirty else 'false'}",
            "total_errors=0",
            "",
        ]
    )
    summary = "\n".join(
        [
            "query,ok_count,error_count,mean_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms",
            "q1_recall_number_lookup,100,0,1.0,1.0,1.2,1.3,0.9,1.4",
            "q2_entity_uuid_lookup,100,0,2.0,2.0,2.2,2.3,1.9,2.4",
            "q3_activity_uuid_lookup,100,0,3.0,3.0,3.2,3.3,2.9,3.4",
            "q4_recalling_firm_lookup,100,0,4.0,4.0,4.2,4.3,3.9,4.4",
            "",
        ]
    )
    _write(results_dir / run_id / "manifest.txt", manifest)
    _write(results_dir / run_id / "openfda_food_latency_summary.csv", summary)


class OpenfdaScaleSummaryTest(unittest.TestCase):
    def test_generates_report_for_scale_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_ids = ["scale_5000_001", "scale_10000_001"]
            limits = [5000, 10000]
            for run_id, limit in zip(run_ids, limits):
                _write_run(results_dir, run_id, limit)

            runs = [summarizer.load_run(results_dir, run_id, limit) for run_id, limit in zip(run_ids, limits)]
            errors = [
                error
                for run in runs
                for error in summarizer.validate_run(run, 30, 100, "dev-transactor", True)
            ]
            report = summarizer.markdown_report(runs, 30, 100, "dev-transactor")

            self.assertEqual([], errors)
            self.assertIn("openFDA Disk-Backed Scale Benchmark Summary", report)
            self.assertIn("| 10000 | 10000 | 1234.500 |", report)

    def test_rejects_dirty_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_id = "scale_5000_001"
            _write_run(results_dir, run_id, 5000, dirty=True)

            run = summarizer.load_run(results_dir, run_id, 5000)
            errors = summarizer.validate_run(run, 30, 100, "dev-transactor", True)

            self.assertTrue(any("git_dirty" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
