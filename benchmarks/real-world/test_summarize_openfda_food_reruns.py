#!/usr/bin/env python3
"""Tests for openFDA rerun summary generation."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "current" / "openfda-food" / "summarize_openfda_food_reruns.py"
SPEC = importlib.util.spec_from_file_location("summarize_openfda_food_reruns", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
summarizer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = summarizer
SPEC.loader.exec_module(summarizer)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_run(
    results_dir: Path,
    run_id: str,
    *,
    warmup: int = 30,
    reps: int = 100,
    dirty: bool = False,
    total_errors: int = 0,
    usable_records: int = 1000,
    storage: str = "mem",
) -> None:
    manifest = "\n".join(
        [
            f"run_id={run_id}",
            "source_endpoint=https://api.fda.gov/food/enforcement.json",
            "source_last_updated=2026-04-22",
            "source_total_available=28774",
            f"usable_records={usable_records}",
            f"warmup={warmup}",
            f"reps={reps}",
            f"datomic_storage={storage}",
            "ingest_ms=1234.500",
            "git_commit=test-commit",
            f"git_dirty={'true' if dirty else 'false'}",
            f"total_errors={total_errors}",
            "",
        ]
    )
    summary = "\n".join(
        [
            "query,ok_count,error_count,mean_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms",
            f"q1_recall_number_lookup,{reps},0,1.0,1.0,1.2,1.3,0.9,1.4",
            f"q2_entity_uuid_lookup,{reps},0,2.0,2.0,2.2,2.3,1.9,2.4",
            f"q3_activity_uuid_lookup,{reps},0,3.0,3.0,3.2,3.3,2.9,3.4",
            f"q4_recalling_firm_lookup,{reps},0,4.0,4.0,4.2,4.3,3.9,4.4",
            "",
        ]
    )
    _write(results_dir / run_id / "manifest.txt", manifest)
    _write(results_dir / run_id / "openfda_food_latency_summary.csv", summary)


class OpenfdaRerunSummaryTest(unittest.TestCase):
    def test_generates_report_for_clean_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_ids = ["openfda_food_realworld_001", "openfda_food_realworld_002"]
            for run_id in run_ids:
                _write_run(results_dir, run_id)

            runs = [summarizer.load_run(results_dir, run_id) for run_id in run_ids]
            errors = [
                error
                for run in runs
                for error in summarizer.validate_run(run, 30, 100, "mem", True, 1)
            ]
            report = summarizer.markdown_report(runs, run_ids[-1], 30, 100, "mem", True, 1)

            self.assertEqual([], errors)
            self.assertIn("openFDA Real-World Benchmark Rerun Summary", report)
            self.assertIn("Ingest time range: 1234.500--1234.500 ms", report)
            self.assertIn("not a complete farm-to-retail provenance-chain benchmark", report)

    def test_rejects_dirty_or_wrong_warmup_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_id = "openfda_food_realworld_001"
            _write_run(results_dir, run_id, warmup=10, dirty=True)

            run = summarizer.load_run(results_dir, run_id)
            errors = summarizer.validate_run(run, 30, 100, "mem", True, 1)

            self.assertTrue(any("warmup" in error for error in errors))
            self.assertTrue(any("git_dirty" in error for error in errors))

    def test_rejects_summary_query_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_id = "openfda_food_realworld_001"
            _write_run(results_dir, run_id)
            summary_path = results_dir / run_id / "openfda_food_latency_summary.csv"
            summary_path.write_text(
                summary_path.read_text(encoding="utf-8").replace(
                    "q2_entity_uuid_lookup,100,0,2.0",
                    "q2_entity_uuid_lookup,99,1,2.0",
                ),
                encoding="utf-8",
            )

            run = summarizer.load_run(results_dir, run_id)
            errors = summarizer.validate_run(run, 30, 100, "mem", True, 1)

            self.assertTrue(any("q2_entity_uuid_lookup error_count" in error for error in errors))

    def test_rejects_too_few_usable_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_id = "openfda_food_realworld_001"
            _write_run(results_dir, run_id, usable_records=10)

            run = summarizer.load_run(results_dir, run_id)
            errors = summarizer.validate_run(run, 30, 100, "mem", True, 100)

            self.assertTrue(any("usable_records" in error for error in errors))

    def test_rejects_wrong_storage_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir)
            run_id = "openfda_food_realworld_001"
            _write_run(results_dir, run_id, storage="mem")

            run = summarizer.load_run(results_dir, run_id)
            errors = summarizer.validate_run(run, 30, 100, "dev-transactor", True, 1)

            self.assertTrue(any("datomic_storage" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
