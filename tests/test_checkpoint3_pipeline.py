"""Checks for fresh handoffs, failure stopping, receipt integrity and Spark evidence."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from src.common import load_config, record_spark_baseline, sha256, start_spark, write_json
from src.checkpoint3_pipeline import (
    execute_chain, pipeline_lock, publish_latest, run_stage, validate_receipt, wire_next, working_config,
)


class PipelineControlTests(unittest.TestCase):
    def test_working_config_keeps_policy_but_drops_old_artifact_pins(self):
        original = {"snapshot_run_id": "old", "tables": {"food": {"key": "fdc_id"}},
                    "task2": {"scope_policy_path": "scope.json", "input_contract_path": "stale"},
                    "task3": {"nutrients": [{"id": 2000}], "result_path": "oldresult", "input_handoff_path": "stale"},
                    "task4": {"input_result_sha256": "oldhash"},
                    "task5": {"aggregation_policy": {"round": 6}, "repeatability_reference_path": "stale"},
                    "task6": {"input_handoff_path": "stale"}}
        current = working_config(original, "new")
        self.assertEqual(current["snapshot_run_id"], "new")
        self.assertEqual(original["snapshot_run_id"], "old")
        self.assertEqual(current["task3"]["nutrients"], [{"id": 2000}])
        self.assertEqual(current["task5"]["aggregation_policy"], {"round": 6})
        self.assertNotIn("input_contract_path", current["task2"])
        self.assertNotIn("repeatability_reference_path", current["task5"])
        self.assertEqual(current["task6"], {})

    def receipt_fixture(self, root, status="PASS", snapshot="snapshot"):
        folder = root / "docs/evidence/checkpoint3/stage"
        folder.mkdir(parents=True, exist_ok=True)
        result_path = folder / "input_contract.json"
        write_json(result_path, {"status": status, "snapshot_run_id": snapshot, "elapsed_seconds": 1})
        pointer = root / "receipt.json"
        write_json(pointer, {"status": status, "pipeline_run_id": "pipeline", "result_path": str(result_path),
                             "result_sha256": sha256(result_path)})
        return pointer, result_path

    def test_rejects_changed_receipt_result_and_mixed_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pointer, path = self.receipt_fixture(root)
            validate_receipt(pointer, "snapshot", "pipeline", "input_contract.json", root)
            write_json(path, {"status": "PASS", "snapshot_run_id": "changed"})
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                validate_receipt(pointer, "snapshot", "pipeline", "input_contract.json", root)
            pointer, _ = self.receipt_fixture(root, snapshot="other")
            with self.assertRaisesRegex(ValueError, "another raw snapshot"):
                validate_receipt(pointer, "snapshot", "pipeline", "input_contract.json", root)

    def test_exit_zero_with_failed_evidence_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pointer, _ = self.receipt_fixture(root, status="FAIL")
            with self.assertRaisesRegex(ValueError, "not PASS"):
                validate_receipt(pointer, "snapshot", "pipeline", "input_contract.json", root)

    def test_stage_receipt_ownership_and_nonzero_exit_are_enforced(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pointer, _ = self.receipt_fixture(root)
            with self.assertRaisesRegex(ValueError, "ownership"):
                validate_receipt(pointer, "snapshot", "other-pipeline", "input_contract.json", root)
            folder = root / "run"
            logs = root / "logs"
            folder.mkdir()
            logs.mkdir()
            write_json(root / "config.json", {})
            with self.assertRaisesRegex(RuntimeError, "exited 2"):
                run_stage(("task1", "unused", "input_contract.json"), root / "config.json", folder, logs,
                          "pipeline", "snapshot", executor=lambda *args, **kwargs: SimpleNamespace(returncode=2), root=root)

    def test_failed_stage_stops_chain_and_preserves_latest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            latest = root / "docs/evidence/checkpoint3/latest_pipeline.json"
            write_json(latest, {"accepted": "previous"})
            config = working_config({"snapshot_run_id": "snapshot"})
            config_path = root / "run_config.json"
            write_json(config_path, config)
            folder = root / "run"
            folder.mkdir()
            calls = []
            def fake(stage, *args, **kwargs):
                calls.append(stage[0])
                if stage[0] == "task2":
                    raise RuntimeError("forced stage failure")
                path = root / "docs/evidence/checkpoint3/new/input_contract.json"
                result = {"status": "PASS", "snapshot_run_id": "snapshot"}
                write_json(path, result)
                return path, result, {"name": "task1", "wall_seconds": 1}
            evidence = {"stages": []}
            with self.assertRaisesRegex(RuntimeError, "forced stage failure"):
                execute_chain(config, config_path, folder, root, "pipeline", evidence, stage_runner=fake, root=root)
            self.assertEqual(calls, ["task1", "task2"])
            self.assertIn("new/input_contract.json", config["task2"]["input_contract_path"])
            self.assertEqual(load_config(latest), {"accepted": "previous"})
            with self.assertRaises(ValueError):
                publish_latest(folder, {"status": "FAIL"}, root)
            self.assertEqual(load_config(latest), {"accepted": "previous"})

    def test_next_stage_receives_hash_of_new_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_path = root / "nutrient_result.json"
            write_json(result_path, {"status": "PASS"})
            config = working_config({"snapshot_run_id": "snapshot"})
            wire_next(config, "task3", result_path, {}, root)
            self.assertEqual(config["task4"]["input_result_sha256"], sha256(result_path))
            self.assertEqual(config["task3"]["result_path"], "nutrient_result.json")

    def test_overlapping_pipeline_lock_is_rejected_and_released(self):
        with tempfile.TemporaryDirectory() as temporary:
            lock = Path(temporary) / "pipeline.lock"
            with pipeline_lock(lock):
                with self.assertRaisesRegex(RuntimeError, "Another"):
                    with pipeline_lock(lock):
                        self.fail("An overlapping lock was acquired")
            with pipeline_lock(lock):
                pass


class SparkBaselineTests(unittest.TestCase):
    def test_records_formatted_plan_and_actual_runtime_settings(self):
        spark = start_spark("checkpoint3-baseline-tests")
        spark.sparkContext.setLogLevel("ERROR")
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                frame = spark.range(20).selectExpr("id % 2 as group_id", "id").groupBy("group_id").sum("id")
                info = record_spark_baseline(spark, frame, root, "aggregation", root)
                plan = root / info["explain_formatted_path"]
                self.assertGreaterEqual(info["partitions_observed"], 1)
                self.assertEqual(info["explain_sha256"], sha256(plan))
                self.assertIn("Physical Plan", plan.read_text(encoding="utf-8"))
                baseline = load_config(root / "spark_baseline.json")
                self.assertEqual(baseline["spark"], spark.version)
                self.assertEqual(baseline["master"], "local[*]")
                self.assertIn("spark.sql.adaptive.enabled", baseline["sql_configuration"])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
