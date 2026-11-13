"""Snapshot selection must fail closed instead of mixing releases or raw copies."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from src.common import sha256, start_spark
from src.input_contract import lock_snapshot, measure_input, read_locked_csv


class InputContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        raw = self.root / "data/raw/food.csv"
        raw.parent.mkdir(parents=True)
        raw.write_text("fdc_id\n1\n", encoding="utf-8")
        self.config = {"source": "test", "release": "one", "dataset": "foods", "raw_dir": "data/raw",
                       "manifest": "manifest.jsonl", "default_files": ["food.csv"],
                       "csv_schemas": {"food.csv": {"columns": ["fdc_id"], "types": {"fdc_id": "long"},
                                                     "required_values": ["fdc_id"]}}}
        self.task = {"snapshot_run_id": "pinned", "tables": {"food.csv": {
            "grain": "one food per fdc_id", "primary_key": ["fdc_id"], "join_keys": ["fdc_id"]}}}
        self.event = {"source": "test", "release": "one", "dataset": "foods", "run_id": "pinned",
                      "status": "INGESTED", "file_name": "food.csv", "destination": "data/raw/food.csv",
                      "size_bytes": raw.stat().st_size, "sha256": sha256(raw)}

    def select(self, events):
        (self.root / "manifest.jsonl").write_text(
            "\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
        return lock_snapshot(self.config, self.task, root=self.root)

    def test_pins_original_run_even_when_another_release_or_run_is_newer(self):
        other_release = dict(self.event, release="two")
        other_run = dict(self.event, run_id="newer")
        result = self.select([self.event, other_release, other_run])
        self.assertEqual(result["snapshot_run_id"], "pinned")
        self.assertEqual(len(result["tables"]), 1)

    def test_skip_event_cannot_stand_in_for_missing_ingested_copy(self):
        with self.assertRaisesRegex(ValueError, "got 0"):
            self.select([dict(self.event, status="SKIPPED_DUPLICATE")])

    def test_ambiguous_success_events_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "got 2"):
            self.select([self.event, copy.deepcopy(self.event)])

    def test_same_size_corruption_is_rejected(self):
        (self.root / self.event["destination"]).write_text("fdc_id\n2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.select([self.event])

    def test_path_outside_raw_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid raw path"):
            self.select([dict(self.event, destination="../food.csv")])

    def test_incomplete_multi_table_snapshot_is_rejected(self):
        self.config["default_files"].append("missing.csv")
        self.task["tables"]["missing.csv"] = self.task["tables"]["food.csv"]
        with self.assertRaisesRegex(ValueError, "missing.csv, got 0"):
            self.select([self.event])


class SparkInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("beverage-input-contract-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def read_fixture(self, root):
        table = {"raw_path": "food.csv", "schema_declaration": {
            "columns": ["fdc_id", "amount", "description"],
            "types": {"fdc_id": "long", "amount": "double"}}}
        return read_locked_csv(self.spark, table, root)

    def test_full_count_preserves_quoted_newline_and_measures_null_key(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "food.csv").write_text('fdc_id,amount,description\n1,2.5,"line one\nline two"\n,0,other\n',
                                           encoding="utf-8")
            result = measure_input(self.read_fixture(root), ["fdc_id"])
            self.assertEqual(result, {"row_count": 2, "null_key_counts": {"fdc_id": 1},
                                      "null_counts": {"fdc_id": 1, "amount": 0, "description": 0}})

    def test_count_still_rejects_invalid_number_in_non_key_column(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "food.csv").write_text('fdc_id,amount,description\n1,invalid,other\n', encoding="utf-8")
            with self.assertRaises(Exception) as error:
                measure_input(self.read_fixture(root), ["fdc_id"])
            self.assertIn("MALFORMED_RECORD", str(error.exception))


if __name__ == "__main__":
    unittest.main()
