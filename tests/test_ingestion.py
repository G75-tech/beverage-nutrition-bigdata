"""Critical safety and replay tests, all using isolated temporary fixtures."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from src.ingest_file import ingest_batch, quality_gate


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "input.csv"
        self.original = b'"id","amount"\r\n"1","2.5"\r\n'
        self.source.write_bytes(self.original)
        self.schema = {"columns": ["id", "amount"], "types": {"id": "long", "amount": "double"}, "required_values": ["id"]}
        self.config = {"source": "test", "release": "2026-04-30", "dataset": "fixture", "raw_dir": "data/raw",
                       "manifest": "metadata/manifests/ingestion.jsonl", "type_sample_rows": 1000, "csv_schemas": {"input.csv": self.schema}}

    def run_batch(self, run_id):
        return ingest_batch([self.source], self.config, self.root, run_id)

    def test_three_runs_copy_then_skip_and_preserve_bytes(self):
        first = self.run_batch("first")["events"][0]
        second = self.run_batch("second")["events"][0]
        third = self.run_batch("third")["events"][0]
        self.assertEqual([first["status"], second["status"], third["status"]], ["INGESTED", "SKIPPED_DUPLICATE", "SKIPPED_DUPLICATE"])
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual((self.root / first["destination"]).read_bytes(), self.original)
        self.assertEqual(len(list((self.root / "data/raw").rglob("*.csv"))), 1)
        journal = self.root / self.config["manifest"]
        self.assertEqual(len(journal.read_text().splitlines()), 3)
        for event in (first, second, third):
            self.assertTrue(event["finished_at_utc"].endswith("Z"))
            self.assertEqual(len(event["sha256"]), 64)

    def test_changed_checksum_creates_new_copy(self):
        self.run_batch("first")
        self.source.write_bytes(b'id,amount\n2,3\n')
        event = self.run_batch("changed")["events"][0]
        self.assertEqual(event["status"], "INGESTED")
        self.assertEqual(len(list((self.root / "data/raw").rglob("*.csv"))), 2)

    def test_corrupt_raw_fails_without_overwrite(self):
        first = self.run_batch("first")["events"][0]
        destination = self.root / first["destination"]
        destination.write_bytes(b"corrupt")
        result = self.run_batch("second")
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(destination.read_bytes(), b"corrupt")

    def test_missing_source_failure_is_recorded(self):
        self.source.unlink()
        result = self.run_batch("missing")
        self.assertEqual(result["events"][0]["status"], "FAILED")
        self.assertIn("Missing", result["events"][0]["error"])

    def test_gate_rejects_invalid_inputs(self):
        cases = [b"", b"id,amount\n", b"id,id\n1,2\n", b"wrong,amount\n1,2\n", b"id,amount\n1\n",
                 b"id,amount\nwrong,2\n", b"id,amount\n,2\n", b"id,amount\n1,nan\n", b'id,amount\n"unterminated,2\n']
        for payload in cases:
            with self.subTest(payload=payload):
                self.source.write_bytes(payload)
                with self.assertRaises((ValueError, __import__("csv").Error)):
                    quality_gate(self.source, self.schema, 1000)

    def test_extension_gate(self):
        other = self.root / "input.txt"
        other.write_bytes(self.original)
        with self.assertRaisesRegex(ValueError, "Only CSV"):
            quality_gate(other, self.schema, 1000)

    def test_sample_scope_is_explicit(self):
        self.source.write_bytes(b"id,amount\n1,2\ninvalid,3\n")
        gate = quality_gate(self.source, self.schema, 1)
        self.assertEqual(gate["sample_records_checked"], 1)
        with self.assertRaises(ValueError):
            quality_gate(self.source, self.schema, 2)

    def test_writer_lock_fails_closed(self):
        lock = self.root / "metadata/manifests/ingestion.lock"
        lock.parent.mkdir(parents=True)
        lock.write_text("another writer")
        with self.assertRaises(FileExistsError):
            self.run_batch("locked")

    def test_existing_destination_never_overwritten(self):
        first = self.run_batch("fixed")["events"][0]
        (self.root / self.config["manifest"]).write_text("")
        result = self.run_batch("fixed")
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual((self.root / first["destination"]).read_bytes(), self.original)

    def test_damaged_manifest_fails_closed(self):
        journal = self.root / self.config["manifest"]
        journal.parent.mkdir(parents=True)
        journal.write_text("broken json")
        with self.assertRaises(json.JSONDecodeError):
            self.run_batch("damaged")
        self.assertFalse(journal.with_suffix(".lock").exists())

    def test_manifest_path_escape_rejected(self):
        first = self.run_batch("first")["events"][0]
        first["destination"] = "input.csv"
        journal = self.root / self.config["manifest"]
        journal.write_text(json.dumps(first) + "\n")
        event = self.run_batch("escape")["events"][0]
        self.assertEqual(event["status"], "FAILED")
        self.assertIn("escapes", event["error"])

    def test_invalid_run_id_rejected(self):
        with self.assertRaises(ValueError):
            self.run_batch("../outside")

    def test_batch_stops_after_failed_source(self):
        missing = self.root / "missing.csv"
        config = copy.deepcopy(self.config)
        config["csv_schemas"]["missing.csv"] = self.schema
        result = ingest_batch([missing, self.source], config, self.root, "failfast")
        self.assertEqual(len(result["events"]), 1)
        self.assertEqual(result["status"], "FAIL")
        self.assertFalse((self.root / "data/raw").exists())


if __name__ == "__main__":
    unittest.main()
