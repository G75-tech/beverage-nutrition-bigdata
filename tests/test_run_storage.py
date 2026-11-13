"""Regression checks for run history, failure handling and task handoffs."""
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from src.common import load_config, sha256, write_json
from src.checkpoint3_task import prepare_config, run_one
from src.run_storage import create_run, publish_run, run_paths


class RunStorageTests(unittest.TestCase):
    def test_failed_attempt_keeps_last_success_and_original_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, success, artifacts = create_run("task2", root, "first")
            self.assertTrue(artifacts.is_dir())
            result = success / "filter_result.json"
            write_json(result, {"status": "PASS", "snapshot_run_id": "raw"})
            original_hash = sha256(result)
            (success / "filter_report.md").write_text("PASS", encoding="utf-8")
            publish_run(result, root)
            _, failure, _ = create_run("task2", root, "second")
            write_json(failure / "filter_result.json", {"status": "FAIL"})
            publish_run(failure / "filter_result.json", root)
            task_root = success.parent.parent
            self.assertEqual(load_config(task_root / "latest.json")["run_id"], "first")
            self.assertEqual(load_config(task_root / "last_attempt.json")["status"], "FAIL")
            self.assertEqual(sha256(result), original_hash)
            index = (root / "docs/evidence/checkpoint3/INDEX.md").read_text(encoding="utf-8")
            self.assertIn("task02_beverages/runs/first/filter_report.md", index)
            self.assertIn("artifacts/checkpoint3/task02_beverages/runs/first", index)
            with self.assertRaises(ValueError):
                create_run("task2", root, "first")

    def test_input_contract_does_not_create_empty_data_folder(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, folder, artifacts = create_run("task1", Path(temporary), "input")
            self.assertTrue(folder.is_dir())
            self.assertFalse(artifacts.exists())
            with self.assertRaises(ValueError):
                run_paths("task1", "../escape", Path(temporary))

    def fixture(self, root):
        base = {"snapshot_run_id": "raw", "task2": {"scope_policy_path": "scope.json"}}
        _, folder, _ = create_run("task1", root, "prior")
        result = folder / "input_contract.json"
        write_json(result, {"status": "PASS", "snapshot_run_id": "raw"})
        entry = {"result_path": result.relative_to(root).as_posix(), "result_sha256": sha256(result)}
        state = {"base_config": base, "completed": {"task1": entry, "task2": {"old": True}, "task5": {"old": True}}}
        return base, state, result

    def test_rerun_uses_verified_upstream_and_invalidates_downstream(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            base, state, path = self.fixture(root)
            config, kept = prepare_config(base, state, 2, root=root)
            self.assertEqual(config["task2"]["input_contract_path"], path.relative_to(root).as_posix())
            self.assertEqual(set(kept), {"task1"})
            write_json(path, {"status": "FAIL", "snapshot_run_id": "raw"})
            with self.assertRaisesRegex(ValueError, "thay đổi"):
                prepare_config(base, state, 2, root=root)

    def test_missing_upstream_and_changed_config_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Task 1"):
            prepare_config({"snapshot_run_id": "raw"}, {}, 2)
        base = {"snapshot_run_id": "raw"}
        with self.assertRaisesRegex(ValueError, "task1"):
            prepare_config(base, {"base_config": base, "completed": {}}, 3)

    def test_failed_rerun_cannot_fall_back_to_old_downstream_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            base, state, _ = self.fixture(root)
            write_json(root / "config.json", base)
            state_path = root / "docs/evidence/checkpoint3/state/manual_state.json"
            write_json(state_path, state)
            args = SimpleNamespace(task=2, repeat=False, snapshot_run_id=None, task_config=Path("config.json"))
            with patch("src.checkpoint3_task.run_stage", side_effect=RuntimeError("fake failure")):
                with self.assertRaisesRegex(RuntimeError, "fake failure"):
                    run_one(args, root)
            saved = load_config(state_path)
            self.assertEqual(set(saved["completed"]), {"task1"})
            with self.assertRaisesRegex(ValueError, "task2"):
                prepare_config(base, saved, 3, root=root)


if __name__ == "__main__":
    unittest.main()
