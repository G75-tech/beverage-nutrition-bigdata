"""Run one Checkpoint 3 task with automatic, verified handoffs."""
from __future__ import annotations

import argparse
from pathlib import Path
import os
import subprocess
import sys

from src.common import ROOT, load_config, sha256, utc_now, write_json
from src.checkpoint3_pipeline import STAGES, pipeline_lock, run_stage, wire_next, working_config
from src.run_storage import TASKS, create_run, fresh_id, refresh_index


def prepare_config(base, state, number, repeat=False, root=ROOT):
    """Rebuild from verified upstream runs, never from a stale config pin."""
    if number != 1 and state.get("base_config") != base:
        raise ValueError("Cấu hình đã đổi hoặc chưa chạy Task 1. Hãy bắt đầu lại từ -Task 1.")
    config = working_config(base)
    completed = state.get("completed", {})
    kept = {}
    for index in range(1, number):
        name = f"task{index}"
        if name not in completed:
            raise ValueError(f"Chưa có {name} PASS trong chuỗi hiện tại. Hãy chạy -Task {index} trước.")
        entry = completed[name]
        path = (Path(root) / entry["result_path"]).resolve()
        if not path.is_relative_to(Path(root).resolve() / "docs/evidence/checkpoint3"):
            raise ValueError("Upstream result is outside Checkpoint 3 evidence")
        if sha256(path) != entry["result_sha256"]:
            raise ValueError(f"Kết quả {name} đã thay đổi; hãy chạy lại từ -Task {index}.")
        result = load_config(path)
        if result["status"] != "PASS" or result["snapshot_run_id"] != config["snapshot_run_id"]:
            raise ValueError("Upstream result failed or uses another snapshot")
        wire_next(config, name, path, result, root)
        kept[name] = entry
    if repeat:
        entry = completed.get("task5")
        if not entry:
            raise ValueError("Hãy chạy -Task 5 trước khi dùng -Repeat.")
        path = Path(root) / entry["result_path"]
        if sha256(path) != entry["result_sha256"]:
            raise ValueError("KPI tham chiếu đã thay đổi; hãy chạy lại -Task 5.")
        config["task5"]["repeatability_reference_path"] = entry["result_path"]
        kept["task5"] = entry
    return config, kept


def run_one(args, root=ROOT):
    root = Path(root)
    state_path = root / "docs/evidence/checkpoint3/state/manual_state.json"
    base = load_config(root / args.task_config)
    if args.snapshot_run_id:
        base["snapshot_run_id"] = args.snapshot_run_id
    state = load_config(state_path) if state_path.exists() else {}
    config, kept = prepare_config(base, state, args.task, args.repeat, root)
    run_id, folder, _ = create_run(f"task{args.task}", root, fresh_id("manual"))
    config_path = folder / "run_config.json"
    write_json(config_path, config)
    # Remove downstream handoffs before trying a new upstream run. Historical
    # files stay on disk, but cannot silently feed the next task after a failure.
    state = {"base_config": base, "completed": kept, "updated_at_utc": utc_now()}
    write_json(state_path, state)
    print(f"[CHẠY] Task {args.task}{' — đối chiếu KPI lần hai' if args.repeat else ''}", flush=True)
    print(f"[LOG] {folder / 'console.log'}", flush=True)
    stage = STAGES[args.task - 1]
    try:
        path, result, details = run_stage(stage, config_path, folder, folder, run_id,
                                          config["snapshot_run_id"], root=root)
    except Exception as exc:
        write_json(folder / "runner_error.json", {"status": "FAIL", "error": str(exc)})
        raise
    name = f"task{args.task}"
    state["completed"][name] = {"result_path": path.relative_to(root).as_posix(), "result_sha256": sha256(path)}
    state["updated_at_utc"] = utc_now()
    write_json(state_path, state)
    print(f"[PASS] {details['wall_seconds']:.1f} giây", flush=True)
    print(f"[BÁO CÁO] {path.with_name(TASKS[name][2])}", flush=True)
    print(f"[KẾT QUẢ MỚI NHẤT] {path.parent.parent.parent / 'latest.json'}", flush=True)
    return 0


def run_pipeline(args):
    run_id, folder, _ = create_run("task6", run_id=fresh_id("pipeline"))
    command = [sys.executable, "-B", "-u", "-m", "src.checkpoint3_pipeline", "--task-config", str(args.task_config)]
    if args.snapshot_run_id:
        command.extend(["--snapshot-run-id", args.snapshot_run_id])
    env = dict(os.environ, CHECKPOINT3_STAGE_RUN_ID=run_id, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    with (folder / "console.log").open("w", encoding="utf-8") as log:
        with subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, encoding="utf-8", errors="replace") as process:
            for line in process.stdout:
                log.write(line)
                log.flush()
                print(line, end="", flush=True)
            return process.wait()


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=int, choices=range(1, 7), default=6)
    parser.add_argument("--repeat", action="store_true", help="Repeat Task 5 on exactly the same pinned input")
    parser.add_argument("--task-config", type=Path, default=ROOT / "config/checkpoint3.json")
    parser.add_argument("--snapshot-run-id")
    parser.add_argument("--list", action="store_true", help="List the last attempt and last success of every task")
    args = parser.parse_args(argv)
    if args.repeat and args.task != 5:
        parser.error("--repeat chỉ dùng cùng --task 5")
    try:
        if args.list:
            refresh_index()
            for _, (name, _, _) in TASKS.items():
                folder = ROOT / "docs/evidence/checkpoint3" / name
                attempt = load_config(folder / "last_attempt.json") if (folder / "last_attempt.json").exists() else {}
                latest = load_config(folder / "latest.json") if (folder / "latest.json").exists() else {}
                print(f"{name}: lần thử={attempt.get('status', 'CHƯA CHẠY')}; PASS={latest.get('run_id', 'CHƯA CÓ')}")
            return 0
        if args.task == 6:
            return run_pipeline(args)
        with pipeline_lock(ROOT / "docs/evidence/checkpoint3/state/execution.lock"):
            return run_one(args)
    except Exception as exc:
        print(f"[LỖI] {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
