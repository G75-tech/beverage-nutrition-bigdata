"""Checkpoint 3 output locations and human-readable run indexes."""
from __future__ import annotations

import os
from pathlib import Path
import re
import uuid

from src.common import ROOT, load_config, sha256, utc_now, write_json

TASKS = {
    "task1": ("task01_input", "input_contract.json", "input_contract.md"),
    "task2": ("task02_beverages", "filter_result.json", "filter_report.md"),
    "task3": ("task03_nutrients", "nutrient_result.json", "nutrient_report.md"),
    "task4": ("task04_join", "join_result.json", "join_report.md"),
    "task5": ("task05_kpis", "kpi_result.json", "kpi_report.md"),
    "task6": ("task06_pipeline", "pipeline_result.json", "baseline.md"),
}


def fresh_id(prefix="run"):
    stamp = utc_now().replace("-", "").replace(":", "").replace(".", "")
    return f"{prefix}_{stamp}_{uuid.uuid4().hex[:6]}"


def run_paths(task, run_id, root=ROOT):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID")
    name = TASKS[task][0]
    return (Path(root) / "docs/evidence/checkpoint3" / name / "runs" / run_id,
            Path(root) / "artifacts/checkpoint3" / name / "runs" / run_id)


def create_run(task, root=ROOT, run_id=None):
    run_id = run_id or os.environ.get("CHECKPOINT3_STAGE_RUN_ID") or fresh_id()
    folder, artifacts = run_paths(task, run_id, root)
    # The runner may already have opened console.log in this directory.
    if (folder / TASKS[task][1]).exists():
        raise ValueError("This run already has a result; choose a fresh run ID")
    folder.mkdir(parents=True, exist_ok=True)
    if task not in ("task1", "task6"):
        artifacts.mkdir(parents=True, exist_ok=True)
    return run_id, folder, artifacts


def publish_run(result_path, root=ROOT):
    """Keep last attempt separate from last success; never modify result bytes."""
    root = Path(root).resolve()
    path = Path(result_path).resolve()
    for task, (name, filename, report) in TASKS.items():
        task_root = root / "docs/evidence/checkpoint3" / name
        if path.name == filename and path.parent.parent == task_root / "runs":
            break
    else:
        return  # Legacy result paths are still accepted by receipt readers.
    result = load_config(path)
    folder = path.parent
    _, artifacts = run_paths(task, folder.name, root)
    pointer = {
        "task": task, "run_id": folder.name, "status": result["status"],
        "snapshot_run_id": result.get("snapshot_run_id"),
        "result_path": path.relative_to(root).as_posix(), "result_sha256": sha256(path),
        "report_path": (folder / report).relative_to(root).as_posix(),
        "evidence_dir": folder.relative_to(root).as_posix(),
        "artifacts_dir": artifacts.relative_to(root).as_posix() if artifacts.exists() else None,
        "console_log_path": (folder / "console.log").relative_to(root).as_posix()
                            if (folder / "console.log").exists() else None,
        "finished_at_utc": result.get("finished_at_utc"),
    }
    write_json(task_root / "last_attempt.json", pointer)
    if result["status"] == "PASS":
        write_json(task_root / "latest.json", pointer)
    refresh_index(root)


def refresh_index(root=ROOT):
    root = Path(root)
    base = root / "docs/evidence/checkpoint3"
    lines = ["# Kết quả Checkpoint 3", "",
             "`latest.json`: lần PASS gần nhất; `last_attempt.json`: lần thử gần nhất, kể cả FAIL.",
             "Các đường dẫn trong JSON tính từ thư mục mã nguồn. Mỗi run được giữ riêng.", "",
             "| Task | Lần thử gần nhất | Báo cáo PASS gần nhất | Dữ liệu PASS gần nhất |",
             "| --- | --- | --- | --- |"]
    for task, (name, _, _) in TASKS.items():
        latest_path = base / name / "latest.json"
        attempt_path = base / name / "last_attempt.json"
        attempt = load_config(attempt_path) if attempt_path.exists() else {}
        latest = load_config(latest_path) if latest_path.exists() else {}
        report = f"[{latest['run_id']}]({Path(os.path.relpath(root / latest['report_path'], base)).as_posix()})" if latest else "Chưa chạy"
        data = f"[CSV/schema]({Path(os.path.relpath(root / latest['artifacts_dir'], base)).as_posix()}/)" if latest.get("artifacts_dir") else "—"
        lines.append(f"| {name} | {attempt.get('status', 'Chưa chạy')} | {report} | {data} |")
    base.mkdir(parents=True, exist_ok=True)
    (base / "INDEX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
