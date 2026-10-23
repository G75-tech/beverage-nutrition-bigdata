"""Run the checkpoint with a short Vietnamese console summary and full file logs."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import traceback
import uuid

from src.common import ROOT, load_config, utc_now, write_json


class StepFailed(RuntimeError):
    """A failed child command; its full output remains in the step log."""


def duration(seconds):
    return f"{seconds:.1f}".replace(".", ",") + " s"


def short_error(evidence, log_path, returncode):
    error = evidence.get("error")
    if not error:
        error = next((event.get("error") for event in evidence.get("events", [])
                      if event.get("status") == "FAILED"), None)
    if not error:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        errors = [line for line in lines if re.match(r"^\w*(?:Error|Exception):", line)]
        error = errors[-1] if errors else f"Lệnh kết thúc với mã {returncode}; xem log chi tiết."
    # Tracebacks belong in the file; show only the final exception on the console.
    lines = [line.strip() for line in str(error).splitlines() if line.strip()]
    return (lines[-1] if lines else "Không có chi tiết lỗi.")[:200]


def run_step(title, arguments, evidence_path, log_path, steps, total, publish=None):
    """Capture both native stderr and stdout, then trust exit code AND fresh evidence."""
    print(f"[{len(steps) + 1}/{total}] {title}...", end="", flush=True)
    started = time.perf_counter()
    step = {"title": title, "arguments": arguments, "status": "FAIL",
            "log_path": log_path.relative_to(ROOT).as_posix()}
    steps.append(step)
    # Child Python output is UTF-8 even when redirected to a file on Windows.
    environment = dict(os.environ, PYTHONIOENCODING="utf-8")
    try:
        with log_path.open("w", encoding="utf-8") as stream:
            process = subprocess.run([sys.executable, "-u", *arguments], cwd=ROOT,
                                     stdout=stream, stderr=subprocess.STDOUT, env=environment)
        step["exit_code"] = process.returncode
        evidence = {}
        if evidence_path.is_file():
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
            if publish is not None:
                write_json(publish, evidence)
        if process.returncode != 0 or evidence.get("status") != "PASS":
            if process.returncode == 0 and not evidence:
                raise StepFailed("Thiếu bằng chứng kết quả của bước vừa chạy.")
            raise StepFailed(short_error(evidence, log_path, process.returncode))
        step["status"] = "PASS"
        return evidence
    finally:
        step["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        state = "OK" if step["status"] == "PASS" else "LỖI"
        print(f" {state} ({duration(step['elapsed_seconds'])})", flush=True)


def main(argv=None):
    # Windows PowerShell may attach a cp1252 pipe instead of a Unicode console.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-environment", action="store_true",
                        help="Skip the environment smoke test; still run ingestion and inspection")
    args = parser.parse_args(argv)
    started = time.perf_counter()
    stamp = utc_now().replace("-", "").replace(":", "").replace(".", "")
    run_id = f"run_{stamp}_{uuid.uuid4().hex[:6]}"
    folder = ROOT / "logs/checkpoint2" / run_id
    steps = []
    total = 4 if args.skip_environment else 5
    summary = {"run_id": run_id, "started_at_utc": utc_now(), "status": "FAIL",
               "environment_skipped": args.skip_environment, "steps": steps}
    print("\nCHECKPOINT 2 | SO SÁNH DINH DƯỠNG ĐỒ UỐNG")
    print("-" * 60, flush=True)
    try:
        folder.mkdir(parents=True, exist_ok=False)
        config = load_config()
        if args.skip_environment:
            print("Môi trường: bỏ qua kiểm tra theo tuỳ chọn.\n", flush=True)
        else:
            evidence_path = folder / "environment.json"
            result = run_step("Kiểm tra môi trường", ["-m", "src.env_check", "--output", str(evidence_path)],
                              evidence_path, folder / "01_environment.log", steps, total,
                              publish=ROOT / "docs/evidence/environment.json")
            java = re.search(r'version "([^\"]+)"', result["java_version"])
            java_version = java.group(1) if java else "?"
            print(f"      Python {result['python'].split()[0]} | Java {java_version} | "
                  f"Spark {result['spark']} ({result['master']})")
            count = f"{result['actual_count']:,}".replace(",", ".")
            print(f"      Smoke: {count} dòng | Python worker: OK", flush=True)

        for attempt in range(1, 4):
            ingestion_id = f"{run_id}_{attempt}"
            result = run_step(f"Nạp CSV - lần {attempt}/3",
                              ["-m", "src.ingest_file", "--run-id", ingestion_id],
                              ROOT / "logs/checkpoint2" / f"{ingestion_id}.json",
                              folder / f"ingestion_{attempt}.log", steps, total)
            counts = Counter(event["status"] for event in result["events"])
            print(f"      {len(result['events'])} file | {counts['INGESTED']} mới | "
                  f"{counts['SKIPPED_DUPLICATE']} trùng (bỏ qua)", flush=True)

        evidence_path = folder / "raw_inspection.json"
        result = run_step("Kiểm tra raw bằng Spark",
                          ["-m", "src.inspect_raw", "--output", str(evidence_path)],
                          evidence_path, folder / "raw_inspection.log", steps, total,
                          publish=ROOT / "docs/evidence/raw_inspection.json")
        columns = len(result["schema"]["fields"])
        print(f"      {result['file_name']} | {columns} cột | {result['sample_count']} dòng mẫu", flush=True)
        summary["status"] = "PASS"
    except KeyboardInterrupt:
        summary.update(error="Người dùng dừng chương trình.", exit_code=130)
        print("\n[ĐÃ DỪNG] Người dùng dừng chương trình.", flush=True)
    except Exception as exc:
        summary.update(error=str(exc), exit_code=1)
        if folder.is_dir():
            (folder / "runner_error.log").write_text(traceback.format_exc(), encoding="utf-8")
        print(f"[LỖI] {str(exc).splitlines()[-1][:200]}", flush=True)
    summary.update(finished_at_utc=utc_now(), elapsed_seconds=round(time.perf_counter() - started, 3))
    summary.setdefault("exit_code", 0)
    passed = sum(step["status"] == "PASS" for step in steps)
    print("-" * 60)
    state = "THÀNH CÔNG" if summary["status"] == "PASS" else "CHƯA HOÀN THÀNH"
    print(f"KẾT QUẢ: {state} | {passed}/{total} bước | {duration(summary['elapsed_seconds'])}")
    if summary["status"] == "PASS":
        raw = (Path(config["raw_dir"]) / config["dataset"]).as_posix()
        print(f"Raw: {raw}/")
    if folder.is_dir():
        write_json(folder / "summary.json", summary)
        print(f"Log chi tiết: {folder.relative_to(ROOT).as_posix()}/")
    print(flush=True)
    return summary["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
