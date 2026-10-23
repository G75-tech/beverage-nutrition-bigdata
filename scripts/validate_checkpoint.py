"""Run real checkpoint checks and record subprocess output and pass/fail evidence."""
import json
from pathlib import Path
import subprocess
import sys
import time
import unittest
from collections import Counter

from src.common import ROOT, load_config, sha256, utc_now, write_json


def command(name, arguments):
    started = utc_now()
    begin = time.monotonic()
    log_path = ROOT / "logs/checkpoint2" / (name + "_console.log")
    with log_path.open("w", encoding="utf-8") as stream:
        process = subprocess.run([sys.executable] + arguments, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                 encoding="utf-8", errors="replace")
    evidence = {"name": name, "arguments": arguments, "started_at_utc": started, "finished_at_utc": utc_now(),
                "elapsed_seconds": round(time.monotonic() - begin, 3), "exit_code": process.returncode,
                "status": "PASS" if process.returncode == 0 else "FAIL", "log_path": log_path.relative_to(ROOT).as_posix()}
    print(name, evidence["status"], evidence["elapsed_seconds"], "seconds", flush=True)
    if process.returncode:
        print(log_path.read_text(encoding="utf-8")[-7000:], flush=True)
    return evidence


def main():
    folder = ROOT / "logs/checkpoint2"
    folder.mkdir(parents=True, exist_ok=True)
    evidence = {"started_at_utc": utc_now(), "checks": []}
    evidence["checks"].append(command("environment", ["-m", "src.env_check"]))
    if evidence["checks"][-1]["status"] != "PASS":
        evidence.update(finished_at_utc=utc_now(), status="FAIL", note="Ingestion not started because environment failed")
        write_json(ROOT / "docs/evidence/validation.json", evidence)
        return 1
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    with (folder / "unit_tests_console.log").open("w", encoding="utf-8") as stream:
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    unit = {"name": "unit_tests", "status": "PASS" if result.wasSuccessful() else "FAIL", "tests_run": result.testsRun,
            "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
            "log_path": "logs/checkpoint2/unit_tests_console.log"}
    evidence["checks"].append(unit)
    write_json(ROOT / "docs/evidence/unit_tests.json", unit)
    print("unit_tests", unit, flush=True)
    stamp = utc_now().replace("-", "").replace(":", "").replace(".", "")
    demo = []
    for number in range(1, 4):
        run_id = f"checkpoint2_{stamp}_{number}"
        evidence["checks"].append(command(f"ingestion_{number}", ["-m", "src.ingest_file", "--run-id", run_id]))
        path = folder / (run_id + ".json")
        if path.exists():
            demo.append(json.loads(path.read_text(encoding="utf-8")))
    write_json(ROOT / "docs/evidence/ingestion_three_runs.json", demo)
    evidence["checks"].append(command("raw_inspection", ["-m", "src.inspect_raw"]))
    # Re-hash every original after all operations. Includes both ZIP archives.
    inventory = json.loads((ROOT / "docs/evidence/inventory_before.json").read_text(encoding="utf-8"))
    preservation = []
    for entry in inventory["source_files"]:
        path = ROOT / entry["path"]
        after = {"path": entry["path"], "exists": path.is_file(), "before_sha256": entry["sha256"],
                 "before_size_bytes": entry["size_bytes"], "before_mtime_ns": entry["mtime_ns"]}
        if path.is_file():
            after.update(after_sha256=sha256(path), after_size_bytes=path.stat().st_size, after_mtime_ns=path.stat().st_mtime_ns)
        after["status"] = "PASS" if (after.get("after_sha256") == entry["sha256"]
                         and after.get("after_size_bytes") == entry["size_bytes"]
                         and after.get("after_mtime_ns") == entry["mtime_ns"]) else "FAIL"
        preservation.append(after)
    write_json(ROOT / "docs/evidence/source_preservation.json", preservation)
    evidence["checks"].append({"name": "original_sources_unchanged", "status": "PASS" if all(e["status"] == "PASS" for e in preservation) else "FAIL",
                               "files_checked": len(preservation)})
    # The two retries must all skip, point at the first run's destinations and produce no extra copy.
    config = load_config()
    replay_ok = len(demo) == 3 and all(r["status"] == "PASS" and len(r["events"]) == 4 for r in demo)
    if replay_ok:
        first = {e["file_name"]: e["destination"] for e in demo[0]["events"]}
        replay_ok = all(e["status"] == "SKIPPED_DUPLICATE" and e["destination"] == first[e["file_name"]]
                        for run in demo[1:] for e in run["events"])
    raw_files = list((ROOT / config["raw_dir"]).rglob("*.csv"))
    replay = {"name": "three_run_deduplication", "status": "PASS" if replay_ok and len(raw_files) == 4 else "FAIL",
              "run_status_counts": [dict(Counter(e["status"] for e in run["events"])) for run in demo],
              "raw_csv_count": len(raw_files), "raw_total_bytes": sum(p.stat().st_size for p in raw_files)}
    evidence["checks"].append(replay)
    evidence["finished_at_utc"] = utc_now()
    evidence["status"] = "PASS" if all(c["status"] == "PASS" for c in evidence["checks"]) else "FAIL"
    write_json(ROOT / "docs/evidence/validation.json", evidence)
    print(json.dumps(evidence, indent=2), flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
