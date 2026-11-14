"""Run Checkpoint 3 stages on one snapshot and record a full-data baseline."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import traceback

from src.common import ROOT, load_config, sha256, utc_now, write_json
from src.input_contract import lock_snapshot, verify_raw
from src.join_beverage_nutrients import verify_artifact
from src.run_storage import create_run, fresh_id, publish_run, run_paths
from src.measurement_basis import basis_mode


STAGES = [
    ("task1", "src.input_contract", "input_contract.json"),
    ("task2", "src.filter_beverages", "filter_result.json"),
    ("task3", "src.clean_nutrients", "nutrient_result.json"),
    ("task4", "src.join_beverage_nutrients", "join_result.json"),
    ("task5", "src.aggregate_nutrition_kpis", "kpi_result.json"),
    ("task5_repeat", "src.aggregate_nutrition_kpis", "kpi_result.json"),
]
SOURCE_FILES = ["RUN_CHECKPOINT3.ps1", "src/common.py", "src/checkpoint3_pipeline.py",
                "src/input_contract.py", "src/filter_beverages.py", "src/clean_nutrients.py",
                "src/measurement_basis.py", "src/join_beverage_nutrients.py", "src/aggregate_nutrition_kpis.py",
                "src/run_storage.py", "src/checkpoint3_task.py", "config/project.json"]


@contextmanager
def pipeline_lock(path):
    """An OS lock is released even if the parent process exits unexpectedly."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("Another Checkpoint 3 pipeline is running; the latest result was not changed") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def working_config(base, snapshot=None):
    config = copy.deepcopy(base)
    if snapshot:
        config["snapshot_run_id"] = snapshot
    stale = {"result_path", "result_sha256", "input_contract_path", "input_handoff_path", "input_handoff_sha256",
             "input_result_path", "input_result_sha256", "repeatability_reference_path", "input_recovery_note_path"}
    for name in ("task2", "task3", "task4", "task5"):
        config.setdefault(name, {})
        for key in stale:
            config[name].pop(key, None)
    config["task6"] = {}
    config.pop("task7", None)
    return config


def validate_receipt(pointer, snapshot, pipeline_id, expected_filename, root=ROOT):
    root = Path(root).resolve()
    receipt = load_config(pointer)
    path = Path(receipt["result_path"]).resolve()
    if (receipt.get("pipeline_run_id") != pipeline_id or path.name != expected_filename
            or not path.is_relative_to(root / "docs/evidence/checkpoint3")
            or sha256(path) != receipt["result_sha256"]):
        raise ValueError("Stage receipt has wrong ownership, path or SHA-256")
    result = load_config(path)
    if receipt["status"] != "PASS" or result["status"] != "PASS":
        raise ValueError(f"Stage evidence is not PASS: {result.get('error', path.name)}")
    if result["snapshot_run_id"] != snapshot:
        raise ValueError("Stage used another raw snapshot")
    return path, result


def wire_next(config, name, path, result, root=ROOT):
    relative = Path(path).relative_to(root).as_posix()
    if name == "task1":
        config["task2"]["input_contract_path"] = relative
    elif name == "task2":
        handoff = path.with_name("task3_input.json")
        data = load_config(handoff)
        if data["status"] != "READY_FOR_TASK3" or data["task2_result_sha256"] != sha256(path):
            raise ValueError("Task 2 did not produce a matching Task 3 handoff")
        config["task3"]["input_handoff_path"] = handoff.relative_to(root).as_posix()
    elif name == "task3":
        config["task3"].update(result_path=relative, result_sha256=sha256(path))
        config["task4"].update(input_result_path=relative, input_result_sha256=sha256(path))
    elif name == "task4":
        handoff = path.with_name("task5_input.json")
        data = load_config(handoff)
        if data["status"] != "READY_FOR_TASK5" or data["task4_result_sha256"] != sha256(path):
            raise ValueError("Task 4 did not produce a matching Task 5 handoff")
        config["task4"].update(result_path=relative, result_sha256=sha256(path))
        config["task5"].update(input_handoff_path=handoff.relative_to(root).as_posix(), input_handoff_sha256=sha256(handoff))
    elif name == "task5":
        config["task5"]["repeatability_reference_path"] = relative
    elif name == "task5_repeat":
        if result["repeatability"]["status"] != "PASS":
            raise ValueError("Independent KPI repetition did not pass")
        handoff = path.with_name("task6_input.json")
        data = load_config(handoff)
        if data["status"] != "READY_FOR_TASK6" or data["task5_result_sha256"] != sha256(path):
            raise ValueError("Repeated KPI run did not produce its matching handoff")
        config["task5"].update(result_path=relative, result_sha256=sha256(path))
        config["task6"]["input_handoff_path"] = handoff.relative_to(root).as_posix()


def run_stage(stage, config_path, folder, logs, pipeline_id, snapshot, executor=subprocess.run, root=ROOT):
    name, module, expected_filename = stage
    pointer = folder / f"{name}_receipt.json"
    if pointer.exists():
        raise ValueError("A stage receipt already exists in this fresh pipeline run")
    task = "task5" if name == "task5_repeat" else name
    stage_id = pipeline_id + ("_repeat" if name == "task5_repeat" else "")
    stage_folder, _ = run_paths(task, stage_id, root)
    stage_folder.mkdir(parents=True, exist_ok=True)
    log_path = stage_folder / "console.log"
    # Freeze the exact config consumed by this stage before wire_next updates it.
    stage_config = stage_folder / "run_config.json"
    write_json(stage_config, load_config(config_path))
    command = [sys.executable, "-B", "-u", "-m", module, "--task-config", str(stage_config),
               "--result-pointer", str(pointer), "--collect-baseline"]
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1", CHECKPOINT3_PIPELINE_RUN_ID=pipeline_id,
               CHECKPOINT3_STAGE_RUN_ID=stage_id, CHECKPOINT3_LOCAL_TASK_ATTEMPTS="4")
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as output:
        completed = executor(command, cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
    elapsed = round(time.perf_counter() - started, 3)
    if completed.returncode:
        raise RuntimeError(f"{name} exited {completed.returncode}; detailed log: {log_path}")
    if not pointer.exists():
        raise ValueError(f"{name} returned without its result receipt")
    path, result = validate_receipt(pointer, snapshot, pipeline_id, expected_filename, root)
    baseline_path = path.with_name("spark_baseline.json")
    baseline = load_config(baseline_path)
    if not baseline["frames"]:
        raise ValueError("A stage did not record its Spark baseline")
    for frame in baseline["frames"].values():
        plan = (root / frame["explain_formatted_path"]).resolve()
        if not plan.is_relative_to(path.parent) or sha256(plan) != frame["explain_sha256"] or frame["partitions_observed"] < 1:
            raise ValueError("Stage physical plan or partition evidence is invalid")
    details = {"name": name, "module": module, "status": "PASS", "wall_seconds": elapsed,
               "reported_runtime_seconds": result["elapsed_seconds"], "result_path": path.relative_to(root).as_posix(),
               "result_sha256": sha256(path), "log_path": log_path.relative_to(root).as_posix(),
               "spark_baseline_path": baseline_path.relative_to(root).as_posix(),
               "spark_baseline_sha256": sha256(baseline_path), "spark": baseline}
    return path, result, details


def execute_chain(config, config_path, folder, logs, pipeline_id, evidence, stage_runner=run_stage, root=ROOT):
    results = {}
    for index, stage in enumerate(STAGES, 1):
        name = stage[0]
        evidence["active_stage"] = name
        print(f"[{index}/{len(STAGES)}] {name} đang chạy...", flush=True)
        path, result, details = stage_runner(stage, config_path, folder, logs, pipeline_id, config["snapshot_run_id"], root=root)
        wire_next(config, name, path, result, root)
        write_json(config_path, config)
        evidence["stages"].append(details)
        results[name] = result
        print(f"      PASS | {details['wall_seconds']:.1f} s", flush=True)
    evidence["active_stage"] = None
    return results


def collect_outputs(results, root=ROOT):
    outputs = {}
    for stage, result in results.items():
        for metadata in result.values():
            if isinstance(metadata, dict) and {"path", "sha256", "schema_path", "schema_sha256", "row_count"}.issubset(metadata):
                verify_artifact(metadata, root)
                outputs[metadata["path"]] = dict(metadata, producer_stage=stage)
        reference = result.get("basis_reference")
        if reference:
            if sha256(root / reference["reference_path"]) != reference["reference_sha256"]:
                raise ValueError("Nutrition label reference changed during the pipeline")
    return list(outputs.values())


def collect_inputs(config, contract, root=ROOT):
    """CSV mode never opens, hashes or requires the optional dataset JSON."""
    mode = basis_mode(config["task3"])
    inputs = {"raw_csv_bytes": sum(table["size_bytes"] for table in contract["tables"]),
              "raw_tables": contract["tables"], "basis_mode": mode,
              "json_reference_enabled": mode == "json_crosscheck", "reference_zip_bytes": 0}
    if mode == "json_crosscheck":
        path = Path(root) / config["task3"]["basis_reference_zip"]
        checksum = sha256(path)
        if checksum != config["task3"]["basis_reference_sha256"]:
            raise ValueError("JSON reference ZIP differs from its configured checksum")
        inputs.update(reference_zip_path=config["task3"]["basis_reference_zip"],
                      reference_zip_bytes=path.stat().st_size, reference_zip_sha256=checksum)
    return inputs


def write_report(folder, evidence):
    logical = evidence.get("inputs", {})
    lines = ["# Baseline Checkpoint 3 toàn pipeline", "", f"Trạng thái: {evidence['status']}.",
             f"Pipeline run: `{evidence['run_id']}`.", f"Snapshot raw: `{evidence.get('snapshot_run_id', '?')}`.", "",
             "Phạm vi: Task 1 → 2 → 3 → 4 → 5, sau đó chạy KPI độc lập lần hai để đối chiếu.",
             "Đầu vào bắt đầu từ raw đã ingestion ở Checkpoint 2; giữ dữ liệu nguồn hiện tại.",
             "Mỗi stage có cấu hình bàn giao mới trong run_config.json và một tiến trình Spark local riêng.",
             "Thời gian bao gồm khởi động/dừng Spark, hash dữ liệu, kiểm tra chất lượng, ghi/đọc lại CSV và thu thập kế hoạch thực thi.",
             "Đây là một lượt đo trên máy local; cache hệ điều hành không được ép về trạng thái lạnh.", "",
             f"Chế độ cơ sở đo: {logical.get('basis_mode', 'json_crosscheck')}; dùng dataset JSON: {logical.get('json_reference_enabled', True)}.",
             f"CSV raw logic: {logical.get('raw_csv_bytes', '?')} byte; ZIP JSON được dùng: {logical.get('reference_zip_bytes', '?')} byte.",
             "Dung lượng logic tính mỗi tệp một lần; lượng I/O thực tế có thể lớn hơn do kiểm tra và đọc lại.", "",
             "| Stage | Wall time (s) | Runtime bên trong (s) | Partition quan sát |",
             "| --- | ---: | ---: | --- |"]
    for stage in evidence.get("stages", []):
        partitions = ", ".join(f"{name}={frame['partitions_observed']}" for name, frame in stage["spark"]["frames"].items())
        lines.append(f"| {stage['name']} | {stage['wall_seconds']} | {stage['reported_runtime_seconds']} | {partitions} |")
    lines.extend(["", f"Tổng wall time trình điều phối: {evidence.get('elapsed_seconds', '?')} giây.",
                  f"Tổng thời gian các tiến trình stage: {evidence.get('stage_wall_seconds', '?')} giây.",
                  f"Phần xác minh/điều phối còn lại: {evidence.get('orchestration_seconds', '?')} giây.", ""])
    if evidence.get("quality"):
        lines.extend(["## Kết quả kiểm tra", ""])
        for key, value in evidence["quality"].items():
            lines.append(f"- {key}: {value}")
    lines.extend(["", "Spark version, Java, Python, JVM heap, local master, default parallelism và SQL configuration nằm trong spark_baseline.json của mỗi stage.",
                  "Partition quan sát và explain(formatted) được thu trên frame thực tế, theo cấu hình AQE đang dùng.",
                  "Các tệp plans/*.txt có SHA-256 trong baseline; chúng phục vụ phân tích/tối ưu ở bước tiếp theo.",
                  "Source hash và cấu hình giúp xác định đúng mã đã chạy dù checkout đang có thay đổi chưa commit.",
                  "Log chi tiết console.log nằm trong thư mục evidence của từng task/run; stage lỗi dừng chuỗi và giữ kết quả thành công trước đó.",
                  "Bảng KPI giữ cơ sở 100 ml và 100 g riêng, dùng tập đủ điều kiện và kèm độ phủ theo chính sách Task 5."])
    if "error" in evidence:
        lines.extend(["", f"Lỗi: {evidence['error']}"])
    (folder / "baseline.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def publish_latest(folder, evidence, root=ROOT):
    if evidence["status"] != "PASS":
        raise ValueError("Only a successful complete pipeline may replace latest")
    result_path = folder / "pipeline_result.json"
    last = load_config(root / evidence["stages"][-1]["result_path"])
    joined = load_config(root / next(stage["result_path"] for stage in evidence["stages"] if stage["name"] == "task4"))
    handoff = {"status": "READY_FOR_TASK7", "snapshot_run_id": evidence["snapshot_run_id"],
               "pipeline_result_path": result_path.relative_to(root).as_posix(), "pipeline_result_sha256": sha256(result_path),
               "baseline_report_path": (folder / "baseline.md").relative_to(root).as_posix(),
               "kpi_output": last["output"], "coverage_output": last["coverage_output"], "joined_output": joined["output"],
               "quality": evidence["quality"], "scope": "Prepare checkpoint report, evidence, pitch and exit ticket"}
    write_json(folder / "task7_input.json", handoff)
    pointer = dict(handoff, run_id=evidence["run_id"], elapsed_seconds=evidence["elapsed_seconds"],
                   run_config_path=(folder / "run_config.json").relative_to(root).as_posix(),
                   handoff_path=(folder / "task7_input.json").relative_to(root).as_posix())
    (root / "docs/evidence/checkpoint3/latest_pipeline.md").write_text((folder / "baseline.md").read_text(encoding="utf-8"), encoding="utf-8")
    write_json(root / "docs/evidence/checkpoint3/latest_pipeline.json", pointer)


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-config", type=Path, default=ROOT / "config/checkpoint3.json")
    parser.add_argument("--snapshot-run-id")
    args = parser.parse_args(argv)
    try:
        with pipeline_lock(ROOT / "docs/evidence/checkpoint3/state/execution.lock"):
            return run(args)
    except Exception as exc:
        print(f"[LỖI] {exc}", flush=True)
        return 1


def run(args):
    started = time.perf_counter()
    run_id, folder, _ = create_run("task6", run_id=os.environ.get("CHECKPOINT3_STAGE_RUN_ID") or fresh_id("pipeline"))
    logs = folder
    evidence = {"status": "FAIL", "run_id": run_id, "started_at_utc": utc_now(), "stages": [],
                "scope": "Full-data Task 1–5 plus independent KPI repeat; Checkpoint 2 ingestion excluded",
                "environment": {"python": platform.python_version(), "os": platform.platform(),
                                "processor": platform.processor(), "logical_cpus": os.cpu_count(),
                                "free_disk_bytes_before": shutil.disk_usage(ROOT).free}}
    print(f"CHECKPOINT 3 | PIPELINE | {run_id}", flush=True)
    try:
        source_config_path = (ROOT / args.task_config).resolve()
        base = load_config(source_config_path)
        config = working_config(base, args.snapshot_run_id)
        evidence.update(snapshot_run_id=config["snapshot_run_id"], source_task_config_path=str(source_config_path),
                        source_task_config_sha256=sha256(source_config_path))
        evidence["source_sha256"] = {name: sha256(ROOT / name) for name in SOURCE_FILES}
        scope_source = (ROOT / base["task2"]["scope_policy_path"]).resolve()
        policy = load_config(scope_source)
        write_json(folder / "beverage_scope.json", policy)
        config["task2"]["scope_policy_path"] = (folder / "beverage_scope.json").relative_to(ROOT).as_posix()
        config_path = folder / "run_config.json"
        write_json(config_path, config)
        print("[INPUT] Kiểm tra raw snapshot và cấu hình trước khi chạy...", flush=True)
        contract = lock_snapshot(load_config(), config)
        evidence["inputs"] = collect_inputs(config, contract)
        results = execute_chain(config, config_path, folder, logs, run_id, evidence)
        evidence["outputs"] = collect_outputs(results)
        for table in contract["tables"]:
            verify_raw(table)
        for name, checksum in evidence["source_sha256"].items():
            if sha256(ROOT / name) != checksum:
                raise ValueError(f"Implementation or project config changed during the pipeline: {name}")
        if evidence["inputs"]["json_reference_enabled"]:
            if sha256(ROOT / evidence["inputs"]["reference_zip_path"]) != evidence["inputs"]["reference_zip_sha256"]:
                raise ValueError("JSON reference ZIP changed during the pipeline")
        task2, task3, task4, task5 = (results[name] for name in ("task2", "task3", "task4", "task5_repeat"))
        evidence["quality"] = {"selected_beverages": task2["output"]["row_count"], "review_beverages": task2["review_output"]["row_count"],
                               "clean_nutrient_records": task3["clean_output"]["row_count"], "joined_slots": task4["output"]["row_count"],
                               "missing_source_slots": task4["output_metrics"]["no_source_record"],
                               "value_review_records": task4["output_metrics"]["value_review_records"],
                               "eligible_nutrient_records": task5["output_metrics"]["compared_nutrient_records"],
                               "kpi_rows": task5["output"]["row_count"], "coverage_rows": task5["coverage_output"]["row_count"],
                               "independent_kpi_repeat": task5["repeatability"]["status"]}
        evidence.update(status="PASS", inputs_outputs_verified=True, run_config_sha256=sha256(config_path),
                        free_disk_bytes_after=shutil.disk_usage(ROOT).free)
    except (Exception, KeyboardInterrupt) as exc:
        evidence["error"] = str(exc) or type(exc).__name__
        (folder / "error.log").write_text(traceback.format_exc(), encoding="utf-8")
        print(f"[LỖI] Dừng tại {evidence.get('active_stage', 'input/verification')}: {evidence['error']}", flush=True)
    finally:
        total = round(time.perf_counter() - started, 3)
        stage_total = round(sum(stage["wall_seconds"] for stage in evidence["stages"]), 3)
        evidence.update(finished_at_utc=utc_now(), elapsed_seconds=total, stage_wall_seconds=stage_total,
                        orchestration_seconds=round(total - stage_total, 3))
        write_json(folder / "pipeline_result.json", evidence)
        write_report(folder, evidence)
        publish_run(folder / "pipeline_result.json")
    if evidence["status"] == "PASS":
        publish_latest(folder, evidence)
    print(f"[KẾT QUẢ] {evidence['status']} | {evidence['elapsed_seconds']:.1f} s", flush=True)
    print(f"[BASELINE] {folder.relative_to(ROOT).as_posix()}/baseline.md", flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
