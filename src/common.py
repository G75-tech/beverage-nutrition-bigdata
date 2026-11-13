"""Shared paths, UTC evidence and local Spark configuration."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def load_config(path=None):
    return json.loads(Path(path or ROOT / "config/project.json").read_text(encoding="utf-8"))


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def logger(name):
    folder = ROOT / "logs/checkpoint2"
    folder.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(name)
    if not log.handlers:
        formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
        formatter.converter = time.gmtime
        log.setLevel(logging.INFO)
        for handler in (logging.StreamHandler(), logging.FileHandler(folder / f"{name}.log", encoding="utf-8")):
            handler.setFormatter(formatter)
            log.addHandler(handler)
    return log


def prepare_environment():
    """Use project portable JDK if present, without changing Windows settings."""
    runtime = ROOT / ".tools/runtime.json"
    if runtime.exists():
        data = json.loads(runtime.read_text(encoding="utf-8"))
        java_home = ROOT / data["java_home"]
        if (java_home / "bin/java.exe").is_file():
            os.environ["JAVA_HOME"] = str(java_home)
            os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def start_spark(name):
    prepare_environment()
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[*]").appName(name)
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def add_stage_arguments(parser):
    parser.add_argument("--task-config", type=Path, help="Run-specific Checkpoint 3 configuration")
    parser.add_argument("--result-pointer", type=Path, help="Write a machine-readable stage result receipt")
    parser.add_argument("--collect-baseline", action="store_true", help="Save actual Spark configuration, partitions and formatted plans")


def publish_stage_result(result_path, pointer):
    from src.run_storage import publish_run
    publish_run(result_path)
    if pointer is not None:
        path = Path(result_path).resolve()
        result = load_config(path)
        write_json(pointer, {"status": result["status"], "result_path": str(path), "result_sha256": sha256(path),
                             "pipeline_run_id": os.environ.get("CHECKPOINT3_PIPELINE_RUN_ID")})


def record_spark_baseline(spark, frame, folder, label, root=ROOT):
    """Observe the actual stage frame; formatted plans are stored outside the console."""
    from contextlib import redirect_stdout
    import platform
    folder = Path(folder)
    plans = folder / "plans"
    plans.mkdir(exist_ok=True)
    path = plans / f"{label}.txt"
    partitions = frame.rdd.getNumPartitions()
    with path.open("w", encoding="utf-8") as output, redirect_stdout(output):
        frame.explain("formatted")
    baseline_path = folder / "spark_baseline.json"
    if baseline_path.exists():
        baseline = load_config(baseline_path)
    else:
        names = ["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                 "spark.sql.adaptive.coalescePartitions.enabled", "spark.sql.files.maxPartitionBytes",
                 "spark.sql.csv.parser.columnPruning.enabled", "spark.sql.session.timeZone"]
        baseline = {"python": platform.python_version(), "spark": spark.version,
                    "java": spark._jvm.java.lang.System.getProperty("java.version"),
                    "java_vm": spark._jvm.java.lang.System.getProperty("java.vm.name"),
                    "jvm_max_heap_bytes": spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
                    "master": spark.sparkContext.master, "default_parallelism": spark.sparkContext.defaultParallelism,
                    "sql_configuration": {name: spark.conf.get(name) for name in names}, "frames": {}}
    baseline["frames"][label] = {"observed_at_utc": utc_now(), "partitions_observed": partitions,
                                 "column_count": len(frame.columns), "explain_formatted_path": path.relative_to(root).as_posix(),
                                 "explain_sha256": sha256(path),
                                 "partition_note": "Observed from this frame with current AQE configuration; not a cluster-size measurement"}
    write_json(baseline_path, baseline)
    return baseline["frames"][label]
