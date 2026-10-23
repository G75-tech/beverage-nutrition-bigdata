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
