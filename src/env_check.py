"""Run the required million-row smoke test and save real environment evidence."""
import argparse
import importlib.metadata
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import traceback

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import ROOT, logger, prepare_environment, start_spark, utc_now, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/environment.json")
    args = parser.parse_args()
    log = logger("environment")
    evidence = {"started_at_utc": utc_now(), "python": sys.version, "executable": sys.executable,
                "platform": platform.platform(), "cpu_count": os.cpu_count(), "status": "FAIL"}
    spark = None
    try:
        prepare_environment()
        java = shutil.which("java")
        if not java:
            raise RuntimeError("Java not found; run setup.ps1 or set JAVA_HOME/PATH to Java 17")
        result = subprocess.run([java, "-version"], capture_output=True, text=True, check=True)
        java_version = result.stderr.strip() or result.stdout.strip()
        evidence.update(java_executable=java, java_home=os.environ.get("JAVA_HOME"), java_version=java_version,
                        pyspark=importlib.metadata.version("pyspark"), py4j=importlib.metadata.version("py4j"))
        if sys.version_info[:2] != (3, 11):
            raise RuntimeError("Project baseline requires Python 3.11; use .venv/Scripts/python.exe")
        if 'version "17.' not in java_version:
            raise RuntimeError("Project baseline requires Java 17")
        if evidence["pyspark"] != "3.5.9":
            raise RuntimeError("Expected pinned PySpark 3.5.9")
        spark = start_spark("beverage-checkpoint2-environment")
        spark.sparkContext.setLogLevel("WARN")
        count = spark.range(1_000_000).count()
        # Also exercise a Python worker; range().count() alone only exercises JVM execution.
        worker = spark.sparkContext.parallelize([1, 2, 3], 2).map(lambda n: n + 1).collect()
        evidence.update(spark=spark.version, master=spark.sparkContext.master, expected_count=1_000_000,
                        actual_count=count, python_worker_actual=worker, python_worker_expected=[2, 3, 4],
                        default_parallelism=spark.sparkContext.defaultParallelism)
        if count != 1_000_000 or worker != [2, 3, 4]:
            raise AssertionError("Smoke test mismatch")
        evidence["status"] = "PASS"
        log.info("PASS Spark=%s master=%s count=%s Python worker=%s", spark.version, spark.sparkContext.master, count, worker)
    except Exception:
        evidence["error"] = traceback.format_exc()
        log.exception("Environment smoke test FAILED")
    finally:
        if spark is not None:
            spark.stop()
        evidence["finished_at_utc"] = utc_now()
        write_json(args.output, evidence)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
