"""Pin a Checkpoint 2 raw snapshot and measure its full inputs with Spark."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import platform
import sys
import time
import traceback

from src.common import ROOT, load_config, sha256, start_spark, utc_now, write_json
from src.run_storage import create_run
from src.common import add_stage_arguments, publish_stage_result, record_spark_baseline


CSV_OPTIONS = {
    "header": "true",
    "enforceSchema": "false",
    "mode": "FAILFAST",
    "multiLine": "true",
    "quote": '"',
    "escape": '"',
    "encoding": "UTF-8",
}


def lock_snapshot(config, task_config, snapshot_run_id=None, root=ROOT):
    """Require one successful copy of every configured table in one explicit run."""
    root = Path(root).resolve()
    run_id = snapshot_run_id or task_config["snapshot_run_id"]
    names = config["default_files"]
    if not names or len(set(names)) != len(names):
        raise ValueError("default_files must contain distinct table names")
    if set(task_config["tables"]) != set(names):
        raise ValueError("Task 1 table contracts must match default_files")
    manifest = root / config["manifest"]
    events = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()
              if line.strip()]
    selected = [event for event in events
                if event["status"] == "INGESTED" and event["run_id"] == run_id
                and all(event[key] == config[key] for key in ("source", "release", "dataset"))]
    raw_root = (root / config["raw_dir"]).resolve()
    tables = []
    for name in names:
        matches = [event for event in selected if event["file_name"] == name]
        if len(matches) != 1:
            raise ValueError(f"Snapshot {run_id}: expected one INGESTED event for {name}, got {len(matches)}")
        event = matches[0]
        path = (root / event["destination"]).resolve()
        if not path.is_relative_to(raw_root) or path.name != name:
            raise ValueError(f"Invalid raw path for {name}")
        declaration = config["csv_schemas"][name]
        metadata = task_config["tables"][name]
        keys = list(dict.fromkeys(metadata["primary_key"] + metadata["join_keys"]
                                 + declaration["required_values"]))
        if not keys or not set(keys).issubset(declaration["columns"]):
            raise ValueError(f"Invalid key declaration for {name}")
        table = {
            "file_name": name,
            "raw_path": path.relative_to(root).as_posix(),
            "size_bytes": event["size_bytes"],
            "sha256": event["sha256"],
            "format": "CSV",
            "grain": metadata["grain"],
            "primary_key": metadata["primary_key"],
            "join_keys": metadata["join_keys"],
            "checked_keys": keys,
            "primary_key_uniqueness_checked": False,
            "schema_declaration": declaration,
        }
        verify_raw(table, root)
        tables.append(table)
    return {"source": config["source"], "release": config["release"],
            "dataset": config["dataset"], "snapshot_run_id": run_id,
            "manifest_path": str(manifest), "tables": tables}


def verify_raw(table, root=ROOT):
    path = Path(root) / table["raw_path"]
    if not path.is_file() or path.stat().st_size != table["size_bytes"]:
        raise ValueError(f"Missing raw file or size mismatch: {table['file_name']}")
    if sha256(path) != table["sha256"]:
        raise ValueError(f"Raw SHA-256 mismatch: {table['file_name']}")


def read_locked_csv(spark, table, root=ROOT):
    """Use the schema and path saved in the input contract, including quoted newlines."""
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    declaration = table["schema_declaration"]
    types = {"long": LongType, "double": DoubleType, "string": StringType}
    schema = StructType([
        StructField(name, types[declaration["types"].get(name, "string")](), True)
        for name in declaration["columns"]
    ])
    # Parse every declared field even when the aggregate only references key columns.
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    path = (Path(root) / table["raw_path"]).resolve()
    return spark.read.schema(schema).options(**CSV_OPTIONS).csv(path.as_posix())


def measure_input(frame, keys):
    """Reference every column so a count cannot skip parsing non-key values."""
    from pyspark.sql import functions as F

    expressions = [F.count(F.lit(1)).alias("row_count")]
    expressions.extend(
        F.coalesce(F.sum(F.col(column).isNull().cast("long")), F.lit(0)).alias(f"null_{column}")
        for column in frame.columns
    )
    counts = frame.agg(*expressions).first().asDict()
    return {"row_count": counts["row_count"],
            "null_counts": {column: counts[f"null_{column}"] for column in frame.columns},
            "null_key_counts": {key: counts[f"null_{key}"] for key in keys}}


def write_report(folder, evidence):
    lines = ["# Input contract Checkpoint 3 Task 1", "",
             f"Trạng thái {evidence['status']}. Snapshot `{evidence.get('snapshot_run_id', '?')}`.", "",
             "Counts và null khóa được đo trên toàn bộ record bằng Spark với schema khai báo.",
             "Grain và khóa chính là khai báo thiết kế. Uniqueness và join sẽ kiểm tra ở task sau.", "",
             "| File | Số dòng | Byte | Null ở khóa | Partition quan sát |",
             "| --- | ---: | ---: | --- | ---: |"]
    for table in evidence.get("tables", []):
        lines.append(f"| {table['file_name']} | {table.get('row_count', 'chưa đo')} | "
                     f"{table['size_bytes']} | {table.get('null_key_counts', {})} | "
                     f"{table.get('partitions_observed', '?')} |")
    for table in evidence.get("tables", []):
        lines.extend(["", f"## {table['file_name']}", "", f"Grain `{table['grain']}`.", "",
                      f"Khóa chính khai báo `{table['primary_key']}`. Khóa join `{table['join_keys']}`.", "",
                      f"Raw `{table['raw_path']}`.", "", f"SHA-256 `{table['sha256']}`.", "",
                      "```text", table.get("schema_tree", "Chưa đọc schema bằng Spark"), "```"])
    lines.extend(["", f"Runtime của riêng Task 1 {evidence['elapsed_seconds']} giây.", "",
                  "Chi tiết schema, read options, môi trường và phạm vi kiểm tra nằm trong input_contract.json."])
    (folder / "input_contract.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    add_stage_arguments(parser)
    parser.add_argument("--snapshot-run-id", help="Override the pinned INGESTED run in config/checkpoint3.json")
    args = parser.parse_args(argv)
    started = time.perf_counter()
    run_id, folder, artifacts = create_run("task1")
    evidence = {"status": "FAIL", "run_id": run_id, "started_at_utc": utc_now(), "read_options": CSV_OPTIONS,
                "check_scope": ["raw file size and SHA-256 before and after scan",
                                "full Spark CSV scan with declared types", "row count and null keys"],
                "deferred_checks": ["primary key uniqueness", "foreign key matching",
                                    "nutrition values and measurement basis", "business filtering"],
                "tables": []}
    spark = None
    print("CHECKPOINT 3 | TASK 1 | INPUT CONTRACT", flush=True)
    try:
        print("[INPUT] Xác minh snapshot, đường dẫn và SHA-256...", flush=True)
        config = load_config()
        task_config = load_config(args.task_config or ROOT / "config/checkpoint3.json")
        evidence.update(lock_snapshot(config, task_config, args.snapshot_run_id))
        print(f"[SNAPSHOT] {evidence['snapshot_run_id']}", flush=True)
        spark = start_spark("beverage-checkpoint3-input-contract")
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
        evidence["environment"] = {"python": platform.python_version(), "spark": spark.version,
                                   "java": spark._jvm.java.lang.System.getProperty("java.version"),
                                   "master": spark.sparkContext.master,
                                   "csv_parser_column_pruning": False}
        for index, table in enumerate(evidence["tables"], 1):
            print(f"[{index}/{len(evidence['tables'])}] Đọc toàn bộ {table['file_name']}...", flush=True)
            table_started = time.perf_counter()
            frame = read_locked_csv(spark, table)
            if args.collect_baseline:
                record_spark_baseline(spark, frame, folder, table["file_name"].replace(".csv", ""))
            table.update(schema=frame.schema.jsonValue(), schema_tree=frame._jdf.schema().treeString(),
                         partitions_observed=frame.rdd.getNumPartitions())
            table.update(measure_input(frame, table["checked_keys"]))
            table["elapsed_seconds"] = round(time.perf_counter() - table_started, 3)
            print(f"      {table['row_count']:,} dòng | null khóa {table['null_key_counts']}", flush=True)
            if table["row_count"] == 0 or any(table["null_key_counts"].values()):
                raise ValueError(f"Input gate failed: empty table or null keys in {table['file_name']}")
        for table in evidence["tables"]:
            verify_raw(table)
        evidence.update(status="PASS", total_input_bytes=sum(t["size_bytes"] for t in evidence["tables"]),
                        raw_integrity_verified_before_and_after=True)
    except Exception as exc:
        evidence["error"] = str(exc)
        (folder / "error.log").write_text(traceback.format_exc(), encoding="utf-8")
        print(f"[LỖI] {str(exc).splitlines()[0][:220]}", flush=True)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                evidence.update(status="FAIL", spark_stop_error=str(exc))
        evidence.update(finished_at_utc=utc_now(), elapsed_seconds=round(time.perf_counter() - started, 3))
        write_json(folder / "input_contract.json", evidence)
        write_report(folder, evidence)
    publish_stage_result(folder / "input_contract.json", args.result_pointer)
    print(f"[KẾT QUẢ] {evidence['status']} | {evidence['elapsed_seconds']:.1f} s", flush=True)
    print(f"[EVIDENCE] {folder.relative_to(ROOT).as_posix()}", flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
