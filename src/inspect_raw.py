"""Read a verified raw CSV with explicit Spark schema and show a few rows."""
import argparse
import json
from pathlib import Path
import traceback
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import ROOT, load_config, logger, sha256, start_spark, utc_now, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="branded_food.csv", help="Table name from successful manifest")
    parser.add_argument("--rows", type=int, default=5)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/raw_inspection.json")
    args = parser.parse_args()
    if not 1 <= args.rows <= 20:
        parser.error("--rows must be between 1 and 20")
    config = load_config()
    log = logger("inspect_raw")
    evidence = {"started_at_utc": utc_now(), "status": "FAIL", "file_name": args.file}
    spark = None
    try:
        events = [json.loads(line) for line in (ROOT / config["manifest"]).read_text(encoding="utf-8").splitlines() if line.strip()]
        event = next(e for e in reversed(events) if e["file_name"] == args.file and e["status"] == "INGESTED"
                     and e["source"] == config["source"] and e["release"] == config["release"] and e["dataset"] == config["dataset"])
        path = (ROOT / event["destination"]).resolve()
        if not path.is_relative_to((ROOT / config["raw_dir"]).resolve()):
            raise ValueError("Raw destination escapes configured storage")
        if sha256(path) != event["sha256"]:
            raise ValueError("Raw checksum differs from ingestion manifest")
        from pyspark.sql.types import StructField, StructType, LongType, DoubleType, StringType
        declaration = config["csv_schemas"][args.file]
        kinds = {"long": LongType, "double": DoubleType, "string": StringType}
        schema = StructType([StructField(name, kinds[declaration.get("types", {}).get(name, "string")](), True)
                             for name in declaration["columns"]])
        spark = start_spark("beverage-checkpoint2-inspect")
        spark.sparkContext.setLogLevel("WARN")
        frame = (spark.read.option("header", "true").option("enforceSchema", "false")
                 .option("mode", "FAILFAST").option("multiLine", "true").option("quote", '"')
                 .option("escape", '"').option("encoding", "UTF-8").schema(schema).csv(path.as_posix()))
        frame.printSchema()
        rows = [row.asDict(recursive=True) for row in frame.take(args.rows)]
        if not rows:
            raise ValueError("Raw CSV contains no records")
        frame.show(args.rows, truncate=60)
        evidence.update(status="PASS", spark=spark.version, master=spark.sparkContext.master,
                        raw_path=event["destination"], sha256=event["sha256"], schema=frame.schema.jsonValue(),
                        schema_tree=frame._jdf.schema().treeString(), sample_rows=rows,
                        sample_count=len(rows), row_count=None, full_dataset_validated=False,
                        note="Explicit schema and first rows only; no filtering or ETL; no full row count claimed")
        log.info("PASS file=%s rows_shown=%s columns=%s", args.file, len(rows), len(frame.columns))
    except Exception:
        evidence["error"] = traceback.format_exc()
        log.exception("Raw inspection FAILED")
    finally:
        if spark is not None:
            spark.stop()
        evidence["finished_at_utc"] = utc_now()
        write_json(args.output, evidence)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
