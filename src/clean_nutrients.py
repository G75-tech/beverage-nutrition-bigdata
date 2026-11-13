"""Audit nutrient amounts and duplicate pairs for the pinned Task 2 beverage set."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import platform
import sys
import time
import traceback

from src.common import ROOT, load_config, sha256, start_spark, utc_now, write_json
from src.run_storage import create_run
from src.common import add_stage_arguments, publish_stage_result, record_spark_baseline
from src.filter_beverages import load_locked_input, write_csv
from src.input_contract import CSV_OPTIONS, measure_input, read_locked_csv, verify_raw
from src.measurement_basis import write_json_reference


def load_handoff(path, config, root=ROOT):
    root = Path(root).resolve()
    handoff = load_config(path)
    if handoff.get("status") != "READY_FOR_TASK3":
        raise ValueError("Expected a successful Task 2 handoff")
    result_path = root / handoff["task2_result_path"]
    if sha256(result_path) != handoff["task2_result_sha256"]:
        raise ValueError("Task 2 result differs from the pinned handoff")
    result = load_config(result_path)
    if result["status"] != "PASS" or result["snapshot_run_id"] != handoff["snapshot_run_id"]:
        raise ValueError("Task 2 result is failed or refers to another snapshot")
    contract_path = root / handoff["input_contract_path"]
    if sha256(contract_path) != handoff["input_contract_sha256"]:
        raise ValueError("Task 1 input contract changed")
    contract = load_locked_input(contract_path, config, root)
    if contract["snapshot_run_id"] != handoff["snapshot_run_id"]:
        raise ValueError("Task 1 and Task 2 snapshots differ")
    for name in ("food_nutrient", "nutrient"):
        table = next(table for table in contract["tables"] if table["file_name"] == name + ".csv")
        if handoff[name + "_raw"] != table:
            raise ValueError(f"Task 2 handoff changed the locked {name} table")
    output = result["output"]
    if output != handoff["beverages"]:
        raise ValueError("Handoff beverage output differs from the Task 2 result")
    for key, checksum in (("path", "sha256"), ("schema_path", "schema_sha256")):
        file_path = (root / output[key]).resolve()
        if not file_path.is_relative_to(root / "artifacts") or sha256(file_path) != output[checksum]:
            raise ValueError("Beverage CSV or schema is outside artifacts or has changed")
    return handoff, result, contract


def classify_amounts(raw):
    from pyspark.sql import functions as F

    frame = (raw.select(F.col("id").alias("record_id_raw"), F.col("fdc_id").alias("fdc_id_raw"),
                        F.col("nutrient_id").alias("nutrient_id_raw"), F.col("amount").alias("amount_raw"),
                        "derivation_id")
        .withColumn("id", F.expr("try_cast(trim(record_id_raw) as bigint)"))
        .withColumn("fdc_id", F.expr("try_cast(trim(fdc_id_raw) as bigint)"))
        .withColumn("nutrient_id", F.expr("try_cast(trim(nutrient_id_raw) as bigint)"))
        .withColumn("amount", F.expr("try_cast(trim(amount_raw) as double)")))
    missing = F.col("amount_raw").isNull() | (F.trim(F.col("amount_raw")) == "")
    bad_key = F.col("id").isNull() | F.col("fdc_id").isNull() | F.col("nutrient_id").isNull()
    status = (F.when(bad_key, "invalid_key")
        .when(missing, "missing_amount")
        .when(F.col("amount").isNull(), "invalid_amount")
        .when(F.isnan("amount") | F.col("amount").isin(float("inf"), float("-inf")), "nonfinite_amount")
        .when(F.col("amount") < 0, "negative_amount")
        .otherwise("valid"))
    return (frame.withColumn("amount_status", status)
            .withColumn("amount", F.when(F.col("amount_status").isin("valid", "negative_amount"), F.col("amount"))))


def resolve_pairs(frame):
    """Prefer a valid value over missing; quarantine pairs with distinct valid values."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    keys = ["fdc_id", "nutrient_id"]
    stats = frame.groupBy(*keys).agg(
        F.count("*").alias("source_records"),
        F.countDistinct(F.when(F.col("amount_status") == "valid", F.col("amount"))).alias("distinct_valid_values"),
        F.sum((F.col("amount_status") == "valid").cast("long")).alias("valid_records"),
        F.sum((F.col("amount_status") == "missing_amount").cast("long")).alias("missing_records"),
    )
    pair_status = (F.when(F.col("distinct_valid_values") > 1, "conflicting_duplicate")
        .when(F.col("valid_records") > 0, "valid")
        .when(F.col("missing_records") > 0, "missing_amount")
        .otherwise("invalid_only"))
    stats = stats.withColumn("pair_status", pair_status)
    annotated = frame.join(stats, keys, "left")
    usable = annotated.filter(F.col("pair_status").isin("valid", "missing_amount")
                             & F.col("amount_status").isin("valid", "missing_amount"))
    ordering = Window.partitionBy(*keys).orderBy(
        F.when(F.col("amount_status") == "valid", 0).otherwise(1), "id", "amount_raw", "derivation_id")
    clean = usable.withColumn("_rank", F.row_number().over(ordering)).filter(F.col("_rank") == 1).drop("_rank")
    quarantine = (annotated
        .withColumn("quarantine_reason", F.when(F.col("pair_status") == "conflicting_duplicate", "conflicting_duplicate")
                     .otherwise(F.col("amount_status")))
        .filter((F.col("pair_status") == "conflicting_duplicate")
                | ~F.col("amount_status").isin("valid", "missing_amount")))
    return clean, quarantine, stats


def add_basis(clean, beverages, nutrients, reference, relative_tolerance=0.01, absolute_tolerance=0.005,
              review_limits=None):
    from pyspark.sql import functions as F

    products = beverages.select("fdc_id", "beverage_group", "serving_size",
                                F.col("serving_size_unit_normalized").alias("serving_unit"))
    frame = (clean.join(products, "fdc_id", "left").join(nutrients, "nutrient_id", "left")
             .join(reference, ["fdc_id", "nutrient_id"], "left"))
    same_serving = (F.col("serving_size").isNotNull()
                    & (F.abs(F.col("serving_size") - F.col("json_serving_size")) <= F.lit(1e-8)))
    amount_tolerance = F.greatest(F.lit(absolute_tolerance),
                                  F.greatest(F.abs(F.col("amount")), F.abs(F.col("json_amount"))) * relative_tolerance)
    same_amount = F.abs(F.col("amount") - F.col("json_amount")) <= amount_tolerance
    status = (F.when(F.col("amount_status") == "missing_amount", "amount_missing")
        .when(F.col("record_reference_status").isNull(), "json_record_unavailable")
        .when(F.col("serving_unit").isNull(), "unrecognized_serving_unit")
        .when(F.col("json_serving_unit") != F.col("serving_unit"), "serving_unit_mismatch")
        .when(~same_serving, "serving_size_mismatch")
        .when(F.col("record_reference_status") != "scale_consistent", F.col("record_reference_status"))
        .when(F.col("json_amount").isNull(), "nutrient_absent_in_json")
        .when(~same_amount, "csv_json_amount_mismatch")
        .otherwise("corroborated_export_label_scaling"))
    frame = frame.withColumn("basis_status", status)
    candidate = (F.when(F.col("serving_unit") == "ml", "per_100_ml")
                  .when(F.col("serving_unit") == "g", "per_100_g"))
    limits = []
    for nutrient_id, limit in (review_limits or {}).items():
        limits.extend([F.lit(int(nutrient_id)), F.lit(float(limit))])
    limit_column = F.create_map(*limits)[F.col("nutrient_id")] if limits else F.lit(None).cast("double")
    return (frame.withColumn("measurement_basis_candidate", candidate)
        .withColumn("measurement_basis", F.when(F.col("basis_status") == "corroborated_export_label_scaling", candidate))
        .withColumn("value_review_limit", limit_column)
        .withColumn("value_review_required", F.when((F.col("amount_status") == "valid")
                                                     & (F.col("amount") > limit_column), True).otherwise(False))
        .withColumn("value_review_reason", F.when(F.col("value_review_required"), "above_configured_review_limit")
                     .otherwise("not_flagged"))
        .withColumn("eligible_for_comparison", (F.col("amount_status") == "valid")
                     & (F.col("basis_status") == "corroborated_export_label_scaling")
                     & ~F.col("value_review_required")))


def save_frame(spark, frame, artifacts, name, keys, expected_rows=None, root=ROOT):
    partial = artifacts / f"{name}.csv.partial"
    written, preview = write_csv(frame, partial)
    readback = spark.read.schema(frame.schema).options(**CSV_OPTIONS).csv(partial.as_posix())
    measured = measure_input(readback, keys)
    if written != measured["row_count"] or any(measured["null_key_counts"].values()):
        raise ValueError(f"Readback failed for {name}")
    if expected_rows is not None and written != expected_rows:
        raise ValueError(f"Unexpected row count for {name}")
    path = artifacts / f"{name}.csv"
    partial.rename(path)
    schema_path = artifacts / f"{name}_schema.json"
    write_json(schema_path, frame.schema.jsonValue())
    metadata = {"path": path.relative_to(root).as_posix(), "sha256": sha256(path),
                "schema_path": schema_path.relative_to(root).as_posix(), "schema_sha256": sha256(schema_path),
                "row_count": written, "column_count": len(frame.columns), "size_bytes": path.stat().st_size,
                "readback_metrics": measured, "format": "CSV", "write_mode": "new directory per run"}
    final_frame = spark.read.schema(frame.schema).options(**CSV_OPTIONS).csv(path.as_posix())
    return metadata, final_frame, preview


def write_report(folder, evidence):
    lines = ["# Kiểm tra dữ liệu dinh dưỡng Checkpoint 3 Task 3", "",
             f"Trạng thái {evidence['status']}. Snapshot `{evidence.get('snapshot_run_id', '?')}`.", "",
             "Một khóa kết quả là (fdc_id, nutrient_id). Giá trị thiếu giữ null. Giá trị 0 hợp lệ được giữ.",
             "Trạng thái amount valid xác nhận kiểu số hữu hạn và không âm. Giá trị lớn được gắn cờ riêng để xem xét.",
             "Ưu tiên giá trị hợp lệ hơn giá trị thiếu và chọn id nhỏ nhất khi giá trị không xung đột.",
             "Nhiều giá trị hợp lệ khác nhau trong cùng khóa được đưa toàn bộ vào quarantine.", "",
             "| Chất dinh dưỡng | Hồ sơ đồ uống | Có giá trị hợp lệ | Amount thiếu | Không có record | Xung đột | Chỉ có record lỗi |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in evidence.get("coverage", []):
        lines.append(f"| {row['name']} | {row['expected_beverages']} | {row['valid']} | {row['missing_amount']} | "
                     f"{row['no_source_record']} | {row['conflicting_duplicate']} | {row['invalid_only']} |")
    lines.extend(["", f"Record dinh dưỡng đủ điều kiện so sánh {evidence.get('eligible_nutrient_rows', '?')}.", "",
                  "| Trạng thái cơ sở đo | Số record dinh dưỡng |", "| --- | ---: |"])
    basis_totals = {}
    for row in evidence.get("basis_counts", []):
        basis_totals[row["basis_status"]] = basis_totals.get(row["basis_status"], 0) + row["count"]
    for status, count in sorted(basis_totals.items()):
        lines.append(f"| {status} | {count} |")
    lines.extend(["", "Cơ sở đo được kiểm tra nhất quán với labelNutrients và khẩu phần trong ZIP JSON cùng release.",
                  "Ngưỡng CSV/JSON và phép tính từ khẩu phần dùng sai số tương đối 1% hoặc tuyệt đối 0,005 đơn vị.",
                  "Chỉ gắn measurement_basis khi có ít nhất một giá trị dương kiểm tra được, mọi anchor khớp ngưỡng,",
                  "và unit/serving/amount của record hiện tại khớp giữa CSV và JSON. Zero-only không tạo anchor.",
                  "Đây là kiểm tra tính nhất quán giữa các trường của bản xuất USDA. Các trường hợp chưa đủ bằng chứng giữ basis_status riêng.",
                  "Dữ liệu dùng để so sánh cần eligible_for_comparison=true và cùng measurement_basis.",
                  "Các ngưỡng giá trị lớn là quy tắc sàng lọc để rà soát, được lưu trong config và evidence; dữ liệu nguồn được giữ.", "",
                  "Nguồn https://fdc.nal.usda.gov/GBFPD_Documentation/", ""])
    for name in ("clean_output", "quarantine_output", "value_review_output", "classified_output"):
        output = evidence.get(name, {})
        lines.extend([f"{name} `{output.get('path', '?')}`, {output.get('row_count', '?')} dòng.", ""])
    lines.append(f"Runtime riêng Task 3 {evidence['elapsed_seconds']} giây.")
    if evidence.get("error"):
        lines.append("Có lỗi. Xem error.log trước khi sử dụng output.")
    (folder / "nutrient_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    add_stage_arguments(parser)
    parser.add_argument("--input-handoff", type=Path, help="Override the pinned Task 2 handoff JSON")
    args = parser.parse_args(argv)
    started = time.perf_counter()
    run_id, folder, artifacts = create_run("task3")
    evidence = {"status": "FAIL", "run_id": run_id, "started_at_utc": utc_now(),
                "duplicate_policy": "prefer valid over missing; smallest id for same value; quarantine conflicting valid values"}
    spark = None
    print("CHECKPOINT 3 | TASK 3 | CHẤT LƯỢNG DINH DƯỠNG", flush=True)
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType

        config = load_config()
        task = load_config(args.task_config or ROOT / "config/checkpoint3.json")["task3"]
        handoff_path = (ROOT / (args.input_handoff or task["input_handoff_path"])).resolve()
        handoff_hash = sha256(handoff_path)
        handoff, result, contract = load_handoff(handoff_path, config)
        tables = {table["file_name"]: table for table in contract["tables"]}
        evidence.update(snapshot_run_id=contract["snapshot_run_id"], input_handoff_path=str(handoff_path),
                        input_handoff_sha256=handoff_hash, requested_nutrients=task["nutrients"])
        nutrient_ids = [row["id"] for row in task["nutrients"]]
        if not nutrient_ids or len(set(nutrient_ids)) != len(nutrient_ids):
            raise ValueError("Requested nutrient IDs must be nonempty and distinct")
        if {int(key) for key in task["value_review_limits"]} != set(nutrient_ids):
            raise ValueError("Every requested nutrient needs an explicit large-value review limit")
        evidence["value_review_limits"] = task["value_review_limits"]
        spark = start_spark("beverage-checkpoint3-clean-nutrients")
        spark.sparkContext.setLogLevel("ERROR")
        evidence["environment"] = {"python": platform.python_version(), "spark": spark.version,
                                   "master": spark.sparkContext.master}
        output = handoff["beverages"]
        schema = StructType.fromJson(load_config(ROOT / output["schema_path"]))
        beverages = spark.read.schema(schema).options(**CSV_OPTIONS).csv((ROOT / output["path"]).as_posix())
        beverage_check = measure_input(beverages, ["fdc_id"])
        beverage_ids = beverages.select("fdc_id")
        beverage_count = beverage_ids.distinct().count()
        if (beverage_count != output["row_count"] or beverage_check["row_count"] != beverage_count
                or any(beverage_check["null_key_counts"].values())
                or beverages.filter(F.col("selection_status") != "include").limit(1).count()):
            raise ValueError("Task 2 beverage IDs or selection status are invalid")
        nutrients = read_locked_csv(spark, tables["nutrient.csv"]).filter(F.col("id").isin(nutrient_ids))
        actual = {row.id: row.asDict() for row in nutrients.collect()}
        if len(actual) != len(nutrient_ids) or nutrients.count() != len(nutrient_ids):
            raise ValueError("Requested nutrient IDs are absent or duplicated")
        for expected in task["nutrients"]:
            if any(str(actual[expected["id"]][key]).upper() != str(expected[key]).upper() for key in ("name", "unit_name")):
                raise ValueError("Nutrient name/unit differs from the configured contract")
        nutrient_metadata = nutrients.select(F.col("id").alias("nutrient_id"),
                                              F.col("name").alias("nutrient_name"), "unit_name")

        print("[RAW] Đọc toàn bộ food_nutrient, giữ amount gốc để kiểm tra...", flush=True)
        raw_table = copy.deepcopy(tables["food_nutrient.csv"])
        raw_table["schema_declaration"]["types"] = {}
        parsed = classify_amounts(read_locked_csv(spark, raw_table))
        raw_check = parsed.agg(F.count("*").alias("rows"),
                               F.sum((F.col("amount_status") == "invalid_key").cast("long")).alias("invalid_keys")).first().asDict()
        evidence["raw_check"] = raw_check
        if raw_check["rows"] != raw_table["row_count"] or raw_check["invalid_keys"]:
            raise ValueError("Raw row count changed or raw nutrient keys are invalid")
        scoped = parsed.join(beverage_ids, "fdc_id", "left_semi").filter(F.col("nutrient_id").isin(nutrient_ids))
        if args.collect_baseline:
            record_spark_baseline(spark, scoped, folder, "nutrient_scope")
        print("[SCOPE] Lưu record dinh dưỡng thuộc hồ sơ đồ uống và năm chỉ số...", flush=True)
        classified_meta, classified, _ = save_frame(spark, scoped, artifacts, "nutrients_classified",
                                                   ["id", "fdc_id", "nutrient_id"])
        evidence["classified_output"] = classified_meta
        amount_counts = [row.asDict() for row in classified.groupBy("nutrient_id", "amount_status").count()
                         .orderBy("nutrient_id", "amount_status").collect()]
        write_json(folder / "amount_quality.json", amount_counts)
        clean, quarantine, pair_stats = resolve_pairs(classified)
        pair_counts = [row.asDict() for row in pair_stats.groupBy("nutrient_id", "pair_status").count().collect()]
        pair_summary = pair_stats.agg(
            F.sum((F.col("source_records") > 1).cast("long")).alias("duplicate_pairs"),
            F.sum((F.col("pair_status") == "conflicting_duplicate").cast("long")).alias("conflicting_pairs"),
        ).first().asDict()
        evidence["duplicate_summary"] = pair_summary
        coverage = []
        for requested in task["nutrients"]:
            counts = {status: 0 for status in ("valid", "missing_amount", "conflicting_duplicate", "invalid_only")}
            for row in pair_counts:
                if row["nutrient_id"] == requested["id"]:
                    counts[row["pair_status"]] += row["count"]
            coverage.append(dict(requested, expected_beverages=beverage_count,
                                 no_source_record=beverage_count - sum(counts.values()), **counts))
        evidence["coverage"] = coverage
        clean_rows = sum(row["valid"] + row["missing_amount"] for row in coverage)
        print(f"[QUALITY] {classified_meta['row_count']:,} record nguồn | {pair_summary}", flush=True)
        quarantine_meta, _, quarantine_preview = save_frame(spark, quarantine.orderBy("fdc_id", "nutrient_id", "id"),
                                                            artifacts, "nutrients_quarantine", ["id", "fdc_id", "nutrient_id"])
        evidence["quarantine_output"] = quarantine_meta
        write_json(folder / "quarantine_preview.json", quarantine_preview)

        print("[BASIS] Đối chiếu ZIP JSON local với khẩu phần và labelNutrients...", flush=True)
        zip_path = (ROOT / task["basis_reference_zip"]).resolve()
        zip_hash = sha256(zip_path)
        if zip_hash != task["basis_reference_sha256"]:
            raise ValueError("JSON reference archive differs from the source-preservation manifest")
        aliases = result["policy"]["serving_unit_aliases"]
        id_set = {row.fdc_id for row in beverage_ids.toLocalIterator()}
        reference_partial = artifacts / "label_reference.csv.partial"
        reference_info = write_json_reference(zip_path, reference_partial, id_set, nutrient_ids, aliases,
                                              task["basis_relative_tolerance"], task["basis_absolute_tolerance"])
        reference_schema = ("fdc_id long, nutrient_id long, json_amount double, label_amount double, "
                            "json_serving_size double, json_serving_unit string, record_reference_status string, "
                            "scale_checks long, scale_mismatches long")
        reference = spark.read.schema(reference_schema).options(**CSV_OPTIONS).csv(reference_partial.as_posix())
        if reference.count() != reference_info["reference_rows"]:
            raise ValueError("JSON reference row count is inconsistent")
        reference_path = artifacts / "label_reference.csv"
        reference_partial.rename(reference_path)
        reference = spark.read.schema(reference_schema).options(**CSV_OPTIONS).csv(reference_path.as_posix())
        evidence["basis_reference"] = dict(reference_info, zip_path=str(zip_path), zip_sha256=zip_hash,
                                            reference_path=reference_path.relative_to(ROOT).as_posix(),
                                            reference_sha256=sha256(reference_path))
        enriched = add_basis(clean, beverages, nutrient_metadata, reference, task["basis_relative_tolerance"],
                             task["basis_absolute_tolerance"], task["value_review_limits"])
        if args.collect_baseline:
            record_spark_baseline(spark, enriched, folder, "nutrition_clean")
        print("[OUTPUT] Ghi dữ liệu đã xử lý và kiểm tra lại bằng Spark...", flush=True)
        clean_meta, cleaned, preview = save_frame(spark, enriched.orderBy("fdc_id", "nutrient_id"), artifacts,
                                                 "nutrients_clean", ["id", "fdc_id", "nutrient_id"], clean_rows)
        if cleaned.select("fdc_id", "nutrient_id").distinct().count() != clean_rows:
            raise ValueError("Clean output is not unique on (fdc_id, nutrient_id)")
        evidence["clean_output"] = clean_meta
        write_json(folder / "preview.json", preview)
        review_meta, _, review_preview = save_frame(spark, cleaned.filter(F.col("value_review_required"))
                                                   .orderBy("nutrient_id", F.desc("amount"), "fdc_id"), artifacts,
                                                   "nutrients_value_review", ["id", "fdc_id", "nutrient_id"])
        evidence["value_review_output"] = review_meta
        write_json(folder / "value_review_preview.json", review_preview)
        evidence["basis_counts"] = [row.asDict() for row in cleaned.groupBy(
            "nutrient_id", "basis_status", "measurement_basis", "eligible_for_comparison", "value_review_required"
        ).count().orderBy("nutrient_id", "basis_status").collect()]
        evidence["value_ranges"] = [row.asDict() for row in cleaned.filter(F.col("amount_status") == "valid")
                                    .groupBy("nutrient_id").agg(F.min("amount").alias("min_amount"),
                                                                F.max("amount").alias("max_amount")).collect()]
        removed = classified_meta["row_count"] - clean_meta["row_count"] - quarantine_meta["row_count"]
        if removed < 0:
            raise ValueError("Output counts do not reconcile with scoped input")
        evidence["redundant_non_conflicting_records_removed"] = removed
        eligible = sum(row["count"] for row in evidence["basis_counts"] if row["eligible_for_comparison"])
        valid_rows = sum(row["valid"] for row in coverage)
        corroborated = sum(row["count"] for row in evidence["basis_counts"]
                           if row["basis_status"] == "corroborated_export_label_scaling")
        evidence.update(basis_audit_complete=True, eligible_nutrient_rows=eligible,
                        all_valid_rows_basis_corroborated=corroborated == valid_rows)
        for table in contract["tables"]:
            verify_raw(table)
        if sha256(zip_path) != zip_hash or sha256(handoff_path) != handoff_hash:
            raise ValueError("Reference archive or input handoff changed during Task 3")
        if sha256(ROOT / output["path"]) != output["sha256"]:
            raise ValueError("Task 2 beverage CSV changed during Task 3")
        evidence.update(status="PASS", raw_integrity_verified_before_and_after=True)
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
        write_json(folder / "nutrient_result.json", evidence)
        write_report(folder, evidence)
    publish_stage_result(folder / "nutrient_result.json", args.result_pointer)
    print(f"[KẾT QUẢ] {evidence['status']} | {evidence['elapsed_seconds']:.1f} s", flush=True)
    print(f"[EVIDENCE] {folder.relative_to(ROOT).as_posix()}", flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
