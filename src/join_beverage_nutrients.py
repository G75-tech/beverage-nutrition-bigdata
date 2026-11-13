"""Join pinned beverage metadata with cleaned nutrient records and audit coverage."""
from __future__ import annotations

import argparse
from pathlib import Path
import platform
import sys
import time
import traceback

from src.common import ROOT, load_config, sha256, start_spark, utc_now, write_json
from src.run_storage import create_run
from src.common import add_stage_arguments, publish_stage_result, record_spark_baseline
from src.clean_nutrients import load_handoff, resolve_pairs, save_frame
from src.input_contract import CSV_OPTIONS, measure_input, read_locked_csv, verify_raw


KEYS = ["fdc_id", "nutrient_id"]
PRODUCT_COLUMNS = [
    "fdc_id", "description", "brand_owner", "brand_name", "gtin_upc",
    "branded_food_category", "beverage_group", "serving_size", "serving_size_unit",
    "serving_size_unit_normalized", "serving_size_unit_status", "preparation_status",
    "food_publication_date", "market_country",
]


def verify_artifact(metadata, root=ROOT):
    root = Path(root).resolve()
    for name, checksum in (("path", "sha256"), ("schema_path", "schema_sha256")):
        path = (root / metadata[name]).resolve()
        if not path.is_relative_to(root / "artifacts") or sha256(path) != metadata[checksum]:
            raise ValueError(f"Artifact changed or is outside artifacts: {metadata[name]}")


def load_inputs(task_config, project_config, root=ROOT):
    root = Path(root).resolve()
    task = task_config["task4"]
    path = (root / task["input_result_path"]).resolve()
    if sha256(path) != task["input_result_sha256"]:
        raise ValueError("Pinned Task 3 result changed")
    result = load_config(path)
    if result["status"] != "PASS" or not result["raw_integrity_verified_before_and_after"]:
        raise ValueError("Task 3 result has not passed its integrity checks")
    if (task_config["task3"]["result_sha256"] != task["input_result_sha256"]
            or (root / task_config["task3"]["result_path"]).resolve() != path):
        raise ValueError("Task 4 and Task 3 result pins differ")
    handoff_path = (root / result["input_handoff_path"]).resolve()
    if sha256(handoff_path) != result["input_handoff_sha256"]:
        raise ValueError("Task 2 handoff changed after Task 3")
    handoff, task2, contract = load_handoff(handoff_path, project_config, root)
    if result["snapshot_run_id"] != contract["snapshot_run_id"]:
        raise ValueError("Task 3 belongs to another raw snapshot")
    if result["requested_nutrients"] != task_config["task3"]["nutrients"]:
        raise ValueError("Requested nutrients changed after the pinned Task 3 run")
    for key in ("clean_output", "classified_output"):
        verify_artifact(result[key], root)
    return path, result, handoff, task2, contract


def key_profile(frame, keys):
    from functools import reduce
    from pyspark.sql import functions as F

    grouped = frame.groupBy(*keys).count()
    missing = reduce(lambda a, b: a | b, [F.col(key).isNull() for key in keys])
    row = grouped.agg(
        F.coalesce(F.sum("count"), F.lit(0)).alias("row_count"),
        F.count("*").alias("distinct_key_tuples"),
        F.coalesce(F.sum(F.when(missing, F.col("count")).otherwise(0)), F.lit(0)).alias("null_key_rows"),
        F.coalesce(F.sum((F.col("count") > 1).cast("long")), F.lit(0)).alias("duplicate_keys"),
        F.coalesce(F.sum(F.greatest(F.col("count") - 1, F.lit(0))), F.lit(0)).alias("duplicate_extra_rows"),
    ).first().asDict()
    row["key_types"] = {key: frame.schema[key].dataType.simpleString() for key in keys}
    return row


def require_unique(frame, keys, label):
    profile = key_profile(frame, keys)
    if profile["null_key_rows"] or profile["duplicate_keys"]:
        raise ValueError(f"{label}: null or duplicate keys: {profile}")
    return profile


def audited_left_join(left, right, keys, label):
    """Require a unique right side so a left join cannot multiply left rows."""
    from pyspark.sql import functions as F

    left_profile = key_profile(left, keys)
    right_profile = require_unique(right, keys, label + " right")
    if left_profile["null_key_rows"] or left_profile["key_types"] != right_profile["key_types"]:
        raise ValueError(f"{label}: null left keys or different key types")
    overlaps = (set(left.columns) & set(right.columns)) - set(keys)
    if overlaps:
        raise ValueError(f"{label}: ambiguous payload columns: {sorted(overlaps)}")
    marker = "_task4_right_present"
    if marker in left.columns or marker in right.columns:
        raise ValueError("Reserved join marker already exists")
    joined = left.join(right.withColumn(marker, F.lit(True)), keys, "left")
    counts = joined.agg(F.count("*").alias("output_rows"),
                        F.coalesce(F.sum(F.col(marker).isNotNull().cast("long")), F.lit(0)).alias("matched_left_rows"),
                        F.coalesce(F.sum(F.col(marker).isNull().cast("long")), F.lit(0)).alias("unmatched_left_rows")).first().asDict()
    if counts["output_rows"] != left_profile["row_count"]:
        raise ValueError(f"{label}: left join multiplied rows")
    left_keys = left.select(*keys).distinct()
    counts["matched_left_keys"] = left_keys.join(right.select(*keys), keys, "left_semi").count()
    counts["unmatched_right_keys"] = right.select(*keys).join(left_keys, keys, "left_anti").count()
    audit = {"name": label, "join_type": "left", "keys": keys,
             "left": left_profile, "right": right_profile, **counts}
    return joined.drop(marker), audit


def validate_scope(clean, pair_stats, products, nutrient_dimension):
    require_unique(clean, KEYS, "Task 3 clean")
    require_unique(pair_stats, KEYS, "Task 3 source pairs")
    require_unique(products, ["fdc_id"], "Task 2 beverages")
    for label, frame in (("clean", clean), ("source pairs", pair_stats)):
        for key, dimension in (("fdc_id", products), ("nutrient_id", nutrient_dimension)):
            if frame.select(key).distinct().join(dimension.select(key), key, "left_anti").limit(1).count():
                raise ValueError(f"Task 3 {label} contains an out-of-scope {key}")


def build_fact(products, nutrient_dimension, clean, pair_stats):
    from pyspark.sql import functions as F

    validate_scope(clean, pair_stats, products, nutrient_dimension)
    # Product metadata and nutrient definitions are reattached from their locked dimensions.
    redundant = {"beverage_group", "serving_size", "serving_unit", "nutrient_name", "unit_name",
                 "source_records", "distinct_valid_values", "valid_records", "missing_records", "pair_status"}
    payload = clean.drop(*sorted(redundant)).withColumnRenamed("id", "food_nutrient_id")
    enriched, nutrient_audit = audited_left_join(payload, nutrient_dimension, ["nutrient_id"], "clean_to_nutrient")
    if nutrient_audit["unmatched_left_rows"]:
        raise ValueError("Clean nutrient has no dictionary definition")
    enriched, product_audit = audited_left_join(enriched, products.select("fdc_id"), ["fdc_id"], "clean_to_beverage")
    if product_audit["unmatched_left_rows"]:
        raise ValueError("Clean nutrient has no selected beverage")
    enriched = enriched.drop("nutrient_name", "unit_name")
    # Explicit requested slots retain an absent nutrient for every selected beverage.
    grid = products.crossJoin(F.broadcast(nutrient_dimension))
    grid_profile = require_unique(grid, KEYS, "beverage x requested nutrient slots")
    source = pair_stats.withColumnRenamed("pair_status", "nutrient_record_status")
    annotated, source_audit = audited_left_join(grid, source, KEYS, "slots_to_source_pair_status")
    annotated = annotated.withColumn("nutrient_record_status", F.coalesce("nutrient_record_status", F.lit("no_source_record")))
    joined, clean_audit = audited_left_join(annotated, enriched.withColumn("clean_record_present", F.lit(True)), KEYS, "slots_to_clean_nutrient")
    joined = (joined
        .withColumn("clean_record_present", F.coalesce("clean_record_present", F.lit(False)))
        .withColumn("source_record_present", F.col("nutrient_record_status") != "no_source_record")
        .withColumn("source_records", F.coalesce("source_records", F.lit(0)))
        .withColumn("eligible_for_comparison", F.coalesce("eligible_for_comparison", F.lit(False)))
        .withColumn("value_review_required", F.coalesce("value_review_required", F.lit(False)))
        .withColumn("amount_status", F.when(~F.col("clean_record_present"), F.col("nutrient_record_status")).otherwise(F.col("amount_status")))
        .withColumn("basis_status", F.when(~F.col("clean_record_present"), F.lit("no_clean_record")).otherwise(F.col("basis_status")))
        .withColumn("value_review_reason", F.when(~F.col("clean_record_present"), F.lit("no_clean_record")).otherwise(F.col("value_review_reason"))))
    expected_clean = F.col("nutrient_record_status").isin("valid", "missing_amount")
    if joined.filter(F.col("clean_record_present") != expected_clean).limit(1).count():
        raise ValueError("Source pair status and clean record presence disagree")
    columns = products.columns + ["nutrient_id", "nutrient_name", "unit_name"]
    columns += [name for name in joined.columns if name not in columns]
    return joined.select(*columns), {
        "grid": grid_profile,
        "joins": [nutrient_audit, product_audit, source_audit, clean_audit],
    }


def coverage_table(fact, dimensions):
    from pyspark.sql import functions as F

    def count_if(condition, name):
        return F.sum(condition.cast("long")).alias(name)

    result = fact.groupBy(*dimensions).agg(
        F.count("*").alias("expected_beverages"),
        count_if(F.col("source_record_present"), "with_source_record"),
        count_if(F.col("clean_record_present"), "with_clean_record"),
        count_if(F.col("amount_status") == "valid", "numeric_valid"),
        count_if(F.col("nutrient_record_status") == "missing_amount", "missing_amount"),
        count_if(F.col("nutrient_record_status") == "no_source_record", "no_source_record"),
        count_if(F.col("nutrient_record_status") == "conflicting_duplicate", "conflicting_duplicate"),
        count_if(F.col("nutrient_record_status") == "invalid_only", "invalid_only"),
        count_if(F.col("value_review_required"), "value_review_required"),
        count_if(F.col("amount_status").eqNullSafe("valid") & F.col("measurement_basis").isNull(), "numeric_valid_basis_unconfirmed"),
        count_if(F.col("eligible_for_comparison"), "eligible_for_comparison"),
    )
    return (result.withColumn("numeric_coverage_pct", F.round(F.col("numeric_valid") * 100.0 / F.col("expected_beverages"), 4))
            .withColumn("eligible_coverage_pct", F.round(F.col("eligible_for_comparison") * 100.0 / F.col("expected_beverages"), 4))
            .orderBy(*dimensions))


def validate_fact(fact, expected_products, expected_slots, expected_clean, expected_eligible):
    from pyspark.sql import functions as F

    profile = require_unique(fact, KEYS, "Task 4 output")
    metrics = fact.agg(
        F.countDistinct("fdc_id").alias("beverages"),
        F.sum(F.col("clean_record_present").cast("long")).alias("clean_records"),
        F.sum(F.col("eligible_for_comparison").cast("long")).alias("eligible_records"),
        F.sum((~F.col("source_record_present")).cast("long")).alias("no_source_record"),
        F.sum(F.col("value_review_required").cast("long")).alias("value_review_records"),
    ).first().asDict()
    if (profile["row_count"] != expected_slots or metrics["beverages"] != expected_products
            or metrics["clean_records"] != expected_clean or metrics["eligible_records"] != expected_eligible):
        raise ValueError(f"Task 4 counts differ from pinned inputs: {profile}, {metrics}")
    invalid_eligible = F.col("eligible_for_comparison") & (
        ~F.col("clean_record_present") | F.col("amount").isNull() | F.isnan("amount")
        | F.col("amount").isin(float("inf"), float("-inf")) | (F.col("amount") < 0)
        | F.col("value_review_required") | F.col("measurement_basis").isNull()
        | ~F.col("amount_status").eqNullSafe("valid"))
    if fact.filter(invalid_eligible).limit(1).count():
        raise ValueError("An ineligible or incomplete value is marked eligible")
    if fact.filter(~F.col("clean_record_present") & F.col("amount").isNotNull()).limit(1).count():
        raise ValueError("A missing clean record was filled with an amount")
    return {"key_profile": profile, **metrics}


def write_report(folder, evidence):
    lines = ["# Checkpoint 3 Task 4: nối bảng và đo chất lượng", "",
             f"Trạng thái: {evidence['status']}. Snapshot: `{evidence.get('snapshot_run_id', '?')}`.", "",
             "Grain: một dòng cho mỗi (fdc_id, nutrient_id) trong danh sách đồ uống × năm chỉ số đã chọn.",
             "Các vị trí chưa có dữ liệu giữ amount=null, có nutrient_record_status; giá trị 0 hợp lệ được giữ.",
             "Tên/mô tả đã được nối từ food.csv ở Task 2, tiếp tục dùng qua CSV/schema được xác minh SHA-256.",
             "Các phép nối trái kiểm tra kiểu khóa, null, khóa trùng phía phải và số dòng trước/sau.", "",
             "| Phép nối | Khóa | Trái trước | Phải trước | Sau | Khớp trái | Chưa khớp trái | Khóa phải chưa dùng |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in evidence.get("join_audit", {}).get("joins", []):
        lines.append(f"| {row['name']} | {', '.join(row['keys'])} | {row['left']['row_count']} | "
                     f"{row['right']['row_count']} | {row['output_rows']} | {row['matched_left_rows']} | "
                     f"{row['unmatched_left_rows']} | {row['unmatched_right_keys']} |")
    metrics = evidence.get("output_metrics", {})
    lines.extend(["", f"Số hồ sơ đồ uống được giữ: {metrics.get('beverages', '?')}.",
                  f"Số vị trí chỉ số: {metrics.get('key_profile', {}).get('row_count', '?')}.",
                  f"Có record clean: {metrics.get('clean_records', '?')}; chưa có record nguồn: {metrics.get('no_source_record', '?')}.",
                  f"Đủ điều kiện so sánh: {metrics.get('eligible_records', '?')}; cần rà soát giá trị: {metrics.get('value_review_records', '?')}.", "",
                  "| Chỉ số | Hồ sơ đồ uống | Có số hợp lệ | Chưa có record | Đủ điều kiện so sánh | Độ phủ đủ điều kiện (%) |",
                  "| --- | ---: | ---: | ---: | ---: | ---: |"])
    for row in evidence.get("coverage_by_nutrient", []):
        lines.append(f"| {row['nutrient_name']} | {row['expected_beverages']} | {row['numeric_valid']} | "
                     f"{row['no_source_record']} | {row['eligible_for_comparison']} | {row['eligible_coverage_pct']} |")
    lines.extend(["", "Cột brand_name có thể thiếu; description được giữ từ nguồn và có thể là mô tả chung.",
                  "eligible_for_comparison kế thừa quy tắc Task 3, chưa xác minh độc lập toàn bộ cơ sở đo.",
                  "Khi tính KPI phải lọc eligible_for_comparison=true, nhóm cùng nutrient_id, unit_name và measurement_basis.",
                  "Giữ mẫu số toàn bộ hồ sơ của mỗi nhóm/chỉ số để báo độ phủ; tập đủ điều kiện có thể không đại diện toàn bộ nhóm.",
                  "Các dòng bị gắn cờ và thiếu bằng chứng cơ sở đo vẫn nằm trong bảng đầy đủ.", ""])
    if "output" in evidence:
        lines.extend([f"CSV chính: `{evidence['output']['path']}`.", ""])
    lines.append(f"Runtime riêng Task 4: {evidence.get('elapsed_seconds', '?')} giây.")
    (folder / "join_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    add_stage_arguments(parser)
    args = parser.parse_args(argv)
    started = time.perf_counter()
    run_id, folder, artifacts = create_run("task4")
    evidence = {"status": "FAIL", "run_id": run_id, "started_at_utc": utc_now(),
                "grain": KEYS, "missing_value_policy": "retain null and explicit source/clean presence flags"}
    spark = None
    handoff = None
    print("CHECKPOINT 3 | TASK 4 | JOIN VÀ ĐO CHẤT LƯỢNG", flush=True)
    try:
        from pyspark import StorageLevel
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType
        from pyspark.sql.window import Window

        config = load_config()
        task_config = load_config(args.task_config or ROOT / "config/checkpoint3.json")
        task3_path, task3, handoff, task2, contract = load_inputs(task_config, config)
        tables = {row["file_name"]: row for row in contract["tables"]}
        evidence.update(snapshot_run_id=contract["snapshot_run_id"],
                        task3_result_path=task3_path.relative_to(ROOT).as_posix(), task3_result_sha256=sha256(task3_path),
                        input_handoff_path=task3["input_handoff_path"], input_handoff_sha256=task3["input_handoff_sha256"],
                        requested_nutrients=task3["requested_nutrients"],
                        food_description_lineage="Reused Task 2 food.csv left join, verified through pinned Task 2 output and handoff")
        spark = start_spark("beverage-checkpoint3-join")
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
        evidence["environment"] = {"python": platform.python_version(), "spark": spark.version,
                                   "master": spark.sparkContext.master}

        def read_artifact(metadata):
            frame = spark.read.schema(StructType.fromJson(load_config(ROOT / metadata["schema_path"]))).options(**CSV_OPTIONS).csv((ROOT / metadata["path"]).as_posix())
            measured = measure_input(frame, ["fdc_id"])
            if measured["row_count"] != metadata["row_count"] or any(measured["null_key_counts"].values()):
                raise ValueError("Input artifact count or keys changed")
            return frame

        print("[INPUT] Xác minh bản Task 2–3, đọc CSV theo schema đã chốt...", flush=True)
        beverages = read_artifact(handoff["beverages"])
        if beverages.filter(~F.col("selection_status").eqNullSafe("include")).limit(1).count():
            raise ValueError("Selected beverage input contains a non-include record")
        products = beverages.select(*PRODUCT_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)
        product_count = require_unique(products, ["fdc_id"], "selected beverages")["row_count"]
        clean = read_artifact(task3["clean_output"]).persist(StorageLevel.MEMORY_AND_DISK)
        classified = read_artifact(task3["classified_output"])
        _, _, pair_stats = resolve_pairs(classified)
        pair_stats = pair_stats.persist(StorageLevel.MEMORY_AND_DISK)
        nutrient_ids = [row["id"] for row in task3["requested_nutrients"]]
        dictionary = read_locked_csv(spark, tables["nutrient.csv"])
        evidence["nutrient_dictionary_keys"] = require_unique(dictionary, ["id"], "nutrient.csv")
        nutrient_dimension = dictionary.filter(F.col("id").isin(nutrient_ids)).select(
            F.col("id").alias("nutrient_id"), F.col("name").alias("nutrient_name"), "unit_name")
        actual_nutrients = {row.nutrient_id: row.asDict() for row in nutrient_dimension.collect()}
        if len(actual_nutrients) != len(nutrient_ids):
            raise ValueError("Requested nutrient dictionary entries are absent")
        for expected in task3["requested_nutrients"]:
            actual = actual_nutrients[expected["id"]]
            if actual["nutrient_name"] != expected["name"] or actual["unit_name"] != expected["unit_name"]:
                raise ValueError("Requested nutrient name/unit differs from the locked dictionary")
        metadata_mismatch = clean.join(nutrient_dimension.withColumnRenamed("nutrient_name", "_dictionary_name")
                                       .withColumnRenamed("unit_name", "_dictionary_unit"), "nutrient_id", "left")
        if metadata_mismatch.filter(~F.col("nutrient_name").eqNullSafe(F.col("_dictionary_name"))
                                    | ~F.col("unit_name").eqNullSafe(F.col("_dictionary_unit"))).limit(1).count():
            raise ValueError("Task 3 nutrient metadata differs from nutrient.csv")
        print("[JOIN] Kiểm tra khóa và tạo đầy đủ vị trí đồ uống × năm chỉ số...", flush=True)
        fact, audit = build_fact(products, nutrient_dimension, clean, pair_stats)
        if args.collect_baseline:
            record_spark_baseline(spark, fact, folder, "beverage_nutrient_join")
        evidence["join_audit"] = audit
        expected_slots = product_count * len(nutrient_ids)
        if audit["grid"]["row_count"] != expected_slots:
            raise ValueError("Unexpected cross-product size")
        print(f"[OUTPUT] Ghi {expected_slots:,} vị trí chỉ số, giữ null cho dữ liệu chưa có...", flush=True)
        output, readback, preview = save_frame(spark, fact.orderBy(*KEYS), artifacts, "beverage_nutrients", KEYS, expected_slots)
        for frame in (products, clean, pair_stats):
            frame.unpersist()
        readback = readback.persist(StorageLevel.MEMORY_AND_DISK)
        evidence["output"] = output
        write_json(folder / "preview.json", preview)
        evidence["output_metrics"] = validate_fact(readback, product_count, expected_slots,
                                                    task3["clean_output"]["row_count"], task3["eligible_nutrient_rows"])
        if evidence["output_metrics"]["value_review_records"] != task3["value_review_output"]["row_count"]:
            raise ValueError("Value review flags changed relative to Task 3")
        by_nutrient = coverage_table(readback, ["nutrient_id", "nutrient_name", "unit_name"])
        by_group = coverage_table(readback, ["beverage_group", "nutrient_id", "nutrient_name", "unit_name"])
        evidence["coverage_by_nutrient"] = [row.asDict() for row in by_nutrient.collect()]
        for row in evidence["coverage_by_nutrient"]:
            previous = next(item for item in task3["coverage"] if item["id"] == row["nutrient_id"])
            for current, old in (("numeric_valid", "valid"), ("missing_amount", "missing_amount"),
                                 ("no_source_record", "no_source_record"), ("conflicting_duplicate", "conflicting_duplicate"),
                                 ("invalid_only", "invalid_only")):
                if row[current] != previous[old] or row["expected_beverages"] != product_count:
                    raise ValueError("Coverage changed relative to Task 3")
        coverage_meta, _, _ = save_frame(spark, by_nutrient, artifacts, "coverage_by_nutrient", ["nutrient_id"], len(nutrient_ids))
        evidence["coverage_output"] = coverage_meta
        group_meta, _, _ = save_frame(spark, by_group, artifacts, "coverage_by_group", ["beverage_group", "nutrient_id"], len(task2["policy"]["groups"]) * len(nutrient_ids))
        evidence["group_coverage_output"] = group_meta
        if sum(row["expected_beverages"] for row in evidence["coverage_by_nutrient"]) != expected_slots:
            raise ValueError("Coverage denominator does not reconcile")
        per_product = readback.groupBy("fdc_id").agg(
            F.sum(F.col("source_record_present").cast("long")).alias("source_nutrients"),
            F.sum(F.col("eligible_for_comparison").cast("long")).alias("eligible_nutrients"))
        evidence["product_coverage_distribution"] = [row.asDict() for row in per_product.groupBy(
            "source_nutrients", "eligible_nutrients").count().orderBy("source_nutrients", "eligible_nutrients").collect()]
        preview_columns = ["fdc_id", "description", "brand_name", "beverage_group", "nutrient_id", "nutrient_name", "amount", "unit_name", "measurement_basis"]
        ranked = readback.filter(F.col("eligible_for_comparison")).withColumn("_rank", F.row_number().over(
            Window.partitionBy("beverage_group").orderBy("fdc_id", "nutrient_id")))
        write_json(folder / "eligible_preview.json", [row.asDict() for row in ranked.filter(F.col("_rank") <= 3).select(*preview_columns).collect()])
        metadata_columns = ("description", "brand_name", "brand_owner", "beverage_group")
        evidence["joined_metadata_null_counts"] = {name: output["readback_metrics"]["null_counts"][name]
                                                   for name in metadata_columns}
        evidence["beverage_metadata_null_counts"] = {name: handoff["beverages"]["readback_metrics"]["null_counts"][name]
                                                     for name in metadata_columns}
        for metadata in (handoff["beverages"], task3["clean_output"], task3["classified_output"]):
            verify_artifact(metadata)
        for table in contract["tables"]:
            verify_raw(table)
        if sha256(task3_path) != evidence["task3_result_sha256"] or sha256(Path(task3["input_handoff_path"])) != task3["input_handoff_sha256"]:
            raise ValueError("Pinned evidence changed during Task 4")
        evidence.update(status="PASS", input_integrity_verified_before_and_after=True)
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
        write_json(folder / "join_result.json", evidence)
        write_report(folder, evidence)
    if evidence["status"] == "PASS":
        try:
            write_json(folder / "task5_input.json", {
                "status": "READY_FOR_TASK5", "snapshot_run_id": evidence["snapshot_run_id"],
                "task4_result_path": (folder / "join_result.json").relative_to(ROOT).as_posix(),
                "task4_result_sha256": sha256(folder / "join_result.json"), "output": evidence["output"],
                "coverage_output": evidence["coverage_output"], "group_coverage_output": evidence["group_coverage_output"],
                "grain": KEYS, "requested_nutrients": evidence["requested_nutrients"],
                "comparison_filter": "eligible_for_comparison = true",
                "comparison_dimensions": ["beverage_group", "nutrient_id", "unit_name", "measurement_basis"],
                "missing_value_policy": "retain null; report coverage against all selected beverages in each group",
                "basis_policy": "Task 3 export consistency rules retained; basis-unconfirmed rows remain in the full output",
            })
        except Exception as exc:
            evidence.update(status="FAIL", error=f"Task 5 handoff failed: {exc}")
            write_json(folder / "join_result.json", evidence)
            write_report(folder, evidence)
    publish_stage_result(folder / "join_result.json", args.result_pointer)
    print(f"[KẾT QUẢ] {evidence['status']} | {evidence['elapsed_seconds']:.1f} s", flush=True)
    print(f"[EVIDENCE] {folder.relative_to(ROOT).as_posix()}", flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
