"""Filter configured beverage categories from a successful Task 1 input contract."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import platform
import re
import sys
import time
import traceback

from src.common import ROOT, load_config, sha256, start_spark, utc_now, write_json
from src.run_storage import create_run
from src.common import add_stage_arguments, publish_stage_result, record_spark_baseline
from src.input_contract import CSV_OPTIONS, measure_input, read_locked_csv, verify_raw


def load_locked_input(path, config, root=ROOT):
    """Accept the pinned evidence only after validating its raw paths and content."""
    root = Path(root).resolve()
    contract = load_config(path)
    if contract.get("status") != "PASS" or not contract.get("raw_integrity_verified_before_and_after"):
        raise ValueError("Task 2 requires a successful Task 1 input contract")
    if any(contract.get(key) != config[key] for key in ("source", "release", "dataset")):
        raise ValueError("Input contract source, release or dataset differs from project config")
    if contract.get("read_options") != CSV_OPTIONS:
        raise ValueError("Input contract CSV options differ from the supported reader")
    tables = contract["tables"]
    names = [table["file_name"] for table in tables]
    if len(set(names)) != len(names) or set(names) != set(config["default_files"]):
        raise ValueError("Input contract must contain each configured table exactly once")
    raw_root = (root / config["raw_dir"]).resolve()
    for table in tables:
        raw_path = (root / table["raw_path"]).resolve()
        if not raw_path.is_relative_to(raw_root) or raw_path.name != table["file_name"]:
            raise ValueError(f"Invalid locked raw path for {table['file_name']}")
        verify_raw(table, root)
    return contract


def policy_maps(policy):
    categories = {}
    category_groups = policy.get("category_groups")
    if category_groups is None:
        category_items = [(label, label) for label in policy["beverage_categories"]]
    else:
        category_items = category_groups.items()
    if "groups" in policy and len(set(policy["groups"])) != len(policy["groups"]):
        raise ValueError("Beverage group names must be distinct")
    for label, group in category_items:
        key = re.sub(r"[\s\u00a0]+", " ", label).strip().lower()
        if not key or key in categories:
            raise ValueError("Beverage categories must be nonempty and distinct after normalization")
        if "groups" in policy and group is not None and group not in policy["groups"]:
            raise ValueError(f"Unknown beverage group: {group}")
        categories[key] = group
    units = {}
    for alias, canonical in policy["serving_unit_aliases"].items():
        key = re.sub(r"[\s\u00a0]+", " ", alias).strip().lower()
        if not key or key in units or canonical not in ("g", "ml"):
            raise ValueError("Serving unit aliases must be distinct and map to g or ml")
        units[key] = canonical
    if not categories or not units:
        raise ValueError("Beverage categories and serving unit aliases are required")
    return categories, units


def normalized_text(column):
    from pyspark.sql import functions as F

    return F.lower(F.trim(F.regexp_replace(F.coalesce(column, F.lit("")), r"[\s\u00A0]+", " ")))


def normalized_labels(labels):
    return [re.sub(r"[\s\u00a0]+", " ", label).strip().lower() for label in labels]


def candidate_condition(policy):
    from pyspark.sql import functions as F

    category = normalized_text(F.col("branded_food_category"))
    known = category.isin(normalized_labels(policy["category_groups"]))
    possible = category.rlike(policy["candidate_category_pattern"]) | (category == "")
    excluded = category.isin(normalized_labels(policy["excluded_categories"]))
    return (known | possible) & ~excluded


def classify_preparation(frame, policy):
    """Make preparation decisions explicit; conflicting or insufficient evidence is reviewed."""
    from pyspark.sql import functions as F

    rules = policy["preparation_rules"]
    description = normalized_text(F.concat_ws(" ", F.col("description"), F.col("short_description")))
    scope_description = F.regexp_replace(description, r"\bnon[\s-]*alcoholic\b|\balcohol[\s-]*free\b", " ")
    serving = normalized_text(F.col("household_serving_fulltext"))
    preparation = F.upper(F.trim(F.coalesce(F.col("preparation_state_code"), F.lit(""))))
    inferred = F.lit(None).cast("string")
    for rule in reversed(policy["group_patterns"]):
        inferred = F.when(description.rlike(rule["pattern"]), F.lit(rule["group"])).otherwise(inferred)
    group = F.coalesce(F.col("beverage_group"), inferred)
    group = F.when((F.col("beverage_group") == "Dairy milk") & (inferred == "Plant-based milk"),
                   F.lit("Plant-based milk")).otherwise(group)
    frame = frame.withColumn("beverage_group", group)
    explicit_rtd = description.rlike(rules["explicit_rtd_pattern"]) | (preparation == "READY_TO_DRINK")
    dry = description.rlike(rules["dry_pattern"]) | serving.rlike(rules["dry_serving_pattern"])
    concentrate_text = F.regexp_replace(description, rules["concentrate_exception_pattern"], " ")
    needs_preparation = dry | concentrate_text.rlike(rules["concentrate_pattern"])
    ambiguous = description.rlike(rules["ambiguous_product_pattern"])
    protein = description.rlike(r"\bprotein\b")
    ordinary_milk = F.col("beverage_group").isin("Dairy milk", "Plant-based milk") & description.rlike(r"\bmilk\b")
    ambiguous = ambiguous | (protein & ~ordinary_milk)
    rtd_category = F.col("category_normalized").isin(normalized_labels(policy["rtd_categories"]))
    liquid_description = ((F.col("serving_size_unit_normalized") == "ml")
                          & description.rlike(rules["liquid_description_pattern"]))
    reason = (F.when(F.col("_food_present").isNull(), F.lit("missing_food_record"))
        .when(scope_description.rlike(rules["excluded_product_pattern"]), F.lit("product_outside_scope"))
        .when(needs_preparation & explicit_rtd, F.lit("conflicting_preparation_evidence"))
        .when(needs_preparation, F.lit("requires_preparation"))
        .when(F.col("category_normalized") == "", F.lit("missing_category"))
        .when(ambiguous & ~explicit_rtd, F.lit("ambiguous_product_description"))
        .when((preparation == "UNPREPARED") & ~explicit_rtd, F.lit("unprepared_code_requires_review"))
        .when(~preparation.isin("", "READY_TO_DRINK", "PREPARED"), F.lit("other_preparation_code"))
        .when(F.col("beverage_group").isNull(), F.lit("unresolved_group"))
        .when(explicit_rtd, F.lit("explicit_ready_to_drink"))
        .when(rtd_category, F.lit("category_without_preparation_marker"))
        .when(liquid_description, F.lit("liquid_serving_and_beverage_description"))
        .otherwise(F.lit("insufficient_preparation_evidence")))
    return (frame.withColumn("selection_reason", reason)
        .withColumn("selection_status",
                    F.when(F.col("selection_reason").isin("product_outside_scope", "requires_preparation"), "exclude")
                     .when(F.col("selection_reason").isin("explicit_ready_to_drink", "category_without_preparation_marker",
                                                         "liquid_serving_and_beverage_description"), "include")
                     .otherwise("review"))
        .withColumn("preparation_status",
                    F.when(F.col("selection_status") == "include", "ready_to_drink")
                     .when(F.col("selection_reason") == "requires_preparation", "requires_preparation")
                     .otherwise("unknown")))


def filter_beverages(frame, policy):
    """Retain raw columns and add explicit category and serving-unit annotations."""
    from pyspark.sql import functions as F

    categories, units = policy_maps(policy)
    added_columns = {"category_normalized", "beverage_group",
                     "serving_size_unit_normalized", "serving_size_unit_status"}
    if added_columns.intersection(frame.columns):
        raise ValueError("Raw input already contains a Task 2 derived column")
    category_entries = []
    for key, label in categories.items():
        category_entries.extend([F.lit(key), F.lit(label)])
    unit_entries = []
    for alias, canonical in units.items():
        unit_entries.extend([F.lit(alias), F.lit(canonical)])
    category = F.lower(F.trim(F.regexp_replace(
        F.coalesce(F.col("branded_food_category"), F.lit("")), r"[\s\u00A0]+", " ")))
    unit = F.lower(F.trim(F.regexp_replace(
        F.coalesce(F.col("serving_size_unit"), F.lit("")), r"[\s\u00A0]+", " ")))
    normalized = (frame
        .withColumn("category_normalized", category)
        .withColumn("beverage_group", F.create_map(*category_entries)[F.col("category_normalized")])
        .withColumn("serving_size_unit_normalized", F.create_map(*unit_entries)[unit])
        .withColumn("serving_size_unit_status",
                    F.when(unit == "", F.lit("missing"))
                     .when(F.col("serving_size_unit_normalized").isNotNull(), F.lit("recognized"))
                     .otherwise(F.lit("unrecognized"))))
    if "preparation_rules" in policy:
        normalized = classify_preparation(normalized, policy)
        return normalized, normalized.filter(F.col("selection_status") == "include")
    return normalized, normalized.filter(F.col("beverage_group").isNotNull())


def unit_counts(frame):
    columns = ["beverage_group", "serving_size_unit", "serving_size_unit_normalized",
               "serving_size_unit_status"]
    return [row.asDict() for row in frame.groupBy(*columns).count().orderBy(*columns).collect()]


def write_csv(frame, path, preview_rows=10):
    """Stream Spark rows into a local CSV using standard CSV quoting on Windows."""
    preview = []
    count = 0
    with Path(path).open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(frame.columns)
        for row in frame.toLocalIterator():
            writer.writerow(tuple(row))
            count += 1
            if len(preview) < preview_rows:
                preview.append(row.asDict())
        stream.flush()
        os.fsync(stream.fileno())
    return count, preview


def write_report(folder, evidence):
    metrics = evidence.get("metrics", {})
    output = evidence.get("output", {})
    lines = ["# Lọc đồ uống Checkpoint 3 Task 2", "",
             f"Trạng thái {evidence['status']}. Snapshot `{evidence.get('snapshot_run_id', '?')}`.", "",
             f"Input contract `{evidence.get('input_contract_path', '?')}`.", "",
             f"Input {metrics.get('input_rows', '?')} dòng. Giữ {metrics.get('selected_rows', '?')} dòng.",
             f"Category thiếu {metrics.get('missing_category_rows', '?')} dòng trên toàn input.", "",
             "Phạm vi gồm tám nhóm đồ uống không cồn dùng trực tiếp theo chính sách rtd_non_alcoholic_v2.",
             f"Cần xem xét {metrics.get('review_rows', '?')} dòng. Loại khỏi phạm vi {metrics.get('excluded_rows', '?')} dòng.",
             "Mỗi dòng có selection_status, selection_reason và preparation_status để truy vết quy tắc.",
             "category_mapping.json ghi quyết định và số dòng cho từng category trong toàn input.",
             "Đơn vị chuẩn hóa mô tả khẩu phần. Cơ sở đo dinh dưỡng sẽ được xác minh trong task sau.", "",
             "| Nhóm đồ uống | Số dòng |", "| --- | ---: |"]
    for group, count in metrics.get("rows_by_group", {}).items():
        lines.append(f"| {group} | {count} |")
    lines.extend(["", "| Trạng thái đơn vị trong dữ liệu đã lọc | Số dòng |", "| --- | ---: |"])
    for status, count in metrics.get("rows_by_unit_status", {}).items():
        lines.append(f"| {status} | {count} |")
    lines.extend(["", f"CSV đầu ra `{output.get('path', '?')}`.", "",
                  f"Schema đầu ra `{output.get('schema_path', '?')}`.", "",
                  "Ghi bằng Python csv.writer qua iterator Spark để tương thích môi trường Windows hiện tại.",
                  "Mỗi lần chạy lưu trong một thư mục riêng. Đọc lại CSV bằng Spark để kiểm tra counts, null và đơn vị.",
                  "Chỉ dùng output của lần chạy có filter_result.json ghi PASS.", "",
                  f"CSV cần xem xét `{evidence.get('review_output', {}).get('path', '?')}`.", "",
                  "Quy tắc xác định dạng uống trực tiếp sử dụng category, description, khẩu phần và preparation_state_code.",
                  "Các trường hợp thiếu hoặc mâu thuẫn bằng chứng được đưa vào review thay vì tự gán dạng uống trực tiếp.",
                  "Preview nằm trong preview.json. Giá trị category và đơn vị gốc được giữ trong các cột gốc.", "",
                  f"Runtime của riêng Task 2 {evidence['elapsed_seconds']} giây."])
    if "error" in evidence:
        lines.extend(["", "Có lỗi. Xem error.log và filter_result.json trước khi dùng output."])
    (folder / "filter_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def join_descriptions(spark, candidates, food_table, root=ROOT):
    from pyspark.sql import functions as F

    foods = read_locked_csv(spark, food_table, root)
    duplicate_keys = foods.groupBy("fdc_id").count().filter(F.col("count") > 1).count()
    if duplicate_keys:
        raise ValueError(f"food.csv has {duplicate_keys} duplicate fdc_id keys")
    dimension = foods.select("fdc_id", "description", F.col("publication_date").alias("food_publication_date"))
    dimension = dimension.withColumn("_food_present", F.lit(1))
    return candidates.join(dimension, "fdc_id", "left"), duplicate_keys


def export_output(spark, frame, artifacts, name, expected_rows, expected_groups, keys):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    frame = frame.drop("_food_present")
    partial = artifacts / f"{name}.csv.partial"
    written_rows, _ = write_csv(frame.orderBy("fdc_id"), partial)
    readback = spark.read.schema(frame.schema).options(**CSV_OPTIONS).csv(partial.as_posix())
    measured = measure_input(readback, keys)
    groups = {row["beverage_group"]: row["count"] for row in readback.groupBy("beverage_group").count().collect()}
    if (written_rows != expected_rows or measured["row_count"] != expected_rows
            or any(measured["null_key_counts"].values()) or groups != expected_groups):
        raise ValueError(f"Written {name} differs from the classified data")
    distinct_ids = readback.select("fdc_id").distinct().count()
    if distinct_ids != expected_rows:
        raise ValueError(f"Written {name} has duplicate fdc_id keys")
    units = unit_counts(readback)
    sample_key = "beverage_group" if name == "beverages" else "selection_reason"
    ranked = readback.withColumn("_sample_rank", F.row_number().over(Window.partitionBy(sample_key).orderBy("fdc_id")))
    preview = [row.asDict() for row in ranked.filter(F.col("_sample_rank") <= 3)
               .drop("_sample_rank").orderBy(sample_key, "fdc_id").collect()]
    path = artifacts / f"{name}.csv"
    partial.rename(path)
    schema_path = artifacts / ("output_schema.json" if name == "beverages" else "review_schema.json")
    write_json(schema_path, frame.schema.jsonValue())
    result = {"path": path.relative_to(ROOT).as_posix(), "schema_path": schema_path.relative_to(ROOT).as_posix(),
              "schema_sha256": sha256(schema_path), "sha256": sha256(path), "size_bytes": path.stat().st_size,
              "row_count": written_rows, "distinct_fdc_ids": distinct_ids, "column_count": len(frame.columns),
              "format": "CSV", "writer": "Python csv.writer from Spark toLocalIterator",
              "write_mode": "new directory per run; partial renamed after validation", "readback_metrics": measured}
    return result, units, preview


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    add_stage_arguments(parser)
    parser.add_argument("--input-contract", type=Path, help="Explicit successful Task 1 JSON contract")
    args = parser.parse_args(argv)
    started = time.perf_counter()
    run_id, folder, artifacts = create_run("task2")
    evidence = {"status": "FAIL", "run_id": run_id, "started_at_utc": utc_now(),
                "scope": "Eight non-alcoholic ready-to-drink groups; ambiguous records retained separately",
                "measurement_basis_verified": False}
    spark = None
    print("CHECKPOINT 3 | TASK 2 | LỌC ĐỒ UỐNG", flush=True)
    try:
        config = load_config()
        task_config = load_config(args.task_config or ROOT / "config/checkpoint3.json")["task2"]
        policy = load_config(ROOT / task_config["scope_policy_path"])
        policy_maps(policy)
        path = (ROOT / (args.input_contract or task_config["input_contract_path"])).resolve()
        contract_hash = sha256(path)
        contract = load_locked_input(path, config)
        evidence.update(input_contract_path=str(path), input_contract_sha256=contract_hash,
                        snapshot_run_id=contract["snapshot_run_id"], policy=policy)
        tables = {table["file_name"]: table for table in contract["tables"]}
        table = tables["branded_food.csv"]
        print(f"[INPUT] {contract['snapshot_run_id']} | {table['file_name']}", flush=True)
        spark = start_spark("beverage-checkpoint3-filter")
        spark.sparkContext.setLogLevel("ERROR")
        evidence["environment"] = {"python": platform.python_version(), "spark": spark.version,
                                   "master": spark.sparkContext.master}
        from pyspark.sql import functions as F

        raw = read_locked_csv(spark, table)
        candidate = candidate_condition(policy)
        print("[CATEGORY] Thống kê toàn input và chọn ứng viên cần phân loại...", flush=True)
        category_counts = [row.asDict() for row in raw.groupBy(
            "branded_food_category", normalized_text(F.col("branded_food_category")).alias("category_normalized"),
            candidate.alias("candidate")
        ).count().orderBy("category_normalized", "branded_food_category").collect()]
        write_json(folder / "category_counts.json", category_counts)
        input_rows = sum(row["count"] for row in category_counts)
        candidate_rows = sum(row["count"] for row in category_counts if row["candidate"])
        if input_rows != table["row_count"]:
            raise ValueError("Source row count differs from Task 1")
        print(f"[JOIN] {candidate_rows:,} ứng viên + mô tả food.csv; kiểm tra khóa dimension...", flush=True)
        enriched, duplicate_keys = join_descriptions(spark, raw.filter(candidate), tables["food.csv"])
        normalized, selected = filter_beverages(enriched, policy)
        if args.collect_baseline:
            record_spark_baseline(spark, selected, folder, "beverage_filter")
        decisions = [row.asDict() for row in normalized.groupBy(
            "branded_food_category", "category_normalized", "beverage_group", "selection_status",
            "selection_reason", "preparation_status", "_food_present"
        ).count().orderBy("category_normalized", "selection_status", "selection_reason", "beverage_group").collect()]
        write_json(folder / "decision_counts.json", decisions)
        joined_rows = sum(row["count"] for row in decisions)
        unmatched = sum(row["count"] for row in decisions if row["_food_present"] is None)
        if joined_rows != candidate_rows:
            raise ValueError("Description join changed candidate row count")
        evidence["description_join"] = {"key": "fdc_id", "type": "left", "dimension_duplicate_keys": duplicate_keys,
                                        "rows_before": candidate_rows, "rows_after": joined_rows,
                                        "matched_rows": joined_rows - unmatched, "unmatched_rows": unmatched}
        rows_by_group = dict.fromkeys(policy["groups"], 0)
        review_groups = {}
        reason_counts = {}
        for row in decisions:
            reason_counts[row["selection_reason"]] = reason_counts.get(row["selection_reason"], 0) + row["count"]
            if row["selection_status"] == "include":
                rows_by_group[row["beverage_group"]] += row["count"]
            elif row["selection_status"] == "review":
                group = row["beverage_group"]
                review_groups[group] = review_groups.get(group, 0) + row["count"]
        selected_rows = sum(rows_by_group.values())
        review_rows = sum(review_groups.values())
        mapping = []
        excluded_labels = {key: reason for key, reason in zip(normalized_labels(policy["excluded_categories"]),
                                                              policy["excluded_categories"].values())}
        for category in category_counts:
            matching = [row for row in decisions if row["branded_food_category"] == category["branded_food_category"]]
            counts = {status: sum(row["count"] for row in matching if row["selection_status"] == status)
                      for status in ("include", "exclude", "review")}
            if not category["candidate"]:
                counts["exclude"] = category["count"]
            category_reasons = {}
            for row in matching:
                reason = row["selection_reason"]
                category_reasons[reason] = category_reasons.get(reason, 0) + row["count"]
            if not category["candidate"]:
                reason = excluded_labels.get(category["category_normalized"], "category_outside_scope")
                category_reasons[reason] = category["count"]
            if sum(counts.values()) != category["count"]:
                raise ValueError("Category decisions do not reconcile with raw input")
            mapping.append(dict(category, decision_counts=counts,
                                reason_counts=category_reasons,
                                observed_groups=sorted({row["beverage_group"] for row in matching if row["beverage_group"]})))
        write_json(folder / "category_mapping.json", mapping)
        evidence["metrics"] = {
            "input_rows": input_rows, "selected_rows": selected_rows,
            "candidate_rows": candidate_rows, "review_rows": review_rows,
            "excluded_rows": input_rows - selected_rows - review_rows, "rows_by_group": rows_by_group,
            "selection_reasons": reason_counts,
            "missing_category_rows": sum(row["count"] for row in category_counts
                                         if row["category_normalized"] == ""),
            "category_variants_observed": len(category_counts),
        }
        if any(count == 0 for count in rows_by_group.values()):
            raise ValueError("A configured beverage group has no included records")
        print(f"[FILTER] {selected_rows:,} đưa vào | {review_rows:,} cần xem xét", flush=True)
        print("[OUTPUT] Ghi và đọc lại CSV đồ uống...", flush=True)
        included_groups = {group: count for group, count in rows_by_group.items() if count}
        output, units, preview = export_output(spark, selected, artifacts, "beverages", selected_rows,
                                              included_groups, table["checked_keys"])
        evidence["output"] = output
        write_json(folder / "preview.json", preview)
        write_json(folder / "unit_counts.json", units)
        status_counts = {"recognized": 0, "missing": 0, "unrecognized": 0}
        for row in units:
            status_counts[row["serving_size_unit_status"]] += row["count"]
        evidence["metrics"]["rows_by_unit_status"] = status_counts
        if sum(status_counts.values()) != selected_rows:
            raise ValueError("Unit counts do not reconcile with selected rows")

        print("[REVIEW] Ghi riêng các trường hợp chưa đủ hoặc mâu thuẫn bằng chứng...", flush=True)
        review = normalized.filter(F.col("selection_status") == "review")
        review_output, _, review_preview = export_output(spark, review, artifacts, "review", review_rows,
                                                        review_groups, table["checked_keys"])
        evidence["review_output"] = review_output
        write_json(folder / "review_preview.json", review_preview)
        for raw_table in contract["tables"]:
            verify_raw(raw_table)
        if sha256(path) != contract_hash:
            raise ValueError("Input contract changed during Task 2")
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
        write_json(folder / "filter_result.json", evidence)
        write_report(folder, evidence)
    if evidence["status"] == "PASS":
        handoff = {"status": "READY_FOR_TASK3", "snapshot_run_id": evidence["snapshot_run_id"],
                   "task2_result_path": (folder / "filter_result.json").relative_to(ROOT).as_posix(),
                   "task2_result_sha256": sha256(folder / "filter_result.json"),
                   "input_contract_path": str(path), "input_contract_sha256": contract_hash,
                   "beverages": evidence["output"], "review": evidence["review_output"],
                   "food_nutrient_raw": tables["food_nutrient.csv"], "nutrient_raw": tables["nutrient.csv"],
                   "measurement_basis_verified": False, "nutrition_values_cleaned": False,
                   "next_checks": ["choose nutrient IDs from nutrient.csv", "missing/invalid/negative/nonfinite amounts",
                                   "duplicate (fdc_id, nutrient_id) records", "verify measurement basis before comparing"]}
        try:
            write_json(folder / "task3_input.json", handoff)
        except Exception as exc:
            evidence.update(status="FAIL", error=f"Task 3 handoff failed: {exc}")
            (folder / "error.log").write_text(traceback.format_exc(), encoding="utf-8")
            write_json(folder / "filter_result.json", evidence)
            write_report(folder, evidence)
    publish_stage_result(folder / "filter_result.json", args.result_pointer)
    print(f"[KẾT QUẢ] {evidence['status']} | {evidence['elapsed_seconds']:.1f} s", flush=True)
    print(f"[EVIDENCE] {folder.relative_to(ROOT).as_posix()}", flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
