"""Aggregate nutrition statistics by beverage group, nutrient and verified basis."""
from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path
import platform
import sys
import time
import traceback

from src.common import ROOT, load_config, sha256, start_spark, utc_now, write_json
from src.run_storage import create_run
from src.common import add_stage_arguments, publish_stage_result, record_spark_baseline
from src.clean_nutrients import save_frame
from src.input_contract import CSV_OPTIONS, measure_input
from src.join_beverage_nutrients import (
    KEYS, coverage_table, require_unique, validate_fact, verify_artifact,
)


GROUP_KEYS = ["beverage_group", "nutrient_id", "nutrient_name", "unit_name"]
KPI_KEYS = ["beverage_group", "nutrient_id", "unit_name", "measurement_basis"]
STATISTICS = ["mean_amount", "median_amount", "min_amount", "max_amount", "p25_amount", "p75_amount"]


def load_inputs(config, root=ROOT):
    root = Path(root).resolve()
    task = config["task5"]
    handoff_path = (root / task["input_handoff_path"]).resolve()
    if sha256(handoff_path) != task["input_handoff_sha256"]:
        raise ValueError("Pinned Task 5 input handoff changed")
    handoff = load_config(handoff_path)
    if handoff["status"] != "READY_FOR_TASK5":
        raise ValueError("Expected the successful Task 4 handoff")
    result_path = (root / handoff["task4_result_path"]).resolve()
    if ((root / config["task4"]["result_path"]).resolve() != result_path
            or config["task4"]["result_sha256"] != handoff["task4_result_sha256"]
            or sha256(result_path) != handoff["task4_result_sha256"]):
        raise ValueError("Task 4 result changed or differs from the configured pin")
    result = load_config(result_path)
    if result["status"] != "PASS" or not result["input_integrity_verified_before_and_after"]:
        raise ValueError("Task 4 has not passed its integrity checks")
    if (result["snapshot_run_id"] != handoff["snapshot_run_id"]
            or result["snapshot_run_id"] != config["snapshot_run_id"]
            or handoff["grain"] != KEYS or result["grain"] != KEYS
            or handoff["requested_nutrients"] != result["requested_nutrients"]
            or handoff["requested_nutrients"] != config["task3"]["nutrients"]):
        raise ValueError("Task 4 handoff snapshot, grain or nutrient scope differs")
    if (handoff["comparison_filter"] != "eligible_for_comparison = true"
            or handoff["comparison_dimensions"] != KPI_KEYS):
        raise ValueError("Unsupported comparison policy in Task 4 handoff")
    task3_path = (root / result["task3_result_path"]).resolve()
    if sha256(task3_path) != result["task3_result_sha256"] or result["task3_result_sha256"] != config["task3"]["result_sha256"]:
        raise ValueError("Upstream Task 3 evidence changed")
    for key in ("output", "coverage_output", "group_coverage_output"):
        if handoff[key] != result[key]:
            raise ValueError(f"Task 4 handoff metadata differs: {key}")
        verify_artifact(handoff[key], root)
    return handoff_path, handoff, result_path, result


def validate_policy(policy):
    if policy["measurement_bases"] != ["per_100_ml", "per_100_g"]:
        raise ValueError("Expected explicit and separate 100 ml / 100 g bases")
    if policy["weighting"] != "one equal weight per fdc_id within each group/nutrient/basis":
        raise ValueError("Unsupported weighting policy")
    if policy["percentile_method"] != "Spark exact percentile with linear interpolation":
        raise ValueError("Unsupported percentile policy")
    if not isinstance(policy["round_decimal_places"], int) or not 0 <= policy["round_decimal_places"] <= 12:
        raise ValueError("Invalid output precision")
    for name in ("repeat_relative_tolerance", "repeat_absolute_tolerance"):
        if not math.isfinite(policy[name]) or policy[name] < 0:
            raise ValueError("Repeatability tolerance must be finite and nonnegative")


def validate_eligible(fact, bases):
    from pyspark.sql import functions as F

    eligible = F.col("eligible_for_comparison")
    bad = eligible & (
        ~F.col("clean_record_present").eqNullSafe(True)
        | ~F.col("value_review_required").eqNullSafe(False)
        | ~F.col("amount_status").eqNullSafe("valid")
        | ~F.col("basis_status").eqNullSafe("corroborated_export_label_scaling")
        | F.col("measurement_basis").isNull() | ~F.col("measurement_basis").isin(bases)
        | F.col("amount").isNull() | F.isnan("amount")
        | F.col("amount").isin(float("inf"), float("-inf")) | (F.col("amount") < 0))
    if fact.filter(eligible.isNull() | bad).limit(1).count():
        raise ValueError("An unsafe value is marked eligible for KPI calculation")


def compute_kpis(fact, group_coverage, policy):
    from pyspark.sql import functions as F

    bases = policy["measurement_bases"]
    validate_eligible(fact, bases)
    eligible = fact.filter(F.col("eligible_for_comparison"))
    # One FDC record has at most one amount for each nutrient after Task 3/4 checks.
    stats = eligible.groupBy(*(GROUP_KEYS + ["measurement_basis"])).agg(
        F.count("*").alias("n_compared"), F.avg("amount").alias("mean_amount"),
        F.min("amount").alias("min_amount"), F.max("amount").alias("max_amount"),
        F.percentile("amount", [0.25, 0.5, 0.75]).alias("_quantiles"))
    stats = (stats.withColumn("p25_amount", F.col("_quantiles")[0])
             .withColumn("median_amount", F.col("_quantiles")[1])
             .withColumn("p75_amount", F.col("_quantiles")[2]).drop("_quantiles"))
    slots = (group_coverage.select(*GROUP_KEYS,
                 F.col("expected_beverages").alias("n_beverages_total"),
                 F.col("numeric_valid").alias("n_numeric_available"),
                 F.col("eligible_for_comparison").alias("n_eligible_any_basis"))
             .withColumn("measurement_basis", F.explode(F.array(*[F.lit(basis) for basis in bases]))))
    kpis = (slots.join(stats, GROUP_KEYS + ["measurement_basis"], "left")
            .withColumn("n_compared", F.coalesce("n_compared", F.lit(0)))
            .withColumn("coverage_pct_all_beverages", F.round(F.col("n_compared") * 100.0 / F.col("n_beverages_total"), 4))
            .withColumn("kpi_status", F.when(F.col("n_compared") > 0, "COMPUTED").otherwise("NO_ELIGIBLE_RECORDS")))
    for name in STATISTICS:
        kpis = kpis.withColumn(name, F.round(F.col(name), policy["round_decimal_places"]))
    columns = GROUP_KEYS + ["measurement_basis", "n_beverages_total", "n_numeric_available",
                           "n_eligible_any_basis", "n_compared", "coverage_pct_all_beverages"] + STATISTICS + ["kpi_status"]
    by_basis = eligible.groupBy(*GROUP_KEYS).agg(
        *[F.sum((F.col("measurement_basis") == basis).cast("long")).alias("n_compared_" + basis) for basis in bases])
    quality = group_coverage.join(by_basis, GROUP_KEYS, "left").fillna({"n_compared_" + basis: 0 for basis in bases})
    return kpis.select(*columns).orderBy(*KPI_KEYS), quality.orderBy("beverage_group", "nutrient_id")


def validate_kpis(kpis, quality, expected_eligible, expected_rows, precision):
    from pyspark.sql import functions as F

    profile = require_unique(kpis, KPI_KEYS, "KPI output")
    require_unique(quality, ["beverage_group", "nutrient_id"], "KPI coverage")
    summary = kpis.agg(F.sum("n_compared").alias("compared_nutrient_records"),
                       F.sum((F.col("n_compared") > 0).cast("long")).alias("computed_rows"),
                       F.sum((F.col("n_compared") == 0).cast("long")).alias("empty_rows")).first().asDict()
    if profile["row_count"] != expected_rows or summary["compared_nutrient_records"] != expected_eligible:
        raise ValueError("KPI row count or compared record count changed")
    if quality.filter((F.col("n_compared_per_100_ml") + F.col("n_compared_per_100_g")) != F.col("eligible_for_comparison")).limit(1).count():
        raise ValueError("KPI basis counts do not reconcile with coverage")
    counts_bad = ((F.col("n_compared") < 0) | (F.col("n_compared") > F.col("n_eligible_any_basis"))
                  | (F.col("n_eligible_any_basis") > F.col("n_numeric_available"))
                  | (F.col("n_numeric_available") > F.col("n_beverages_total"))
                  | (F.col("n_beverages_total") <= 0))
    if kpis.filter(counts_bad).limit(1).count():
        raise ValueError("Invalid KPI coverage counts")
    from functools import reduce
    null_stat = reduce(lambda a, b: a | b, [F.col(name).isNull() for name in STATISTICS])
    any_stat = reduce(lambda a, b: a | b, [F.col(name).isNotNull() for name in STATISTICS])
    nonfinite = reduce(lambda a, b: a | b, [F.isnan(name) | F.col(name).isin(float("inf"), float("-inf")) for name in STATISTICS])
    tolerance = 10.0 ** (-precision)
    bounds_bad = ((F.col("min_amount") > F.col("p25_amount")) | (F.col("p25_amount") > F.col("median_amount"))
                  | (F.col("median_amount") > F.col("p75_amount")) | (F.col("p75_amount") > F.col("max_amount"))
                  | (F.col("mean_amount") < F.col("min_amount") - tolerance)
                  | (F.col("mean_amount") > F.col("max_amount") + tolerance))
    bad = (((F.col("n_compared") == 0) & (any_stat | ~F.col("kpi_status").eqNullSafe("NO_ELIGIBLE_RECORDS")))
           | ((F.col("n_compared") > 0) & (null_stat | nonfinite | bounds_bad | ~F.col("kpi_status").eqNullSafe("COMPUTED"))))
    if kpis.filter(bad).limit(1).count():
        raise ValueError("KPI statistics or empty-group status are inconsistent")
    return {"key_profile": profile, **summary}


def compare_csv_tables(current, previous, keys, relative_tolerance, absolute_tolerance, root=ROOT):
    """Compare actual exported cells; integer counts and nonnumeric fields are exact."""
    root = Path(root)
    for metadata in (current, previous):
        verify_artifact(metadata, root)
    schema = load_config(root / current["schema_path"])
    if schema != load_config(root / previous["schema_path"]):
        raise ValueError("Repeated output schemas differ")
    types = {field["name"]: field["type"] for field in schema["fields"]}

    def rows(metadata):
        with (root / metadata["path"]).open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != list(types):
                raise ValueError("Repeated CSV columns differ from their schema")
            found = {}
            for row in reader:
                key = tuple(row[name] for name in keys)
                if key in found:
                    raise ValueError("Repeated output contains duplicate keys")
                found[key] = row
        if len(found) != metadata["row_count"]:
            raise ValueError("Repeated output row count differs from metadata")
        return found

    actual, expected = rows(current), rows(previous)
    if actual.keys() != expected.keys():
        raise ValueError("Repeated output key sets differ")
    max_difference = 0.0
    cells = 0
    for key in actual:
        for name, kind in types.items():
            cells += 1
            a, b = actual[key][name], expected[key][name]
            if a == b:
                continue
            if kind == "double" and a and b:
                first, second = float(a), float(b)
                if math.isfinite(first) and math.isfinite(second) and math.isclose(first, second, rel_tol=relative_tolerance, abs_tol=absolute_tolerance):
                    max_difference = max(max_difference, abs(first - second))
                    continue
            raise ValueError(f"Repeated output differs at {key}, column {name}: {a!r} != {b!r}")
    return {"rows_compared": len(actual), "cells_compared": cells, "max_absolute_difference": max_difference,
            "csv_sha256_identical": current["sha256"] == previous["sha256"]}


def verify_repeatability(previous_path, evidence, root=ROOT):
    root = Path(root)
    previous_path = (root / previous_path).resolve()
    previous_hash = sha256(previous_path)
    previous = load_config(previous_path)
    if (previous["status"] != "PASS" or previous["run_id"] == evidence["run_id"]
            or previous["snapshot_run_id"] != evidence["snapshot_run_id"]
            or previous["input_output_sha256"] != evidence["input_output_sha256"]
            or previous["task4_result_sha256"] != evidence["task4_result_sha256"]
            or previous["aggregation_policy"] != evidence["aggregation_policy"]):
        raise ValueError("Repeatability reference must be a separate successful run on the same input and policy")
    policy = evidence["aggregation_policy"]
    compared = {}
    for name, keys in (("output", KPI_KEYS), ("coverage_output", ["beverage_group", "nutrient_id"])):
        compared[name] = compare_csv_tables(evidence[name], previous[name], keys,
                                            policy["repeat_relative_tolerance"], policy["repeat_absolute_tolerance"], root)
    if sha256(previous_path) != previous_hash:
        raise ValueError("Repeatability reference changed during comparison")
    return {"status": "PASS", "reference_result_path": str(previous_path), "reference_result_sha256": previous_hash,
            "reference_run_id": previous["run_id"], "tables": compared}


def write_report(folder, evidence):
    policy = evidence.get("aggregation_policy", {})
    metrics = evidence.get("output_metrics", {})
    lines = ["# Checkpoint 3 Task 5: KPI dinh dưỡng", "", f"Trạng thái: {evidence['status']}.",
             f"Snapshot: `{evidence.get('snapshot_run_id', '?')}`.", "",
             "KPI ở đây là số liệu tổng hợp để so sánh thông tin dinh dưỡng giữa các nhóm đồ uống.",
             "Grain: (beverage_group, nutrient_id, unit_name, measurement_basis). Hai cơ sở 100 ml và 100 g được tách riêng.",
             "Mỗi hồ sơ FDC đủ điều kiện có trọng số bằng nhau trong từng nhóm/chỉ số/cơ sở đo.",
             "Số 0 hợp lệ tham gia tính; chỉ số thiếu giữ null và không tham gia mẫu số trung bình.",
             "Chỉ dùng eligible_for_comparison=true; các cờ và điều kiện cơ sở đo được kế thừa Task 3–4.", "",
             "Các thống kê: mean (trung bình), median (trung vị), min, max, p25, p75 (các phân vị 25% và 75%).",
             "Phân vị tính bằng Spark percentile chính xác, có nội suy tuyến tính; trung vị là phân vị 50%.",
             f"Giá trị thống kê xuất với tối đa {policy.get('round_decimal_places', '?')} chữ số sau dấu thập phân.",
             "n_compared là số hồ sơ thực sự tham gia phép tính trong cơ sở đo của dòng hiện tại.",
             "coverage_pct_all_beverages = n_compared / n_beverages_total × 100; mẫu số giữ toàn bộ hồ sơ trong nhóm.",
             "Nhóm không có mẫu đủ điều kiện giữ các thống kê null, n_compared=0 và kpi_status=NO_ELIGIBLE_RECORDS.", "",
             f"Số dòng KPI: {metrics.get('key_profile', {}).get('row_count', '?')}; có kết quả: {metrics.get('computed_rows', '?')}; chưa có mẫu: {metrics.get('empty_rows', '?')}.",
             f"Tổng record dinh dưỡng được dùng: {metrics.get('compared_nutrient_records', '?')}.",
             f"Đối chiếu hai lần chạy độc lập: {evidence.get('repeatability', {}).get('status', 'NOT_RUN')}.", "",
             "| Nhóm đồ uống | Cơ sở đo | Số hồ sơ | Trung bình đường | Trung vị đường | Độ phủ (%) |",
             "| --- | --- | ---: | ---: | ---: | ---: |"]
    for row in evidence.get("sugar_preview", []):
        def text(value):
            return "—" if value is None else str(value)
        lines.append(f"| {row['beverage_group']} | {row['measurement_basis']} | {row['n_compared']} | "
                     f"{text(row['mean_amount'])} | {text(row['median_amount'])} | {row['coverage_pct_all_beverages']} |")
    lines.extend(["", "Đường có đơn vị G theo cơ sở đo của dòng: G/100 ml hoặc G/100 g.",
                  "Kết quả mô tả các hồ sơ đủ điều kiện trong snapshot; độ phủ thấp cần được nêu cùng khi so sánh.",
                  "Mỗi fdc_id là một hồ sơ nguồn; phép tính chưa chọn phiên bản mới nhất theo GTIN hoặc dùng trọng số doanh số.",
                  "Tên/mô tả từng đồ uống vẫn ở bảng Task 4; bảng KPI tổng hợp theo nhóm.", ""])
    for name in ("output", "coverage_output"):
        if name in evidence:
            lines.append(f"{name}: `{evidence[name]['path']}` ({evidence[name]['row_count']} dòng).")
    lines.extend(["", f"Runtime riêng Task 5: {evidence.get('elapsed_seconds', '?')} giây."])
    (folder / "kpi_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    add_stage_arguments(parser)
    parser.add_argument("--compare-with", type=Path, help="A prior successful KPI result on the same pinned input")
    args = parser.parse_args(argv)
    started = time.perf_counter()
    run_id, folder, artifacts = create_run("task5")
    evidence = {"status": "FAIL", "run_id": run_id, "started_at_utc": utc_now(),
                "grain": KPI_KEYS, "repeatability": {"status": "NOT_RUN"}}
    spark = None
    print("CHECKPOINT 3 | TASK 5 | TÍNH KPI DINH DƯỠNG", flush=True)
    try:
        from pyspark import StorageLevel
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType

        config = load_config(args.task_config or ROOT / "config/checkpoint3.json")
        task = config["task5"]
        policy = task["aggregation_policy"]
        validate_policy(policy)
        handoff_path, handoff, result_path, task4 = load_inputs(config)
        evidence.update(snapshot_run_id=handoff["snapshot_run_id"], input_handoff_path=str(handoff_path),
                        input_handoff_sha256=sha256(handoff_path), task4_result_path=str(result_path),
                        task4_result_sha256=sha256(result_path), input_output_sha256=handoff["output"]["sha256"],
                        requested_nutrients=handoff["requested_nutrients"], aggregation_policy=policy,
                        implementation_sha256=sha256(Path(__file__)))
        spark = start_spark("beverage-checkpoint3-kpis")
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
        evidence["environment"] = {"python": platform.python_version(), "spark": spark.version,
                                   "master": spark.sparkContext.master}

        def read_artifact(metadata):
            return spark.read.schema(StructType.fromJson(load_config(ROOT / metadata["schema_path"]))).options(**CSV_OPTIONS).csv((ROOT / metadata["path"]).as_posix())

        print("[INPUT] Đọc và kiểm tra bảng join đã chốt của Task 4...", flush=True)
        fact = read_artifact(handoff["output"]).persist(StorageLevel.MEMORY_AND_DISK)
        measured = measure_input(fact, KEYS)
        if measured["row_count"] != handoff["output"]["row_count"] or any(measured["null_key_counts"].values()):
            raise ValueError("Task 4 input count or null keys changed")
        previous = task4["output_metrics"]
        evidence["input_metrics"] = validate_fact(fact, previous["beverages"], handoff["output"]["row_count"],
                                                  previous["clean_records"], previous["eligible_records"])
        if evidence["input_metrics"]["value_review_records"] != previous["value_review_records"]:
            raise ValueError("Task 4 value review flags changed")
        metadata = [row.asDict() for row in fact.select("nutrient_id", "nutrient_name", "unit_name").distinct().collect()]
        expected = [{"nutrient_id": row["id"], "nutrient_name": row["name"], "unit_name": row["unit_name"]} for row in handoff["requested_nutrients"]]
        if sorted(metadata, key=lambda row: row["nutrient_id"]) != sorted(expected, key=lambda row: row["nutrient_id"]):
            raise ValueError("Input nutrient definitions differ from the handoff")
        actual_coverage = coverage_table(fact, GROUP_KEYS).persist(StorageLevel.MEMORY_AND_DISK)
        locked_coverage = read_artifact(handoff["group_coverage_output"])
        if actual_coverage.exceptAll(locked_coverage).limit(1).count() or locked_coverage.exceptAll(actual_coverage).limit(1).count():
            raise ValueError("Coverage recomputed from Task 4 differs from its locked report")
        coverage_rows = require_unique(actual_coverage, ["beverage_group", "nutrient_id"], "input coverage")["row_count"]
        print("[KPI] Lọc đủ điều kiện, tách cơ sở đo và tính trung bình/trung vị/phân vị...", flush=True)
        kpis, quality = compute_kpis(fact, actual_coverage, policy)
        if args.collect_baseline:
            record_spark_baseline(spark, kpis, folder, "nutrition_kpis")
        expected_rows = coverage_rows * len(policy["measurement_bases"])
        output, readback, preview = save_frame(spark, kpis, artifacts, "nutrition_kpis", KPI_KEYS, expected_rows)
        coverage_output, quality_readback, _ = save_frame(spark, quality, artifacts, "kpi_coverage", ["beverage_group", "nutrient_id"], coverage_rows)
        evidence.update(output=output, coverage_output=coverage_output)
        evidence["output_metrics"] = validate_kpis(readback, quality_readback, previous["eligible_records"], expected_rows,
                                                   policy["round_decimal_places"])
        write_json(folder / "preview.json", preview)
        evidence["sugar_preview"] = [row.asDict() for row in readback.filter(F.col("nutrient_id") == 2000).orderBy("measurement_basis", "beverage_group").collect()]
        evidence["coverage_summary"] = [row.asDict() for row in quality_readback.groupBy("nutrient_id", "nutrient_name", "unit_name").agg(
            F.sum("expected_beverages").alias("all_beverages"), F.sum("numeric_valid").alias("numeric_available"),
            F.sum("eligible_for_comparison").alias("eligible_any_basis"), F.sum("n_compared_per_100_ml").alias("compared_per_100_ml"),
            F.sum("n_compared_per_100_g").alias("compared_per_100_g")).orderBy("nutrient_id").collect()]
        reference = args.compare_with or task.get("repeatability_reference_path")
        if reference:
            print("[REPEAT] Đối chiếu CSV với lần chạy độc lập cùng snapshot...", flush=True)
            evidence["repeatability"] = verify_repeatability(reference, evidence)
        for key in ("output", "coverage_output", "group_coverage_output"):
            verify_artifact(handoff[key])
        if sha256(handoff_path) != evidence["input_handoff_sha256"] or sha256(result_path) != evidence["task4_result_sha256"]:
            raise ValueError("Pinned Task 4 evidence changed during Task 5")
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
        write_json(folder / "kpi_result.json", evidence)
        write_report(folder, evidence)
    if evidence["status"] == "PASS" and evidence["repeatability"]["status"] == "PASS":
        try:
            write_json(folder / "task6_input.json", {"status": "READY_FOR_TASK6", "snapshot_run_id": evidence["snapshot_run_id"],
                "task5_result_path": (folder / "kpi_result.json").relative_to(ROOT).as_posix(),
                "task5_result_sha256": sha256(folder / "kpi_result.json"), "output": evidence["output"],
                "coverage_output": evidence["coverage_output"], "grain": KPI_KEYS, "aggregation_policy": policy,
                "repeatability": evidence["repeatability"], "next_step": "Package the pipeline entry point and measure its full baseline"})
        except Exception as exc:
            evidence.update(status="FAIL", error=f"Task 6 handoff failed: {exc}")
            write_json(folder / "kpi_result.json", evidence)
            write_report(folder, evidence)
    publish_stage_result(folder / "kpi_result.json", args.result_pointer)
    print(f"[KẾT QUẢ] {evidence['status']} | {evidence['elapsed_seconds']:.1f} s | repeat={evidence['repeatability']['status']}", flush=True)
    print(f"[EVIDENCE] {folder.relative_to(ROOT).as_posix()}", flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
