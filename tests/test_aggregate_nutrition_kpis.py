"""Statistical checks for basis separation, empty groups and reproducibility."""
import csv
from pathlib import Path
import tempfile
import unittest

from src.common import sha256, start_spark, write_json
from src.clean_nutrients import save_frame
from src.join_beverage_nutrients import coverage_table
from src.aggregate_nutrition_kpis import (
    GROUP_KEYS, KPI_KEYS, STATISTICS, compare_csv_tables, compute_kpis, validate_kpis, verify_repeatability,
)


POLICY = {
    "measurement_bases": ["per_100_ml", "per_100_g"],
    "weighting": "one equal weight per fdc_id within each group/nutrient/basis",
    "percentile_method": "Spark exact percentile with linear interpolation",
    "round_decimal_places": 6,
    "repeat_relative_tolerance": 1e-9,
    "repeat_absolute_tolerance": 1e-9,
}


class RepeatabilityTests(unittest.TestCase):
    def artifact(self, root, name, amount, count=2, basis="per_100_ml"):
        folder = root / "artifacts"
        folder.mkdir(exist_ok=True)
        path = folder / f"{name}.csv"
        schema_path = folder / f"{name}_schema.json"
        columns = KPI_KEYS + ["n_compared", "mean_amount"]
        kinds = ["string", "long", "string", "string", "long", "double"]
        write_json(schema_path, {"type": "struct", "fields": [
            {"name": column, "type": kind, "nullable": True, "metadata": {}}
            for column, kind in zip(columns, kinds)]})
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(columns)
            writer.writerow(["Nhóm A", 2000, "G", basis, count, amount])
        return {"path": path.relative_to(root).as_posix(), "sha256": sha256(path),
                "schema_path": schema_path.relative_to(root).as_posix(), "schema_sha256": sha256(schema_path),
                "row_count": 1}

    def test_float_tolerance_does_not_allow_changed_integer_counts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old = self.artifact(root, "old", "0.3333333333")
            new = self.artifact(root, "new", "0.3333333334")
            result = compare_csv_tables(new, old, KPI_KEYS, 1e-9, 1e-9, root)
            self.assertEqual(result["rows_compared"], 1)
            changed_count = self.artifact(root, "count", "0.3333333333", count=3)
            with self.assertRaisesRegex(ValueError, "n_compared"):
                compare_csv_tables(changed_count, old, KPI_KEYS, 1e-9, 1e-9, root)

    def test_null_zero_and_changed_basis_are_distinguished(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old = self.artifact(root, "old", "")
            for name, amount, basis in (("zero", "0", "per_100_ml"), ("basis", "", "per_100_g")):
                new = self.artifact(root, name, amount, basis=basis)
                with self.subTest(name=name), self.assertRaises(ValueError):
                    compare_csv_tables(new, old, KPI_KEYS, 1e-9, 1e-9, root)

    def test_references_with_different_input_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            previous = {"status": "PASS", "run_id": "first", "snapshot_run_id": "snapshot",
                        "input_output_sha256": "old", "task4_result_sha256": "result", "aggregation_policy": POLICY}
            write_json(root / "previous.json", previous)
            current = dict(previous, run_id="second", input_output_sha256="changed")
            with self.assertRaisesRegex(ValueError, "same input"):
                verify_repeatability("previous.json", current, root)


class KpiSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("beverage-task5-kpi-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "4")
        # SQL VALUES avoids repeated Windows Python-worker startup for tiny fixtures.
        cls.fact = cls.spark.sql("""
            SELECT cast(fdc_id as bigint) fdc_id, beverage_group,
                   cast(2000 as bigint) nutrient_id, 'Total Sugars' nutrient_name, 'G' unit_name,
                   measurement_basis, cast(amount as double) amount,
                   eligible_for_comparison, value_review_required, amount_status, basis_status,
                   amount_status <> 'no_source_record' clean_record_present,
                   amount_status <> 'no_source_record' source_record_present,
                   amount_status nutrient_record_status
            FROM VALUES
                (1, 'Nhóm A', 'per_100_ml', 0.0, true, false, 'valid', 'corroborated_export_label_scaling'),
                (2, 'Nhóm A', 'per_100_ml', 10.0, true, false, 'valid', 'corroborated_export_label_scaling'),
                (3, 'Nhóm A', 'per_100_g', 20.0, true, false, 'valid', 'corroborated_export_label_scaling'),
                (4, 'Nhóm A', 'per_100_ml', 80.0, false, true, 'valid', 'corroborated_export_label_scaling'),
                (5, 'Nhóm A', null, 30.0, false, false, 'valid', 'json_record_unavailable'),
                (6, 'Nhóm A', null, null, false, false, 'no_source_record', 'no_clean_record'),
                (7, 'Nhóm A', null, null, false, false, 'missing_amount', 'amount_missing'),
                (8, 'Nhóm B', null, null, false, false, 'no_source_record', 'no_clean_record'),
                (9, 'Nhóm B', null, 0.0, false, false, 'valid', 'no_positive_scaling_anchor')
            AS fixture(fdc_id, beverage_group, measurement_basis, amount, eligible_for_comparison,
                       value_review_required, amount_status, basis_status)
        """).cache()
        cls.coverage = coverage_table(cls.fact, GROUP_KEYS).cache()
        cls.kpis, cls.quality = compute_kpis(cls.fact, cls.coverage, POLICY)
        cls.kpis = cls.kpis.cache()
        cls.quality = cls.quality.cache()
        cls.rows = {(row.beverage_group, row.measurement_basis): row for row in cls.kpis.collect()}

    @classmethod
    def tearDownClass(cls):
        for frame in (cls.kpis, cls.quality, cls.coverage, cls.fact):
            frame.unpersist()
        cls.spark.stop()

    def test_statistics_include_zero_exclude_flags_and_separate_bases(self):
        ml = self.rows["Nhóm A", "per_100_ml"]
        grams = self.rows["Nhóm A", "per_100_g"]
        self.assertEqual(ml.n_compared, 2)
        self.assertEqual(ml.mean_amount, 5)
        self.assertEqual(ml.median_amount, 5)
        self.assertEqual(ml.min_amount, 0)
        self.assertEqual(ml.max_amount, 10)
        self.assertEqual(ml.p25_amount, 2.5)
        self.assertEqual(ml.p75_amount, 7.5)
        self.assertEqual(grams.n_compared, 1)
        self.assertEqual(grams.mean_amount, 20)
        self.assertEqual(ml.n_beverages_total, 7)
        self.assertEqual(ml.n_eligible_any_basis, 3)
        self.assertEqual(ml.coverage_pct_all_beverages, 28.5714)

    def test_empty_groups_keep_null_statistics_and_zero_coverage(self):
        for basis in POLICY["measurement_bases"]:
            row = self.rows["Nhóm B", basis]
            self.assertEqual(row.n_compared, 0)
            self.assertEqual(row.n_beverages_total, 2)
            self.assertEqual(row.coverage_pct_all_beverages, 0)
            self.assertEqual(row.kpi_status, "NO_ELIGIBLE_RECORDS")
            self.assertTrue(all(row[name] is None for name in STATISTICS))

    def test_rejects_flagged_value_accidentally_marked_eligible(self):
        from pyspark.sql import functions as F
        forged = self.fact.withColumn("eligible_for_comparison", F.when(F.col("fdc_id") == 4, True)
                                     .otherwise(F.col("eligible_for_comparison")))
        with self.assertRaisesRegex(ValueError, "unsafe value"):
            compute_kpis(forged, self.coverage, POLICY)

    def test_csv_mode_accepts_documented_basis_and_rejects_mixed_policy(self):
        from pyspark.sql import functions as F

        csv_fact = self.fact.withColumn("basis_status", F.when(
            F.col("basis_status") == "corroborated_export_label_scaling", "documented_csv_serving_unit")
            .otherwise(F.col("basis_status")))
        kpis, _ = compute_kpis(csv_fact, self.coverage, POLICY, mode="csv")
        rows = {(row.beverage_group, row.measurement_basis): row for row in kpis.collect()}
        self.assertEqual(rows["Nhóm A", "per_100_ml"].n_compared, 2)
        self.assertEqual(rows["Nhóm A", "per_100_ml"].mean_amount, 5)
        self.assertEqual(rows["Nhóm A", "per_100_g"].mean_amount, 20)
        with self.assertRaisesRegex(ValueError, "unsafe value"):
            compute_kpis(csv_fact, self.coverage, POLICY, mode="json_crosscheck")
        with self.assertRaisesRegex(ValueError, "unsafe value"):
            compute_kpis(self.fact, self.coverage, POLICY, mode="csv")

    def test_csv_roundtrip_preserves_unicode_nulls_and_counts(self):
        summary = validate_kpis(self.kpis, self.quality, 3, 4, 6)
        self.assertEqual(summary["computed_rows"], 2)
        self.assertEqual(summary["empty_rows"], 2)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            folder = root / "artifacts"
            folder.mkdir()
            metadata, readback, _ = save_frame(self.spark, self.kpis.coalesce(1), folder,
                                              "kpis", KPI_KEYS, 4, root)
            actual = {(row.beverage_group, row.measurement_basis): row for row in readback.collect()}
            self.assertEqual(metadata["row_count"], 4)
            self.assertEqual(actual["Nhóm A", "per_100_ml"].mean_amount, 5)
            self.assertIsNone(actual["Nhóm B", "per_100_ml"].mean_amount)


if __name__ == "__main__":
    unittest.main()
