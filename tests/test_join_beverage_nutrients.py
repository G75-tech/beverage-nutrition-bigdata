"""Checks for join multiplication, missing nutrient slots and coverage safety."""
from pathlib import Path
import tempfile
import unittest

from src.common import sha256, start_spark
from src.join_beverage_nutrients import (
    audited_left_join, build_fact, coverage_table, validate_fact, validate_scope, verify_artifact,
)


class ArtifactIntegrityTests(unittest.TestCase):
    def test_changed_csv_is_rejected_before_join(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            folder = root / "artifacts"
            folder.mkdir()
            csv = folder / "input.csv"
            schema = folder / "schema.json"
            csv.write_text("fdc_id,amount\n1,0\n", encoding="utf-8")
            schema.write_text("{}", encoding="utf-8")
            metadata = {"path": "artifacts/input.csv", "sha256": sha256(csv),
                        "schema_path": "artifacts/schema.json", "schema_sha256": sha256(schema)}
            verify_artifact(metadata, root)
            csv.write_text("fdc_id,amount\n1,9\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                verify_artifact(metadata, root)


class JoinQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("beverage-task4-join-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.products = cls.spark.createDataFrame(
            [(1, "Drink A", "Group A"), (2, "Drink B", "Group A"), (3, "Drink C", "Group A"),
             (4, "Drink D", "Group B"), (5, "Drink E", "Group B")],
            "fdc_id long, description string, beverage_group string")
        cls.dimension = cls.spark.createDataFrame([(2000, "Total Sugars", "G"), (1093, "Sodium, Na", "MG")],
                                                  "nutrient_id long, nutrient_name string, unit_name string")
        cls.clean = cls.spark.createDataFrame([
            (1, 2000, 1, 0.0, "valid", "per_100_ml", True, False, "corroborated_export_label_scaling", "not_flagged", "Total Sugars", "G"),
            (1, 1093, 2, None, "missing_amount", None, False, False, "amount_missing", "not_flagged", "Sodium, Na", "MG"),
            (2, 2000, 3, 150.0, "valid", "per_100_ml", False, True, "corroborated_export_label_scaling", "above_configured_review_limit", "Total Sugars", "G"),
            (2, 1093, 4, 30.0, "valid", None, False, False, "json_record_unavailable", "not_flagged", "Sodium, Na", "MG"),
        ], "fdc_id long, nutrient_id long, id long, amount double, amount_status string, measurement_basis string, "
           "eligible_for_comparison boolean, value_review_required boolean, basis_status string, value_review_reason string, "
           "nutrient_name string, unit_name string")
        cls.pairs = cls.spark.createDataFrame([
            (1, 2000, 1, 1, 1, 0, "valid"), (1, 1093, 1, 0, 0, 1, "missing_amount"),
            (2, 2000, 1, 1, 1, 0, "valid"), (2, 1093, 1, 1, 1, 0, "valid"),
            (4, 2000, 2, 2, 2, 0, "conflicting_duplicate"), (5, 1093, 1, 0, 0, 0, "invalid_only"),
        ], "fdc_id long, nutrient_id long, source_records long, distinct_valid_values long, "
           "valid_records long, missing_records long, pair_status string")
        cls.fact, cls.audit = build_fact(cls.products, cls.dimension, cls.clean, cls.pairs)
        cls.fact = cls.fact.cache()
        cls.rows = {(row.fdc_id, row.nutrient_id): row for row in cls.fact.collect()}

    @classmethod
    def tearDownClass(cls):
        cls.fact.unpersist()
        cls.spark.stop()

    def test_keeps_all_beverages_and_zero_without_filling_absent_nutrients(self):
        self.assertEqual(len(self.rows), 10)
        self.assertEqual({key[0] for key in self.rows}, {1, 2, 3, 4, 5})
        self.assertEqual(self.rows[1, 2000].amount, 0)
        self.assertTrue(self.rows[1, 2000].eligible_for_comparison)
        for nutrient_id in (2000, 1093):
            row = self.rows[3, nutrient_id]
            self.assertIsNone(row.amount)
            self.assertEqual(row.nutrient_record_status, "no_source_record")
            self.assertFalse(row.source_record_present)
            self.assertFalse(row.eligible_for_comparison)
        metrics = validate_fact(self.fact, 5, 10, 4, 1)
        self.assertEqual(metrics["no_source_record"], 4)
        self.assertEqual(metrics["value_review_records"], 1)

    def test_missing_amount_conflict_and_invalid_only_have_distinct_reasons(self):
        self.assertEqual(self.rows[1, 1093].nutrient_record_status, "missing_amount")
        self.assertTrue(self.rows[1, 1093].clean_record_present)
        self.assertEqual(self.rows[4, 2000].nutrient_record_status, "conflicting_duplicate")
        self.assertFalse(self.rows[4, 2000].clean_record_present)
        self.assertEqual(self.rows[5, 1093].nutrient_record_status, "invalid_only")
        self.assertTrue(self.rows[5, 1093].source_record_present)
        self.assertFalse(self.rows[2, 2000].eligible_for_comparison)
        for audit in self.audit["joins"]:
            self.assertEqual(audit["output_rows"], audit["left"]["row_count"])
        self.assertEqual(self.audit["joins"][-1]["unmatched_left_rows"], 6)

    def test_coverage_uses_every_selected_beverage_as_the_denominator(self):
        result = {row.nutrient_id: row for row in coverage_table(
            self.fact, ["nutrient_id", "nutrient_name", "unit_name"]).collect()}
        self.assertEqual(result[2000].expected_beverages, 5)
        self.assertEqual(result[2000].numeric_valid, 2)
        self.assertEqual(result[2000].eligible_for_comparison, 1)
        self.assertEqual(result[2000].eligible_coverage_pct, 20)
        self.assertEqual(result[2000].conflicting_duplicate, 1)
        self.assertEqual(result[1093].missing_amount, 1)
        self.assertEqual(result[1093].invalid_only, 1)
        self.assertEqual(result[1093].numeric_valid_basis_unconfirmed, 1)

    def test_rejects_duplicate_null_and_mismatched_dimension_keys(self):
        left = self.products.select("fdc_id")
        for rows, schema in (([(1,), (1,)], "fdc_id long"), ([(None,)], "fdc_id long"),
                             ([("1",)], "fdc_id string")):
            with self.subTest(schema=schema, rows=rows), self.assertRaises(ValueError):
                audited_left_join(left, self.spark.createDataFrame(rows, schema), ["fdc_id"], "unsafe dimension")

    def test_rejects_out_of_scope_food_and_flagged_value_marked_eligible(self):
        from pyspark.sql import functions as F

        wrong = self.clean.withColumn("fdc_id", F.lit(999).cast("long"))
        # Keep only one row so the test exercises unknown IDs rather than duplicate keys.
        with self.assertRaisesRegex(ValueError, "out-of-scope"):
            validate_scope(wrong.limit(1), self.pairs, self.products, self.dimension)
        forged = self.fact.withColumn("eligible_for_comparison", F.when(
            (F.col("fdc_id") == 2) & (F.col("nutrient_id") == 2000), True).otherwise(F.col("eligible_for_comparison")))
        with self.assertRaisesRegex(ValueError, "marked eligible"):
            validate_fact(forged, 5, 10, 4, 2)


if __name__ == "__main__":
    unittest.main()
