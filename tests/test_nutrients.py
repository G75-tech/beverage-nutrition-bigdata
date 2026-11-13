"""Regression checks for missing values, conflicting duplicates and basis evidence."""
import io
import json
from pathlib import Path
import tempfile
import unittest

from src.common import start_spark
from src.clean_nutrients import add_basis, classify_amounts, resolve_pairs, save_frame
from src.measurement_basis import iter_foods, reference_rows


class JsonReferenceTests(unittest.TestCase):
    def test_streams_records_across_chunk_boundaries_and_preserves_unicode(self):
        foods = [{"fdcId": 1, "description": "Nước " + "x" * 100}, {"fdcId": 2}]
        source = io.StringIO(json.dumps({"BrandedFoods": foods}, ensure_ascii=False, indent=2))
        self.assertEqual(list(iter_foods(source, chunk_size=32)), foods)

    def test_rejects_truncated_and_trailing_comma_json(self):
        for text in ['{"BrandedFoods":[{"fdcId":1}', '{"BrandedFoods":[{"fdcId":1},]}']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                list(iter_foods(io.StringIO(text)))

    def test_zero_only_reference_does_not_prove_scaling(self):
        food = {"fdcId": 1, "servingSize": 250, "servingSizeUnit": "ml",
                "foodNutrients": [{"nutrient": {"id": 2000}, "amount": 0}],
                "labelNutrients": {"sugars": {"value": 0}}}
        row = reference_rows(food, [2000], {"ml": "ml"}, 0.01, 0.005)[0]
        self.assertEqual(row["record_reference_status"], "no_positive_scaling_anchor")

    def test_label_scaling_and_conflicting_json_values_are_distinguished(self):
        food = {"fdcId": 1, "servingSize": 250, "servingSizeUnit": "ml",
                "foodNutrients": [{"nutrient": {"id": 2000}, "amount": 8}],
                "labelNutrients": {"sugars": {"value": 20}}}
        self.assertEqual(reference_rows(food, [2000], {"ml": "ml"}, 0.01, 0.005)[0]["record_reference_status"],
                         "scale_consistent")
        food["foodNutrients"].append({"nutrient": {"id": 2000}, "amount": 9})
        self.assertEqual(reference_rows(food, [2000], {"ml": "ml"}, 0.01, 0.005)[0]["record_reference_status"],
                         "conflicting_json_nutrients")


class NutrientSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("beverage-nutrient-quality-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def source(self, rows):
        return self.spark.createDataFrame(rows, "id string, fdc_id string, nutrient_id string, amount string, derivation_id string")

    def test_amount_rules_preserve_zero_and_separate_missing_invalid_and_nonfinite(self):
        values = ["0", None, " ", "bad", "NaN", "Infinity", "-Infinity", "-2", " 1.5 "]
        source = self.source([(str(index), "1", "2000", value, "71") for index, value in enumerate(values, 1)])
        rows = classify_amounts(source).orderBy("id").collect()
        self.assertEqual([row.amount_status for row in rows], ["valid", "missing_amount", "missing_amount", "invalid_amount",
                                                              "nonfinite_amount", "nonfinite_amount", "nonfinite_amount",
                                                              "negative_amount", "valid"])
        self.assertEqual(rows[0].amount, 0)
        self.assertIsNone(rows[4].amount)
        self.assertEqual(rows[7].amount, -2)

    def test_duplicate_policy_uses_valid_over_missing_and_quarantines_conflicts(self):
        rows = [("1", "1", "2000", None, "71"), ("2", "1", "2000", "8", "71"),
                ("3", "1", "2000", "8.0", "72"), ("4", "2", "2000", "5", "71"),
                ("5", "2", "2000", "6", "71"), ("6", "3", "2000", None, "71"),
                ("7", "3", "2000", "-1", "71"), ("8", "4", "2000", "bad", "71")]
        clean, quarantine, stats = resolve_pairs(classify_amounts(self.source(rows)))
        chosen = {row.fdc_id: row for row in clean.collect()}
        self.assertEqual(set(chosen), {1, 3})
        self.assertEqual(chosen[1].id, 2)
        self.assertEqual(chosen[1].amount, 8)
        self.assertIsNone(chosen[3].amount)
        self.assertEqual(quarantine.count(), 4)
        self.assertEqual({row.fdc_id: row.pair_status for row in stats.collect()},
                         {1: "valid", 2: "conflicting_duplicate", 3: "missing_amount", 4: "invalid_only"})

    def test_basis_needs_matching_export_metadata_and_a_nonzero_scaling_anchor(self):
        clean = self.spark.createDataFrame([(1, 2000, 8.0, "valid"), (2, 2000, 0.0, "valid"),
                                           (3, 2000, 8.0, "valid"), (4, 2000, 12.68, "valid"),
                                           (5, 2000, 2300.0, "valid")],
                                          "fdc_id long, nutrient_id long, amount double, amount_status string")
        beverages = self.spark.createDataFrame([(1, "Soda", 250.0, "ml"), (2, "Water", 250.0, "ml"),
                                               (3, "Soda", 250.0, "ml"), (4, "Soda", 355.0, "ml"),
                                               (5, "Soda", 100.0, "ml")],
                                              "fdc_id long, beverage_group string, serving_size double, serving_size_unit_normalized string")
        nutrients = self.spark.createDataFrame([(2000, "Total Sugars", "G")], "nutrient_id long, nutrient_name string, unit_name string")
        reference = self.spark.createDataFrame([(1, 2000, 8.0, 250.0, "ml", "scale_consistent"),
                                               (2, 2000, 0.0, 250.0, "ml", "no_positive_scaling_anchor"),
                                               (3, 2000, 8.0, 250.0, "g", "scale_consistent"),
                                               (4, 2000, 12.7, 355.0, "ml", "scale_consistent"),
                                               (5, 2000, 2300.0, 100.0, "ml", "scale_consistent")],
                                              "fdc_id long, nutrient_id long, json_amount double, json_serving_size double, "
                                              "json_serving_unit string, record_reference_status string")
        rows = {row.fdc_id: row for row in add_basis(clean, beverages, nutrients, reference,
                                                    review_limits={2000: 100}).collect()}
        self.assertTrue(rows[1].eligible_for_comparison)
        self.assertEqual(rows[1].measurement_basis, "per_100_ml")
        self.assertFalse(rows[2].eligible_for_comparison)
        self.assertIsNone(rows[2].measurement_basis)
        self.assertEqual(rows[3].basis_status, "serving_unit_mismatch")
        self.assertTrue(rows[4].eligible_for_comparison)
        self.assertTrue(rows[5].value_review_required)
        self.assertFalse(rows[5].eligible_for_comparison)

    def test_csv_round_trip_handles_raw_nonfinite_values_and_boolean_flags(self):
        from pyspark.sql import functions as F

        frame = classify_amounts(self.source([("1", "1", "2000", "NaN", "71"),
                                             ("2", "2", "2000", "0", "71")]))
        frame = frame.withColumn("eligible_for_comparison", F.col("amount_status") == "valid")
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            metadata, readback, _ = save_frame(self.spark, frame.orderBy("id"), root, "fixture", ["id"], 2, root)
            self.assertEqual(metadata["row_count"], 2)
            rows = readback.orderBy("id").collect()
            self.assertEqual(rows[0].amount_raw, "NaN")
            self.assertIsNone(rows[0].amount)
            self.assertFalse(rows[0].eligible_for_comparison)
            self.assertEqual(rows[1].amount, 0)
            self.assertTrue(rows[1].eligible_for_comparison)


if __name__ == "__main__":
    unittest.main()
