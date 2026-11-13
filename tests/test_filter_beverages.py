"""Regressions for beverage scope, raw values and quoted CSV output."""
import json
from pathlib import Path
import tempfile
import unittest

from src.common import ROOT, load_config, sha256, start_spark
from src.filter_beverages import filter_beverages, join_descriptions, load_locked_input, policy_maps, write_csv
from src.input_contract import CSV_OPTIONS


POLICY = {
    "beverage_categories": ["Soda", "Water"],
    "serving_unit_aliases": {"g": "g", "grm": "g", "gm": "g", "ml": "ml", "mlt": "ml"},
}


class LockedInputTests(unittest.TestCase):
    def test_rejects_failed_contract_and_changed_raw(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            raw = root / "raw/branded_food.csv"
            raw.parent.mkdir()
            raw.write_text("fdc_id\n1\n", encoding="utf-8")
            config = {"source": "test", "release": "one", "dataset": "foods", "raw_dir": "raw",
                      "default_files": ["branded_food.csv"]}
            contract = dict(config, status="FAIL", raw_integrity_verified_before_and_after=True,
                            read_options=CSV_OPTIONS, tables=[{
                                "file_name": "branded_food.csv", "raw_path": "raw/branded_food.csv",
                                "sha256": sha256(raw), "size_bytes": raw.stat().st_size}])
            path = root / "contract.json"
            path.write_text(json.dumps(contract), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "successful Task 1"):
                load_locked_input(path, config, root)
            contract["status"] = "PASS"
            path.write_text(json.dumps(contract), encoding="utf-8")
            self.assertEqual(load_locked_input(path, config, root)["status"], "PASS")
            raw.write_text("fdc_id\n2\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                load_locked_input(path, config, root)

    def test_rejects_conflicting_normalized_categories(self):
        with self.assertRaisesRegex(ValueError, "distinct after normalization"):
            policy_maps(dict(POLICY, beverage_categories=["Soda", "  SODA "]))


class BeverageTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("beverage-filter-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        return self.spark.createDataFrame(rows, "fdc_id long, branded_food_category string, "
                                         "serving_size_unit string, gtin_upc string, ingredients string")

    def test_normalizes_whitespace_case_and_units_but_preserves_originals(self):
        source = self.frame([(1, "\u00a0 SODA\t ", " MLT ", "00123", 'Nước, "gas"\nline 2'),
                             (2, "Water", "GRM", "00456", None),
                             (3, "Soda Powder", "g", None, None),
                             (4, None, "ml", None, None)])
        normalized, selected = filter_beverages(source, POLICY)
        self.assertEqual(normalized.count(), 4)
        rows = {row.fdc_id: row.asDict() for row in selected.collect()}
        self.assertEqual(set(rows), {1, 2})
        self.assertEqual(rows[1]["branded_food_category"], "\u00a0 SODA\t ")
        self.assertEqual(rows[1]["serving_size_unit"], " MLT ")
        self.assertEqual(rows[1]["category_normalized"], "soda")
        self.assertEqual(rows[1]["beverage_group"], "Soda")
        self.assertEqual(rows[1]["serving_size_unit_normalized"], "ml")
        self.assertEqual(rows[2]["serving_size_unit_normalized"], "g")

    def test_keeps_duplicate_records_and_rows_with_unknown_or_missing_units(self):
        source = self.frame([(1, "Soda", "MG", None, None),
                             (1, "Soda", "MG", None, None),
                             (2, "Water", "", None, None),
                             (3, "Water", None, None, None)])
        _, selected = filter_beverages(source, POLICY)
        statuses = [row.serving_size_unit_status for row in selected.collect()]
        self.assertCountEqual(statuses, ["unrecognized", "unrecognized", "missing", "missing"])
        self.assertTrue(all(row.serving_size_unit_normalized is None for row in selected.collect()))

    def test_csv_round_trip_preserves_unicode_quotes_newlines_and_upc_zeroes(self):
        _, selected = filter_beverages(self.frame([
            (1, "Soda", "ml", "00123", 'Nước, "gas"\nline 2'),
            (2, "Water", None, "00000", None),
        ]), POLICY)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "beverages.csv"
            count, preview = write_csv(selected.orderBy("fdc_id"), path)
            result = self.spark.read.schema(selected.schema).options(**CSV_OPTIONS).csv(path.as_posix())
            self.assertEqual(count, 2)
            self.assertEqual(len(preview), 2)
            self.assertEqual(result.orderBy("fdc_id").collect(), selected.orderBy("fdc_id").collect())


class ReadyToDrinkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark("beverage-ready-to-drink-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.policy = load_config(ROOT / "config/beverage_scope.json")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def classify(self, records):
        rows = [(index, category, unit, serving, description, preparation, present, None)
                for index, (category, unit, serving, description, preparation, present) in enumerate(records, 1)]
        frame = self.spark.createDataFrame(rows, "fdc_id long, branded_food_category string, serving_size_unit string, "
                                          "household_serving_fulltext string, description string, preparation_state_code string, "
                                          "_food_present int, short_description string")
        normalized, _ = filter_beverages(frame, self.policy)
        return {row.fdc_id: row.asDict() for row in normalized.collect()}

    def test_coffee_with_liquid_unit_still_rejects_ground_packets_and_concentrate(self):
        rows = self.classify([
            ("Coffee", "ml", "12 OZA", "COLD BREW GROUND COFFEE & FLAVOR SACHET", "", 1),
            ("Coffee", "g", "0.5 PACKET", "SWEETENED ICED COFFEE PACKETS", "", 1),
            ("Coffee", "ml", "4 OZA", "COLD BREW COFFEE CONCENTRATE", "", 1),
            ("Coffee", "ml", "13.7 OZA", "PREMIUM ICED COFFEES DRINK", "", 1),
        ])
        self.assertEqual([rows[index]["selection_status"] for index in range(1, 5)],
                         ["exclude", "exclude", "exclude", "include"])
        self.assertEqual(rows[4]["beverage_group"], "Ready-to-drink coffee")

    def test_from_concentrate_is_not_the_same_as_a_concentrate(self):
        rows = self.classify([
            ("Fruit & Vegetable Juice, Nectars & Fruit Drinks", "ml", "1 cup", "ORANGE JUICE FROM CONCENTRATE", "", 1),
            ("Fruit & Vegetable Juice, Nectars & Fruit Drinks", "ml", "1 cup", "ORANGE JUICE NOT FROM CONCENTRATE", "", 1),
            ("Fruit & Vegetable Juice, Nectars & Fruit Drinks", "ml", "2 tbsp", "ORANGE JUICE CONCENTRATE", "", 1),
        ])
        self.assertEqual([rows[index]["selection_status"] for index in range(1, 4)], ["include", "include", "exclude"])

    def test_milk_specialized_products_and_conflicts_follow_scope(self):
        rows = self.classify([
            ("Milk", "ml", "2 tbsp", "SWEETENED CONDENSED MILK", "", 1),
            ("Milk", "ml", "1 cup", "FAT FREE PROTEIN MILK", "", 1),
            ("Energy, Protein & Muscle Recovery Drinks", "ml", "11 OZA", "FRENCH VANILLA PROTEIN-RICH SHAKE", "", 1),
            ("Energy, Protein & Muscle Recovery Drinks", "ml", "1 bottle", "ENERGY DRINK BLENDED WITH GRAPE JUICE FROM CONCENTRATE", "", 1),
            ("Plant Based Milk", "ml", "1 bottle", "READY-TO-DRINK MEAL", "", 1),
            ("Soda", "ml", "1 bottle", "READY-TO-DRINK SODA POWDER", "", 1),
        ])
        self.assertEqual([rows[index]["selection_status"] for index in range(1, 7)],
                         ["exclude", "include", "exclude", "include", "exclude", "review"])

    def test_uncertain_group_missing_metadata_and_unprepared_code_require_review(self):
        rows = self.classify([
            ("Other Drinks", "ml", "1 bottle", "UNSPECIFIED DRINK", "", 1),
            (None, "ml", "1 bottle", "WATER", "", 1),
            ("Water", "ml", "1 bottle", None, "", None),
            ("Water", "ml", "1 bottle", "WATER", "UNPREPARED", 1),
        ])
        self.assertTrue(all(row["selection_status"] == "review" for row in rows.values()))

    def test_plant_beverage_in_generic_category_gets_plant_group(self):
        rows = self.classify([
            ("Other Drinks", "ml", "1 cup", "OAT MILK BEVERAGE", "", 1),
            ("Other Drinks", "ml", "1 cup", "COFFEE DRINK WITH OAT MILK", "", 1),
            ("Other Drinks", "ml", "1 cup", "CHAI LATTE BEVERAGE", "", 1),
        ])
        self.assertTrue(all(row["selection_status"] == "include" for row in rows.values()))
        self.assertEqual(rows[1]["beverage_group"], "Plant-based milk")
        self.assertEqual(rows[2]["beverage_group"], "Ready-to-drink coffee")
        self.assertEqual(rows[3]["beverage_group"], "Ready-to-drink tea")

    def test_non_alcoholic_description_is_not_treated_as_alcoholic(self):
        row = self.classify([("Soda", "ml", "1 can", "NON-ALCOHOLIC BEVERAGE SODA", "", 1)])[1]
        self.assertEqual(row["selection_status"], "include")

    def test_review_samples_do_not_pass_specialized_products_and_ingredients(self):
        rows = self.classify([
            ("Other Drinks", "ml", "1 bottle", "SPECIAL K PROTEIN LIQUIDS VANILLA CAPPUCCINO", "", 1),
            ("Milk", "ml", "1 bottle", "BREAKFAST TG SHAKES MILK CHOCOLATE", "", 1),
            ("Milk", "ml", "0.5 cup", "SOFT SERVE AND SHAKE MIX", "", 1),
            ("Plant Based Milk", "ml", "0.25 cup", "COCONUT MILK", "", 1),
            ("Plant Based Milk", "ml", "1 cup", "READY-TO-DRINK COCONUT MILK BEVERAGE", "", 1),
            ("Fruit & Vegetable Juice, Nectars & Fruit Drinks", "ml", "3 POPS", "FRUIT JUICE FREEZERS", "", 1),
            ("Sport Drinks", "ml", "8 OZA", "FROZEN ICE ENERGY DRINK", "", 1),
            ("Milk", "g", "4 TBSP.", "CULTURED BUTTERMILK BLEND", "", 1),
        ])
        self.assertEqual([rows[index]["selection_status"] for index in range(1, 9)],
                         ["exclude", "exclude", "review", "review", "include", "exclude", "review", "review"])

    def test_description_join_rejects_duplicate_dimension_keys(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "food.csv").write_text("fdc_id,description,publication_date\n1,WATER,2026-01-01\n1,WATER,2026-01-01\n",
                                           encoding="utf-8")
            table = {"raw_path": "food.csv", "schema_declaration": {"columns": ["fdc_id", "description", "publication_date"],
                                                                      "types": {"fdc_id": "long"}}}
            candidates = self.spark.createDataFrame([(1,)], "fdc_id long")
            with self.assertRaisesRegex(ValueError, "duplicate fdc_id"):
                join_descriptions(self.spark, candidates, table, root)


if __name__ == "__main__":
    unittest.main()
