"""Measurement-basis policy and optional USDA JSON label cross-checks."""
from __future__ import annotations

import csv
import io
import json
import math
from pathlib import Path
import re
import zipfile


LABEL_FIELDS = {2000: "sugars", 1008: "calories", 1093: "sodium", 1003: "protein", 1004: "fat"}
REFERENCE_COLUMNS = ["fdc_id", "nutrient_id", "json_amount", "label_amount", "json_serving_size",
                     "json_serving_unit", "record_reference_status", "scale_checks", "scale_mismatches"]
BASIS_STATUSES = {"csv": "documented_csv_serving_unit",
                  "json_crosscheck": "corroborated_export_label_scaling"}


def basis_mode(task):
    """Old pinned configs retain their original JSON policy; new configs select CSV."""
    mode = task.get("basis_mode", "json_crosscheck")
    if mode not in BASIS_STATUSES:
        raise ValueError(f"Unsupported measurement basis mode: {mode!r}")
    return mode


def iter_foods(stream, chunk_size=1024 * 1024):
    """Read the BrandedFoods array incrementally instead of loading the full JSON."""
    decoder = json.JSONDecoder()
    buffer = stream.read(chunk_size)
    match = re.match(r'\s*\{\s*"BrandedFoods"\s*:\s*\[', buffer)
    if match is None:
        raise ValueError("Expected a JSON object containing a BrandedFoods array")
    position = match.end()
    after_item = False
    first_item = True
    while True:
        while position < len(buffer) and buffer[position].isspace():
            position += 1
        if position >= len(buffer):
            more = stream.read(chunk_size)
            if not more:
                raise ValueError("Truncated BrandedFoods array")
            buffer = buffer[position:] + more
            position = 0
            continue
        token = buffer[position]
        if after_item:
            if token == ",":
                position += 1
                after_item = False
                continue
            if token != "]":
                raise ValueError("Expected comma or end of BrandedFoods array")
        if token == "]":
            if not after_item and not first_item:
                raise ValueError("Trailing comma in BrandedFoods array")
            tail = buffer[position + 1:]
            closed = False
            while True:
                for character in tail:
                    if character.isspace():
                        continue
                    if character == "}" and not closed:
                        closed = True
                    else:
                        raise ValueError("Unexpected content after BrandedFoods array")
                tail = stream.read(chunk_size)
                if not tail:
                    break
            if not closed:
                raise ValueError("Missing closing JSON object")
            return
        try:
            item, end = decoder.raw_decode(buffer, position)
        except json.JSONDecodeError as exc:
            if len(buffer) - position > 16 * 1024 * 1024:
                raise ValueError("JSON record exceeds the supported 16 MiB bound") from exc
            more = stream.read(chunk_size)
            if not more:
                raise ValueError("Invalid or truncated branded-food JSON record") from exc
            buffer = buffer[position:] + more
            position = 0
            continue
        if not isinstance(item, dict):
            raise ValueError("Each BrandedFoods entry must be an object")
        yield item
        position = end
        after_item = True
        first_item = False
        if position > chunk_size:
            buffer = buffer[position:]
            position = 0


def finite_number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def reference_rows(food, nutrient_ids, aliases, relative_tolerance, absolute_tolerance):
    serving = finite_number(food.get("servingSize"))
    unit = aliases.get(str(food.get("servingSizeUnit", "")).strip().lower())
    nutrients = {}
    conflict = False
    for entry in food.get("foodNutrients") or []:
        nutrient_id = (entry.get("nutrient") or {}).get("id")
        if nutrient_id in nutrient_ids:
            amount = finite_number(entry.get("amount"))
            if nutrient_id in nutrients and nutrients[nutrient_id] != amount:
                conflict = True
            nutrients[nutrient_id] = amount
    labels = food.get("labelNutrients") or {}
    checks = mismatches = 0
    if serving is not None and serving > 0 and unit in ("g", "ml"):
        for nutrient_id in nutrient_ids:
            label = finite_number((labels.get(LABEL_FIELDS[nutrient_id]) or {}).get("value"))
            amount = nutrients.get(nutrient_id)
            if label is not None and amount is not None and (label != 0 or amount != 0):
                checks += 1
                expected = label * 100.0 / serving
                if label < 0 or amount < 0 or not math.isclose(amount, expected, rel_tol=relative_tolerance, abs_tol=absolute_tolerance):
                    mismatches += 1
    if conflict:
        status = "conflicting_json_nutrients"
    elif serving is None or serving <= 0 or unit not in ("g", "ml"):
        status = "invalid_serving_metadata"
    elif mismatches:
        status = "label_scale_mismatch"
    elif checks:
        status = "scale_consistent"
    else:
        status = "no_positive_scaling_anchor"
    return [{"fdc_id": food["fdcId"], "nutrient_id": nutrient_id,
             "json_amount": nutrients.get(nutrient_id),
             "label_amount": finite_number((labels.get(LABEL_FIELDS[nutrient_id]) or {}).get("value")),
             "json_serving_size": serving, "json_serving_unit": unit,
             "record_reference_status": status, "scale_checks": checks, "scale_mismatches": mismatches}
            for nutrient_id in nutrient_ids]


def write_json_reference(zip_path, output_path, beverage_ids, nutrient_ids, aliases,
                         relative_tolerance=0.01, absolute_tolerance=0.005):
    found = set()
    food_count = 0
    with zipfile.ZipFile(zip_path) as archive:
        names = [info.filename for info in archive.infolist() if info.filename.endswith(".json")]
        if len(names) != 1:
            raise ValueError("Expected one JSON member in the source archive")
        with archive.open(names[0]) as source, io.TextIOWrapper(source, encoding="utf-8-sig") as stream:
            with Path(output_path).open("x", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=REFERENCE_COLUMNS)
                writer.writeheader()
                for food in iter_foods(stream):
                    food_count += 1
                    food_id = food.get("fdcId")
                    if food_id not in beverage_ids:
                        continue
                    if food_id in found:
                        raise ValueError(f"Duplicate top-level fdcId in JSON export: {food_id}")
                    found.add(food_id)
                    writer.writerows(reference_rows(food, nutrient_ids, aliases,
                                                    relative_tolerance, absolute_tolerance))
    return {"archive_member": names[0], "total_json_foods": food_count,
            "matched_beverages": len(found), "beverages_not_in_json": len(beverage_ids) - len(found),
            "reference_rows": len(found) * len(nutrient_ids), "relative_tolerance": relative_tolerance,
            "absolute_tolerance": absolute_tolerance,
            "scope": "Export consistency check using label serving arithmetic; zero-only records have no numerical anchor"}
