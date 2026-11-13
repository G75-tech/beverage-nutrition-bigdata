# Input contract Checkpoint 3 Task 1

Trạng thái PASS. Snapshot `run_20261024T005433432881Z_3ba489_1`.

Counts và null khóa được đo trên toàn bộ record bằng Spark với schema khai báo.
Grain và khóa chính là khai báo thiết kế. Uniqueness và join sẽ kiểm tra ở task sau.

| File | Số dòng | Byte | Null ở khóa | Partition quan sát |
| --- | ---: | ---: | --- | ---: |
| branded_food.csv | 1999950 | 954002257 | {'fdc_id': 0} | 1 |
| food.csv | 1999950 | 375881194 | {'fdc_id': 0} | 1 |
| food_nutrient.csv | 26027437 | 1496063311 | {'id': 0, 'fdc_id': 0, 'nutrient_id': 0} | 1 |
| nutrient.csv | 477 | 21556 | {'id': 0} | 1 |

## branded_food.csv

Grain `One branded food record per fdc_id`.

Khóa chính khai báo `['fdc_id']`. Khóa join `['fdc_id']`.

Raw `data/raw/usda_branded_2026_04/ingest_date=2026-10-24/run_id=run_20261024T005433432881Z_3ba489_1/branded_food.csv`.

SHA-256 `36195ac3a901e9900742fdb3ae812fb0837c12d6daaf4f0997b53f20a825d59e`.

```text
root
 |-- fdc_id: long (nullable = true)
 |-- brand_owner: string (nullable = true)
 |-- brand_name: string (nullable = true)
 |-- subbrand_name: string (nullable = true)
 |-- gtin_upc: string (nullable = true)
 |-- ingredients: string (nullable = true)
 |-- not_a_significant_source_of: string (nullable = true)
 |-- serving_size: double (nullable = true)
 |-- serving_size_unit: string (nullable = true)
 |-- household_serving_fulltext: string (nullable = true)
 |-- branded_food_category: string (nullable = true)
 |-- data_source: string (nullable = true)
 |-- package_weight: string (nullable = true)
 |-- modified_date: string (nullable = true)
 |-- available_date: string (nullable = true)
 |-- market_country: string (nullable = true)
 |-- discontinued_date: string (nullable = true)
 |-- preparation_state_code: string (nullable = true)
 |-- trade_channel: string (nullable = true)
 |-- short_description: string (nullable = true)
 |-- material_code: string (nullable = true)

```

## food.csv

Grain `One food record per fdc_id`.

Khóa chính khai báo `['fdc_id']`. Khóa join `['fdc_id']`.

Raw `data/raw/usda_branded_2026_04/ingest_date=2026-10-24/run_id=run_20261024T005433432881Z_3ba489_1/food.csv`.

SHA-256 `7a93b9b348bd9b8e2b52f93feecd831a8f6527d7ab197b75b374bec246fb5868`.

```text
root
 |-- fdc_id: long (nullable = true)
 |-- data_type: string (nullable = true)
 |-- description: string (nullable = true)
 |-- food_category_id: long (nullable = true)
 |-- publication_date: string (nullable = true)
 |-- market_country: string (nullable = true)
 |-- trade_channel: string (nullable = true)
 |-- microbe_data: string (nullable = true)

```

## food_nutrient.csv

Grain `One nutrient-value record per id; a food can have multiple nutrient records`.

Khóa chính khai báo `['id']`. Khóa join `['fdc_id', 'nutrient_id']`.

Raw `data/raw/usda_branded_2026_04/ingest_date=2026-10-24/run_id=run_20261024T005433432881Z_3ba489_1/food_nutrient.csv`.

SHA-256 `39ee9145992ba65839b0c5cb8aff605b6af2f9aca8f3b431d2ddee53f582a34c`.

```text
root
 |-- id: long (nullable = true)
 |-- fdc_id: long (nullable = true)
 |-- nutrient_id: long (nullable = true)
 |-- amount: double (nullable = true)
 |-- data_points: long (nullable = true)
 |-- derivation_id: long (nullable = true)
 |-- min: double (nullable = true)
 |-- max: double (nullable = true)
 |-- median: double (nullable = true)
 |-- footnote: string (nullable = true)
 |-- min_year_acquired: long (nullable = true)

```

## nutrient.csv

Grain `One nutrient definition per id`.

Khóa chính khai báo `['id']`. Khóa join `['id']`.

Raw `data/raw/usda_branded_2026_04/ingest_date=2026-10-24/run_id=run_20261024T005433432881Z_3ba489_1/nutrient.csv`.

SHA-256 `226ba937d1a73e8c87bd7fdcdd577524699bef4025c9232ee474b7d3083498ff`.

```text
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- unit_name: string (nullable = true)
 |-- nutrient_nbr: string (nullable = true)
 |-- rank: double (nullable = true)

```

Runtime của riêng Task 1 35.902 giây.

Chi tiết schema, read options, môi trường và phạm vi kiểm tra nằm trong input_contract.json.
