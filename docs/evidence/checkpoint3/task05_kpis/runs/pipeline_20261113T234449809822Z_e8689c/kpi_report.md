# Checkpoint 3 Task 5: KPI dinh dưỡng

Trạng thái: PASS.
Snapshot: `run_20261024T005433432881Z_3ba489_1`.

KPI ở đây là số liệu tổng hợp để so sánh thông tin dinh dưỡng giữa các nhóm đồ uống.
Grain: (beverage_group, nutrient_id, unit_name, measurement_basis). Hai cơ sở 100 ml và 100 g được tách riêng.
Mỗi hồ sơ FDC đủ điều kiện có trọng số bằng nhau trong từng nhóm/chỉ số/cơ sở đo.
Số 0 hợp lệ tham gia tính; chỉ số thiếu giữ null và không tham gia mẫu số trung bình.
Chỉ dùng eligible_for_comparison=true; các cờ và điều kiện cơ sở đo được kế thừa Task 3–4.
Nguồn tính: CSV USDA. Cơ sở đo theo đơn vị khẩu phần đã chuẩn hóa; đối chiếu dataset JSON: NOT_RUN.

Các thống kê: mean (trung bình), median (trung vị), min, max, p25, p75 (các phân vị 25% và 75%).
Phân vị tính bằng Spark percentile chính xác, có nội suy tuyến tính; trung vị là phân vị 50%.
Giá trị thống kê xuất với tối đa 6 chữ số sau dấu thập phân.
n_compared là số hồ sơ thực sự tham gia phép tính trong cơ sở đo của dòng hiện tại.
coverage_pct_all_beverages = n_compared / n_beverages_total × 100; mẫu số giữ toàn bộ hồ sơ trong nhóm.
Nhóm không có mẫu đủ điều kiện giữ các thống kê null, n_compared=0 và kpi_status=NO_ELIGIBLE_RECORDS.

Số dòng KPI: 80; có kết quả: 80; chưa có mẫu: 0.
Tổng record dinh dưỡng được dùng: 690137.
Đối chiếu hai lần chạy độc lập: NOT_RUN.

| Chỉ số | Có số hợp lệ | Đủ điều kiện cả hai cơ sở đo | Theo 100 ml | Theo 100 g |
| --- | ---: | ---: | ---: | ---: |
| Protein | 143050 | 141750 | 140359 | 1391 |
| Total lipid (fat) | 138915 | 137608 | 136233 | 1375 |
| Energy | 142795 | 140648 | 139261 | 1387 |
| Sodium, Na | 141600 | 140232 | 138865 | 1367 |
| Total Sugars | 132165 | 129899 | 128588 | 1311 |

CSV nutrition_kpis.csv chứa đủ năm chỉ số. Bảng dưới đây xem nhanh riêng chỉ số Đường.

| Nhóm đồ uống | Cơ sở đo | Số hồ sơ đủ điều kiện | Trung bình đường | Trung vị đường | Độ phủ (%) |
| --- | --- | ---: | ---: | ---: | ---: |
| Dairy milk | per_100_g | 180 | 23.500444 | 15.065 | 0.918 |
| Juice and fruit drinks | per_100_g | 638 | 9.914984 | 8.85 | 1.4585 |
| Plant-based milk | per_100_g | 155 | 5.316645 | 2.5 | 2.3506 |
| Ready-to-drink coffee | per_100_g | 18 | 5.827778 | 7.795 | 0.4757 |
| Ready-to-drink tea | per_100_g | 126 | 9.659841 | 3.8 | 1.1346 |
| Soft drinks | per_100_g | 117 | 11.433504 | 8.82 | 0.3443 |
| Sports and energy drinks | per_100_g | 21 | 7.001429 | 0.0 | 0.5578 |
| Water | per_100_g | 56 | 5.605 | 0.555 | 0.1895 |
| Dairy milk | per_100_ml | 18074 | 6.731313 | 5.0 | 92.1767 |
| Juice and fruit drinks | per_100_ml | 39674 | 9.030283 | 9.58 | 90.6959 |
| Plant-based milk | per_100_ml | 5845 | 4.665829 | 2.92 | 88.6412 |
| Ready-to-drink coffee | per_100_ml | 3132 | 5.564773 | 5.0 | 82.7696 |
| Ready-to-drink tea | per_100_ml | 9662 | 6.096768 | 5.0 | 87.0059 |
| Soft drinks | per_100_ml | 30620 | 8.210183 | 10.56 | 90.1092 |
| Sports and energy drinks | per_100_ml | 3203 | 3.483565 | 3.61 | 85.073 |
| Water | per_100_ml | 18378 | 1.584519 | 0.0 | 62.1782 |

Đường có đơn vị G theo cơ sở đo của dòng: G/100 ml hoặc G/100 g.
Kết quả mô tả các hồ sơ đủ điều kiện trong snapshot; độ phủ thấp cần được nêu cùng khi so sánh.
Mỗi fdc_id là một hồ sơ nguồn; phép tính chưa chọn phiên bản mới nhất theo GTIN hoặc dùng trọng số doanh số.
Tên/mô tả từng đồ uống vẫn ở bảng Task 4; bảng KPI tổng hợp theo nhóm.

output: `artifacts/checkpoint3/task05_kpis/runs/pipeline_20261113T234449809822Z_e8689c/nutrition_kpis.csv` (80 dòng).
coverage_output: `artifacts/checkpoint3/task05_kpis/runs/pipeline_20261113T234449809822Z_e8689c/kpi_coverage.csv` (40 dòng).

Runtime riêng Task 5: 24.439 giây.
