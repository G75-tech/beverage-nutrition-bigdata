# Checkpoint 3 Task 5: KPI dinh dưỡng

Trạng thái: PASS.
Snapshot: `run_20261024T005433432881Z_3ba489_1`.

KPI ở đây là số liệu tổng hợp để so sánh thông tin dinh dưỡng giữa các nhóm đồ uống.
Grain: (beverage_group, nutrient_id, unit_name, measurement_basis). Hai cơ sở 100 ml và 100 g được tách riêng.
Mỗi hồ sơ FDC đủ điều kiện có trọng số bằng nhau trong từng nhóm/chỉ số/cơ sở đo.
Số 0 hợp lệ tham gia tính; chỉ số thiếu giữ null và không tham gia mẫu số trung bình.
Chỉ dùng eligible_for_comparison=true; các cờ và điều kiện cơ sở đo được kế thừa Task 3–4.

Các thống kê: mean (trung bình), median (trung vị), min, max, p25, p75 (các phân vị 25% và 75%).
Phân vị tính bằng Spark percentile chính xác, có nội suy tuyến tính; trung vị là phân vị 50%.
Giá trị thống kê xuất với tối đa 6 chữ số sau dấu thập phân.
n_compared là số hồ sơ thực sự tham gia phép tính trong cơ sở đo của dòng hiện tại.
coverage_pct_all_beverages = n_compared / n_beverages_total × 100; mẫu số giữ toàn bộ hồ sơ trong nhóm.
Nhóm không có mẫu đủ điều kiện giữ các thống kê null, n_compared=0 và kpi_status=NO_ELIGIBLE_RECORDS.

Số dòng KPI: 80; có kết quả: 80; chưa có mẫu: 0.
Tổng record dinh dưỡng được dùng: 144482.
Đối chiếu hai lần chạy độc lập: PASS.

| Nhóm đồ uống | Cơ sở đo | Số hồ sơ | Trung bình đường | Trung vị đường | Độ phủ (%) |
| --- | --- | ---: | ---: | ---: | ---: |
| Dairy milk | per_100_g | 58 | 24.731724 | 13.66 | 0.2958 |
| Juice and fruit drinks | per_100_g | 171 | 11.781053 | 9.58 | 0.3909 |
| Plant-based milk | per_100_g | 31 | 5.657419 | 2.92 | 0.4701 |
| Ready-to-drink coffee | per_100_g | 18 | 5.827778 | 7.795 | 0.4757 |
| Ready-to-drink tea | per_100_g | 30 | 12.327667 | 3.8 | 0.2701 |
| Soft drinks | per_100_g | 39 | 10.568462 | 9.3 | 0.1148 |
| Sports and energy drinks | per_100_g | 8 | 3.31125 | 0.0 | 0.2125 |
| Water | per_100_g | 20 | 5.23 | 0.145 | 0.0677 |
| Dairy milk | per_100_ml | 3916 | 6.856047 | 5.0 | 19.9714 |
| Juice and fruit drinks | per_100_ml | 9056 | 9.13389 | 9.58 | 20.7023 |
| Plant-based milk | per_100_ml | 1196 | 3.023503 | 2.905 | 18.1377 |
| Ready-to-drink coffee | per_100_ml | 795 | 5.745321 | 5.64 | 21.0095 |
| Ready-to-drink tea | per_100_ml | 2330 | 5.472584 | 5.2 | 20.9815 |
| Soft drinks | per_100_ml | 7478 | 8.632105 | 10.83 | 22.0064 |
| Sports and energy drinks | per_100_ml | 798 | 3.669386 | 3.8 | 21.1952 |
| Water | per_100_ml | 2245 | 3.09314 | 2.06 | 7.5955 |

Đường có đơn vị G theo cơ sở đo của dòng: G/100 ml hoặc G/100 g.
Kết quả mô tả các hồ sơ đủ điều kiện trong snapshot; độ phủ thấp cần được nêu cùng khi so sánh.
Mỗi fdc_id là một hồ sơ nguồn; phép tính chưa chọn phiên bản mới nhất theo GTIN hoặc dùng trọng số doanh số.
Tên/mô tả từng đồ uống vẫn ở bảng Task 4; bảng KPI tổng hợp theo nhóm.

output: `artifacts/checkpoint3/task05_kpis/runs/pipeline_20261113T154538688059Z_b15d77_repeat/nutrition_kpis.csv` (80 dòng).
coverage_output: `artifacts/checkpoint3/task05_kpis/runs/pipeline_20261113T154538688059Z_b15d77_repeat/kpi_coverage.csv` (40 dòng).

Runtime riêng Task 5: 24.918 giây.
