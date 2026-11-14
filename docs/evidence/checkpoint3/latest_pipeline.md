# Baseline Checkpoint 3 toàn pipeline

Trạng thái: PASS.
Pipeline run: `pipeline_20261114T000129685710Z_fc0ef0`.
Snapshot raw: `run_20261024T005433432881Z_3ba489_1`.

Phạm vi: Task 1 → 2 → 3 → 4 → 5, sau đó chạy KPI độc lập lần hai để đối chiếu.
Đầu vào bắt đầu từ raw đã ingestion ở Checkpoint 2; giữ dữ liệu nguồn hiện tại.
Mỗi stage có cấu hình bàn giao mới trong run_config.json và một tiến trình Spark local riêng.
Thời gian bao gồm khởi động/dừng Spark, hash dữ liệu, kiểm tra chất lượng, ghi/đọc lại CSV và thu thập kế hoạch thực thi.
Đây là một lượt đo trên máy local; cache hệ điều hành không được ép về trạng thái lạnh.

Chế độ cơ sở đo: csv; dùng dataset JSON: False.
CSV raw logic: 2825968318 byte; ZIP JSON được dùng: 0 byte.
Dung lượng logic tính mỗi tệp một lần; lượng I/O thực tế có thể lớn hơn do kiểm tra và đọc lại.

| Stage | Wall time (s) | Runtime bên trong (s) | Partition quan sát |
| --- | ---: | ---: | --- |
| task1 | 30.927 | 30.827 | branded_food=1, food=1, food_nutrient=1, nutrient=1 |
| task2 | 108.95 | 108.799 | beverage_filter=13 |
| task3 | 97.982 | 97.83 | nutrient_scope=1, nutrition_clean=13 |
| task4 | 107.196 | 107.049 | beverage_nutrient_join=200 |
| task5 | 29.051 | 28.884 | nutrition_kpis=1 |
| task5_repeat | 29.01 | 28.862 | nutrition_kpis=1 |

Tổng wall time trình điều phối: 408.808 giây.
Tổng thời gian các tiến trình stage: 403.116 giây.
Phần xác minh/điều phối còn lại: 5.692 giây.

## Kết quả kiểm tra

- selected_beverages: 152138
- review_beverages: 34138
- clean_nutrient_records: 698525
- joined_slots: 760690
- missing_source_slots: 62165
- value_review_records: 2074
- eligible_nutrient_records: 690137
- kpi_rows: 80
- coverage_rows: 40
- independent_kpi_repeat: PASS

Spark version, Java, Python, JVM heap, local master, default parallelism và SQL configuration nằm trong spark_baseline.json của mỗi stage.
Partition quan sát và explain(formatted) được thu trên frame thực tế, theo cấu hình AQE đang dùng.
Các tệp plans/*.txt có SHA-256 trong baseline; chúng phục vụ phân tích/tối ưu ở bước tiếp theo.
Source hash và cấu hình giúp xác định đúng mã đã chạy dù checkout đang có thay đổi chưa commit.
Log chi tiết console.log nằm trong thư mục evidence của từng task/run; stage lỗi dừng chuỗi và giữ kết quả thành công trước đó.
Bảng KPI giữ cơ sở 100 ml và 100 g riêng, dùng tập đủ điều kiện và kèm độ phủ theo chính sách Task 5.
