# Baseline Checkpoint 3 toàn pipeline

Trạng thái: FAIL.
Pipeline run: `pipeline_20261113T233019481932Z_71d991`.
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
| task1 | 72.885 | 72.759 | branded_food=1, food=1, food_nutrient=1, nutrient=1 |
| task2 | 87.06 | 86.956 | beverage_filter=13 |

Tổng wall time trình điều phối: 384.746 giây.
Tổng thời gian các tiến trình stage: 159.945 giây.
Phần xác minh/điều phối còn lại: 224.801 giây.


Spark version, Java, Python, JVM heap, local master, default parallelism và SQL configuration nằm trong spark_baseline.json của mỗi stage.
Partition quan sát và explain(formatted) được thu trên frame thực tế, theo cấu hình AQE đang dùng.
Các tệp plans/*.txt có SHA-256 trong baseline; chúng phục vụ phân tích/tối ưu ở bước tiếp theo.
Source hash và cấu hình giúp xác định đúng mã đã chạy dù checkout đang có thay đổi chưa commit.
Log chi tiết console.log nằm trong thư mục evidence của từng task/run; stage lỗi dừng chuỗi và giữ kết quả thành công trước đó.
Bảng KPI giữ cơ sở 100 ml và 100 g riêng, dùng tập đủ điều kiện và kèm độ phủ theo chính sách Task 5.

Lỗi: task3 exited 1; detailed log: G:\BigDATA\beverage-nutrition-bigdata\docs\evidence\checkpoint3\task03_nutrients\runs\pipeline_20261113T233019481932Z_71d991\console.log
