# Công việc sau Checkpoint 2

1. Bổ sung tên nhóm, thành viên, owner và hạn do người dùng xác nhận; dùng report đã điền theo mục 4 của template làm nội dung nộp. Hai tài liệu đã đọc tại `G:\Downloads\Documents`.
2. Người dùng chọn repo GitHub/tài khoản, thêm remote và xác thực. Kiểm tra file staged và evidence trước commit/tag/push; xem trạng thái thật trong report.
3. Lưu bản source URL/ngày tải gốc nếu còn lịch sử tải; không suy diễn từ ngày ingest hoặc mtime.
4. Ở checkpoint kế tiếp mới chốt tiêu chí nhận diện đồ uống, nối `fdc_id`, định nghĩa chất dinh dưỡng/đơn vị và kiểm tra dữ liệu toàn bộ.
5. Chỉ ingest Open Food Facts nếu đề bài hoặc nhu cầu so sánh đa nguồn yêu cầu. JSON USDA vẫn giữ ZIP gốc, chưa cần giải nén.

Môi trường, ingest bốn CSV, kiểm tra chạy lại, Spark schema và evidence được triển khai ở Checkpoint 2. Không tự mở rộng sang ETL sâu hoặc dashboard.
