# Ngữ cảnh dự án so sánh dinh dưỡng đồ uống

Ngày đối chiếu: 23/10/2026 (Asia/Saigon).

## Cuộc chat nguồn

- Dự án ChatGPT: **Big Data**.
- Tên chat: **So sánh dinh dưỡng đồ uống**.
- Liên kết: https://chatgpt.com/c/6ab4fef2-5e18-83ec-8dbc-796cfe384961
- Mã cuộc chat: `6ab4fef2-5e18-83ec-8dbc-796cfe384961`.
- Mã dự án ChatGPT: `g-p-6ad87c10a9388191bbb6f708689355c3`.

Tệp này ghi lại phần ngữ cảnh đã đọc qua công cụ đọc chat. Đây là bản chụp ngữ cảnh, không phải cơ chế đồng bộ tự động hai chiều với ChatGPT. Khi cần thông tin mới hơn, đọc lại đúng cuộc chat nguồn.

## Thư mục mã nguồn do người dùng chọn

`G:\BigDATA\beverage-nutrition-bigdata`

Người dùng xác nhận trong chat nguồn rằng thư mục này dùng để chứa toàn bộ mã nguồn và dataset, và yêu cầu các thao tác sau này thực hiện tại đây. Trong chat hiện tại, người dùng yêu cầu liên kết ngữ cảnh để có thể làm việc trực tiếp tại thư mục đó.

## Thông tin đã xác nhận từ lời người dùng trong chat nguồn

1. Người dùng đã tải dataset dưới dạng CSV và JSON, phiên bản 2026. Các tệp thực tế đã thấy được liệt kê bên dưới; chưa đối chiếu nội dung hoặc nguồn tải.
2. Người dùng đang chuẩn bị cho checkpoint buổi 2 và muốn tiếp nối kết quả checkpoint buổi 1.
3. Người dùng đã hỏi cách tạo repository GitHub, thiết lập môi trường, Docker và kết nối mã nguồn với GitHub; cần hướng dẫn dễ theo dõi vì đã lâu chưa thao tác Git/GitHub.
4. Hai tài liệu được đính kèm trong chat nguồn:
   - `BigData_Buoi06_DoAn2_Brief_Checkpoint_Template.docx`
   - `BigData_Buoi06_DoAn2_Environment_Ingestion_RawStorage.pptx`

## Dataset đã thấy trong thư mục làm việc

Lần kiểm tra tiếp theo ngày 23/10/2026 thấy thư mục `Dataset` xuất hiện, gồm:

- `Dataset/FoodData_Central_branded_food_csv_2026-04-30.zip`: 448.767.220 byte.
- `Dataset/FoodData_Central_branded_food_json_2026-04-30.zip`: 204.270.542 byte.
- Thư mục `Dataset/FoodData_Central_branded_food_csv_2026-04-30/`, chứa thêm một thư mục cùng tên.

Tên các tệp thể hiện bộ FoodData Central branded food ngày 30/04/2026. Chưa đọc nội dung, kiểm tra tính toàn vẹn, thống kê bản ghi hay xác nhận đầy đủ quá trình giải nén. Không cần tải lại dữ liệu trước khi kiểm tra các tệp sẵn có.

## Giới hạn của phần ngữ cảnh đã đọc

- Công cụ trả về 5 lượt trao đổi gần nhất; nội dung trả lời của trợ lý hiển thị dưới dạng tham chiếu, không phải toàn văn. Không suy diễn các quyết định từ những tham chiếu này.
- Chưa đọc nội dung hai tài liệu đính kèm và chưa kiểm tra kết quả checkpoint buổi 1. Không coi tên tài liệu là bằng chứng cho một công nghệ hay yêu cầu cụ thể.
- Thư mục trống ở lần kiểm tra ban đầu; thư mục `Dataset` và các gói dữ liệu nêu trên xuất hiện ở lần kiểm tra tiếp theo. Chưa có mã nguồn xử lý dữ liệu được tạo trong thao tác liên kết này.
- Chưa xác nhận Git, Docker, môi trường Python, URL repository hoặc công nghệ xử lý dữ liệu của dự án.
- Yêu cầu hiện tại là tiếp nối ngữ cảnh và chuẩn bị làm việc tại thư mục được chọn; chưa phải yêu cầu triển khai toàn bộ pipeline.

## Cách tiếp tục

Thực hiện các lệnh với thư mục làm việc là `G:\BigDATA\beverage-nutrition-bigdata`. Trước khi triển khai phần phụ thuộc dữ liệu, kiểm tra các dataset thực tế trong thư mục và đối chiếu yêu cầu checkpoint có thể đọc được. Phân biệt rõ yêu cầu trực tiếp của người dùng, nội dung tài liệu tham khảo và đề xuất của trợ lý.

Để các chat mới tự phát hiện ngữ cảnh và có quyền ghi trong thư mục này, mở hoặc thêm chính thư mục trên làm dự án cục bộ trong ứng dụng. Phiên đang thiết lập liên kết ban đầu có thư mục làm việc là `G:\Data_User\Documents\ChatGPT\Big Data`; việc tạo các tệp ngữ cảnh không tự thay đổi cấu hình dự án của ứng dụng.

## Cập nhật cấu trúc ngày 23/10/2026

Người dùng đã yêu cầu khởi tạo cấu trúc dự án Python/PySpark và chuẩn bị code.
Thông tin trong mục này thay thế đường dẫn Dataset cũ ở bản chụp ban đầu:

- CSV đã giải nén và ZIP CSV: `datasets/usda_branded_2026/csv/`.
- ZIP JSON: `datasets/usda_branded_2026/json/`; chưa giải nén JSON.
- Nguồn/đích khi chuyển, kích thước và SHA-256: `metadata/dataset_layout_manifest.json`.
- Các tệp gốc đã đối chiếu hash trước/sau khi chuyển; chỉ dọn thư mục Dataset cũ nếu trống.
- Có README, requirements, Dockerfile, Compose và `src/main.py` để kiểm tra môi trường.
- Chưa triển khai ingestion, lọc đồ uống hoặc so sánh dinh dưỡng.
- Chưa tự khởi tạo Git/GitHub, cài môi trường vào hệ thống hoặc chạy Docker.

## Quyết định môi trường mới nhất: phương án A — local PySpark

Người dùng chọn chạy PySpark local trên Windows và yêu cầu bỏ Docker.
Quyết định này thay thế phần cấu hình môi trường Docker trong các ghi nhận trước đó.

- Đã bỏ `Dockerfile`, `compose.yaml` và `.dockerignore` khỏi thư mục dự án.
- README hướng dẫn `.venv`, Python 3.11, Java 17, `JAVA_HOME` và `PYSPARK_PYTHON`.
- Giữ PySpark 3.5.9 trong `requirements.txt` và chế độ Spark `local[2]` trong `src/main.py`.
- Chưa cài môi trường local vào máy hoặc xác nhận Spark khởi động thành công.


## Kết quả triển khai Checkpoint 2 ngày 23/10/2026

Mục này cập nhật trạng thái mới nhất, thay các ghi nhận "chưa chạy/chưa ingest" ở các bản chụp cũ:

- Đã đọc hai tài liệu môn học gốc tại G:\Downloads\Documents; report Markdown điền mục 4 và Exit ticket, kèm ma trận rubric.
- Runtime riêng trong .tools và .venv: Python 3.11.17, Temurin Java 17.0.20.1, PySpark/Spark 3.5.9, Py4J 0.10.9.7. Không thay cấu hình hệ thống.
- Smoke Spark local[*] đếm đúng 1.000.000 dòng; Python worker PASS; 13 unit tests PASS.
- Bốn CSV thật branded_food, food, food_nutrient, nutrient đã vào raw, tổng 2.825.968.318 byte. Ba run demo: 4 INGESTED, 4 SKIPPED_DUPLICATE, 4 SKIPPED_DUPLICATE.
- 12/12 tệp nguồn giữ nguyên hash/size/mtime. JSON giữ nguyên ZIP. Chưa lọc đồ uống hoặc ETL sâu.
- Spark inspect PASS với schema 21 cột và 5 dòng. Một lỗi mã hoá path dấu = đã sửa; evidence FAIL ban đầu được giữ.
- Xem docs/checkpoint2_report.md, docs/architecture.md, docs/PITCH_4_MINUTES.md và docs/evidence/validation.json. Gate kiểu dữ liệu chỉ kiểm tra tối đa 1000 record đầu; chưa đếm/validate toàn raw.
- Repo có sẵn, chưa cấu hình remote khi kiểm tra. Mốc nộp dự kiến là tag checkpoint-2 cục bộ; xác minh thực tế bằng git show checkpoint-2. Không tự tạo/chọn tài khoản GitHub.
- Tên nhóm/thành viên, owner/deadline chưa được cung cấp; cài sạch trên máy thứ hai chưa thử.
