# Pitch Checkpoint 2 trong tối đa 4 phút

## 0 đến 40 giây — Architecture

Mở `docs/architecture.md`. Đề tài là so sánh thông tin dinh dưỡng đồ uống. Luồng hiện tại: CSV USDA có sẵn → quality gate → copy nguyên byte vào raw → manifest/log → Spark inspect. Đồ uống sẽ được lọc ở bước sau.

## 40 đến 90 giây — Environment

Mở `docs/evidence/environment.json`: Python 3.11.17, Java 17.0.20.1, Spark 3.5.9, `local[*]`. Count đúng 1,000,000; Python worker `[2, 3, 4]`. Demo lệnh `.\.venv\Scripts\python.exe -m src.env_check`. Runtime riêng và requirements được ghim; chưa thử máy thứ hai.

## 90 đến 140 giây — Source

Mở `metadata/source_usda.json` và inventory. Nguồn là USDA FDC Branded 04/2026, CSV/JSON archive mang nhãn 2026-04-30. Có 9 CSV, hai ZIP và workbook mô tả. USDA công bố CC0. URL/ngày tải gốc chưa ghi nhận. Bốn CSV ingest tổng 2,825,968,318 byte.

## 140 đến 210 giây — Evidence

Mở raw folder `data/raw/usda_branded_2026_04/ingest_date=2026-10-23/run_id=checkpoint2_20261023T062747094486Z_1` và `metadata/manifests/ingestion.jsonl`. Mở `docs/evidence/ingestion_three_runs.json`: lần 1: 4 INGESTED; lần 2: 4 SKIPPED_DUPLICATE; lần 3: 4 SKIPPED_DUPLICATE. Mở `raw_inspection.json`: schema 21 cột và 5 dòng. Nêu 13 tests PASS và 12/12 nguồn giữ nguyên hash/size/mtime. Chỉ ra tag `checkpoint-2` bằng `git show --no-patch checkpoint-2` sau khi hoàn tất commit/tag.

## 210 đến 240 giây — Blocker và trade-off

Repo chưa có remote; nhóm cần chọn repo/tài khoản và xác thực. Trước Buổi 7 cần chốt tiêu chí đồ uống/đơn vị dinh dưỡng, owner/deadline. Local Spark đủ để demo trên vài GB và dễ tái tạo; chưa chứng minh cluster. CSV multiLine giữ quoted records nhưng một file lớn khó chia partition. Không tự chấm điểm hoặc tuyên bố mọi thành viên đã hiểu; từng người cần giải thích phần mình.
