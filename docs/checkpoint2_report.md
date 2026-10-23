# Báo cáo Checkpoint 2 môn Big Data

Đề tài: **So sánh thông tin dinh dưỡng đồ uống**. Phần kỹ thuật environment, ingestion và raw storage đã chạy thật với trạng thái tổng hợp **PASS**. Bốn CSV có tổng 2,825,968,318 byte đã vào raw và hai lần chạy lại không tạo thêm bản sao. Chưa làm ETL sâu hoặc lọc đồ uống.

Ngày thực hiện: 23/10/2026 (Asia/Saigon). Báo cáo tổng hợp lúc `2026-10-23T07:39:57.985530Z`. Bản Markdown này điền các trường mục 4 và Exit ticket của template Word; không tái tạo layout toàn bộ handout.

## Các trường theo mẫu nộp

| Trường | Nội dung đã xác nhận |
| --- | --- |
| Tên nhóm / thành viên | **Chưa được cung cấp**; cần nhóm tự điền trước khi nộp |
| Đề tài | So sánh thông tin dinh dưỡng đồ uống |
| Git commit/tag | Mốc checkpoint cục bộ: `checkpoint-2`; hash chính xác tra bằng `git rev-parse checkpoint-2^{commit}`. Remote chưa được cấu hình ở lần kiểm kê; chưa push |
| Environment | Python **3.11.17**, Java **17.0.20.1** theo output `java -version`, PySpark/Spark **3.5.9**, Py4J **0.10.9.7**, mode **local[*]**, 12 parallelism |
| Data source | USDA FoodData Central Branded Foods release **04/2026**. Tên ZIP cục bộ mang nhãn `2026-04-30`. [Trang tải chính thức](https://fdc.nal.usda.gov/download-datasets/); input `datasets/usda_branded_2026/csv/` |
| Ingestion command | `.\.venv\Scripts\python.exe -m src.ingest_file` |
| Raw location | `data/raw/usda_branded_2026_04/ingest_date=2026-10-23/run_id=checkpoint2_20261023T062747094486Z_1/` |
| Manifest/log location | `metadata/manifests/ingestion.jsonl`; `logs/checkpoint2/ingestion.log`; log riêng từng run trong `logs/checkpoint2/` |
| Rerun behavior | lần 1: 4 INGESTED; lần 2: 4 SKIPPED_DUPLICATE; lần 3: 4 SKIPPED_DUPLICATE. Raw vẫn 4 CSV, không overwrite hoặc append record |
| Blocker còn lại | Chưa có remote GitHub và chưa xác thực remote; chưa có tên nhóm/thành viên, owner, deadline. Không có blocker kỹ thuật với pipeline đã chạy |
| Owner + deadline | **Chưa được cung cấp/chốt**. Nhóm cần chỉ định người cấu hình GitHub và người kiểm tra tiêu chí đồ uống trước Buổi 7; không tự gán tên hoặc hạn |

## Environment và khả năng tái tạo

Tổ hợp thực tế: `3.11.17 (main, Oct 21 2026, 17:46:24) [MSC v.1944 64 bit (AMD64)]`; Java output:

```text
openjdk version "17.0.20.1" 2026-08-18
OpenJDK Runtime Environment Temurin-17.0.20.1+1 (build 17.0.20.1+1)
OpenJDK 64-Bit Server VM Temurin-17.0.20.1+1 (build 17.0.20.1+1, mixed mode, sharing)
```

SparkSession chạy `local[*]`, `spark.range(1_000_000).count()` trả **1,000,000**, khớp expected **1,000,000**. Thử Python worker trả `[2, 3, 4]`. Log: `logs/checkpoint2/environment_console.log`; bằng chứng có thể commit: `docs/evidence/environment.json`.

`setup.ps1` tải runtime riêng vào `.tools/`, kiểm tra checksum, tạo `.venv` và cài hai dependency ghim phiên bản. URL, SHA-256 và phiên bản runtime nằm trong `docs/evidence/runtime_downloads.json`; package thực tế ở `docs/evidence/pip_freeze.txt`. `pip check` đã trả “No broken requirements found.” Runtime hệ thống Python 3.14/JDK 25 không được dùng để chạy Spark và không bị thay đổi. Cài sạch trên máy thứ hai **chưa được chạy**.

## Nguồn dữ liệu và provenance

Đã kiểm kê **12 tệp nguồn** gồm 9 CSV, một workbook mô tả trường và hai ZIP CSV/JSON, tổng **3,744,895,341 byte**. Bốn CSV ưu tiên có thể đọc từ ổ đĩa local. JSON chưa giải nén/ingest; chỉ đọc central directory ZIP để kiểm kê member. Chưa kiểm tra toàn bộ CRC của từng ZIP member.

Trang USDA xác nhận Branded 04/2026 có CSV và JSON. [USDA công bố CC0 1.0 và yêu cầu ghi nguồn](https://fdc.nal.usda.gov/); metadata nguồn đã lưu attribution. URL tải ban đầu và ngày tải thực tế **không có bằng chứng**, để `null` trong `metadata/source_usda.json`. Nhãn release dựa trên tên archive cục bộ và trang phát hành; hash cục bộ dùng xác minh bảo toàn, chưa có checksum upstream độc lập để khẳng định nguồn tải ban đầu. Open Food Facts chưa ingest.

## Raw và ba lần chạy thật

| CSV | Byte | SHA-256 toàn file |
| --- | ---: | --- |
| `branded_food.csv` | 954,002,257 | `36195ac3a901e9900742fdb3ae812fb0837c12d6daaf4f0997b53f20a825d59e` |
| `food.csv` | 375,881,194 | `7a93b9b348bd9b8e2b52f93feecd831a8f6527d7ab197b75b374bec246fb5868` |
| `food_nutrient.csv` | 1,496,063,311 | `39ee9145992ba65839b0c5cb8aff605b6af2f9aca8f3b431d2ddee53f582a34c` |
| `nutrient.csv` | 21,556 | `226ba937d1a73e8c87bd7fdcdd577524699bef4025c9232ee474b7d3083498ff` |

Tổng raw: **2,825,968,318 byte**, **4 CSV**. Raw path có dataset, ngày ingest UTC và run ID. Quality gate kiểm tra file tồn tại/khác 0, `.csv`, header đúng thứ tự, độ rộng record và kiểu số trong tối đa 1000 record đầu; `nutrient.csv` ít hơn nên kiểm tra hết phần record có sẵn. SHA-256 bao phủ toàn file nguồn và bản sao. Không đổi encoding, delimiter, line endings hoặc nội dung raw.

| Lần | Run ID | Trạng thái file | Batch |
| --- | --- | --- | --- |
| 1 | `checkpoint2_20261023T062747094486Z_1` | {'INGESTED': 4} | PASS |
| 2 | `checkpoint2_20261023T062747094486Z_2` | {'SKIPPED_DUPLICATE': 4} | PASS |
| 3 | `checkpoint2_20261023T062747094486Z_3` | {'SKIPPED_DUPLICATE': 4} | PASS |

Bằng chứng: `docs/evidence/ingestion_three_runs.json` và **12 sự kiện** trong manifest tương ứng ba run được liệt kê ở trên. Lần chạy thêm của người dùng sẽ append sự kiện mới vào journal, nhưng không tăng raw nếu checksum không đổi. Trước mỗi skip, chương trình kiểm tra lại bản raw cũ. Nếu raw hỏng/thiếu, batch thất bại và dừng ở file lỗi đầu tiên, giữ các file đã thành công và không sửa raw cũ.

Đối chiếu trước/sau: **12/12 tệp nguồn PASS** về hash, size và mtime. Evidence `docs/evidence/source_preservation.json` còn giữ hash trước/sau. Không di chuyển, xoá hoặc sửa nguồn trong lần triển khai này.

## Spark đọc raw và kết quả kiểm thử

Spark inspect đọc đúng raw path trong manifest, in schema **21 cột**, hiển thị **5 record**. `fdc_id` là long, `serving_size` là double, UPC giữ string; các trường khác theo config. Sample và schema lưu ở `docs/evidence/raw_inspection.json`. Các sample đầu có dầu ăn/súp vì checkpoint giữ toàn nguồn, chưa lọc đồ uống. Chưa đếm toàn bộ raw hoặc chứng minh mọi record hợp schema.

| Kiểm tra | Kết quả cuối |
| --- | --- |
| environment | **PASS** |
| unit_tests | **PASS** |
| ingestion_1 | **PASS** |
| ingestion_2 | **PASS** |
| ingestion_3 | **PASS** |
| raw_inspection | **PASS** |
| original_sources_unchanged | **PASS** |
| three_run_deduplication | **PASS** |

Unit tests: **13 PASS**, **0 failure**, **0 error**, **0 skipped**. Fixture tạm kiểm tra đầu vào thiếu/rỗng/header/type/malformed, copy nguyên byte, checksum đổi, dedupe 3 lần, raw hỏng, lock, journal hỏng và chống ghi đè. Các ca cố ý tạo lỗi được đánh giá PASS khi chương trình từ chối đúng. Dataset thật không chịu các thao tác phá hỏng của fixture.

Lần kiểm tra ban đầu có **1 lỗi đã sửa** ở Spark inspection: `Path.as_uri()` mã hoá dấu `=` thành `%3D` trong folder, Hadoop tìm sai path. Đã chuyển sang đường dẫn Windows dạng slash không mã hoá; bước inspect chạy lại và PASS. Giữ `docs/evidence/validation_initial.json` và `raw_inspection_initial_failure.json` để truy vết, không sửa bằng chứng FAIL thành một lần chạy chưa từng xảy ra. `validation.json` tổng hợp các kiểm tra PASS và liên kết lần thất bại ban đầu.

Cảnh báo native Hadoop/winutils có xuất hiện. Spark smoke và đọc CSV vẫn hoàn thành với exit code 0. Console inspect còn có thông báo từ cleanup process của Spark trên Windows sau `spark.stop()`; không tải thêm winutils. Manifest/log ứng dụng là UTC, dòng console JVM Spark dùng múi giờ Windows (Asia/Saigon).

## Đối chiếu rubric và evidence

| Hạng mục trong tài liệu | Evidence hiện có | Giới hạn |
| --- | --- | --- |
| Environment reproducible (2.0) | Setup, requirements, runtime hashes, versions, smoke PASS | Chưa thử clean install trên máy thứ hai |
| Source & provenance (1.5) | Metadata, inventory, release, nguồn local truy cập được, CC0 và attribution | Thiếu lịch sử URL/ngày tải gốc |
| Ingestion job (2.5) | Script, 3 run thật, gate, dedupe, test lỗi | Chỉ batch file local |
| Raw storage (2.0) | 4 CSV, dataset/date/run, checksums, manifest, bảo toàn 12 nguồn | Raw local, chưa distributed |
| Repo & report (1.0) | README, Git safety evidence, mốc `checkpoint-2`, report và pitch | Chưa cấu hình remote/push |
| Team understanding (1.0) | Architecture, luồng gọi, trade-off và Exit ticket | Chưa đánh giá thuyết trình/hiểu bài của từng thành viên |

Không tự chấm điểm đạt; giảng viên xác nhận theo rubric. Hai tài liệu gốc đã đọc từ `G:\Downloads\Documents`, giữ nguyên, hashes ở `docs/evidence/requirements_review.json`. DOCX mục 4 cung cấp các trường báo cáo/Exit ticket; PPTX slide 8, 10, 17, 18, 24, 28, 31, 32 cung cấp yêu cầu đối chiếu.

## Exit ticket

1. **Một lệnh duy nhất kiểm tra environment:** `.\.venv\Scripts\python.exe -m src.env_check`. Cách gọi trực tiếp tương thích template: `.\.venv\Scripts\python.exe src/env_check.py`.
2. **Nguồn và version/snapshot:** USDA FDC Branded 04/2026; giữ tên hai archive `2026-04-30`, inventory SHA-256, metadata nguồn và release trong mọi manifest event. Ngày ingest UTC là thông tin riêng, không phải ngày phát hành/tải.
3. **Chạy ingestion 3 lần:** evidence hiện tại ghi lần 1: 4 INGESTED; lần 2: 4 SKIPPED_DUPLICATE; lần 3: 4 SKIPPED_DUPLICATE; tổng raw vẫn bốn CSV. Khi nguồn đã tồn tại từ trước, cả ba run đều có thể skip. Mỗi run có ID/timestamp riêng để audit.
4. **Rủi ro lớn nhất trước Buổi 7:** chưa chốt tiêu chí đồ uống và đơn vị/chất dinh dưỡng dùng để so sánh. Chưa xử lý việc đó ở checkpoint này. Owner/deadline chờ nhóm xác nhận. Việc nộp GitHub còn cần người dùng cấu hình remote và xác thực.

## Các bước còn lại do người dùng thực hiện

- Điền tên nhóm/thành viên và chốt owner/deadline; chuẩn bị giải thích luồng dữ liệu bằng `docs/PITCH_4_MINUTES.md`.
- Tạo/chọn đúng repo GitHub, cấu hình remote và xác thực; chưa có tài khoản/repo nào được tự tạo hoặc chọn. Đọc `docs/evidence/git_safety.json` và kiểm tra staged files trước push. Xem README cho lệnh.
- Trước checkpoint sau, xác định tiêu chí đồ uống và cách chuẩn hoá dinh dưỡng. Không dùng sample đầu làm danh sách đồ uống hoặc tuyên bố đã hoàn tất ETL.
