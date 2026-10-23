# So sánh dinh dưỡng đồ uống — Checkpoint 2 Big Data

Dự án chạy trực tiếp tại `G:\BigDATA\beverage-nutrition-bigdata` bằng Python 3.11, Java 17 và PySpark 3.5.9 trên Windows. Checkpoint 2 gồm môi trường, ingestion CSV theo batch, lưu raw nguyên byte và provenance. Kết quả chạy thật, giới hạn kiểm chứng và phần còn thiếu nằm trong [báo cáo](docs/checkpoint2_report.md).

Nguồn chính là USDA FoodData Central Branded Foods 04/2026. Các CSV và hai ZIP đã tải nằm trong `datasets/usda_branded_2026/`. JSON chỉ được kiểm kê. Open Food Facts là nguồn dự phòng cho bước sau. Chưa lọc đồ uống, nối bảng, chuẩn hoá chất dinh dưỡng hoặc làm dashboard.

## Bắt đầu từ đâu

| Vị trí | Nội dung |
| --- | --- |
| `docs/architecture.md` | Sơ đồ, luồng chạy và giải thích từng nhóm file |
| `docs/checkpoint2_report.md` | Báo cáo dựa trên evidence thực tế |
| `config/project.json` | Nguồn, release, schema và bốn CSV mặc định |
| `src/env_check.py` | Môi trường và smoke test một triệu dòng |
| `src/ingest_file.py` | Ingestion giữ nguyên nội dung nguồn |
| `src/inspect_raw.py` | Đọc raw bằng Spark với schema rõ ràng |
| `metadata/source_usda.json` | Thông tin nguồn và các trường chưa xác nhận |
| `metadata/manifests/ingestion.jsonl` | Mọi lần thành công, skip và lỗi |
| `docs/evidence/` | Kết quả kiểm thử, versions, hashes, schema và sample |
| `logs/checkpoint2/` | Log console, log UTC và bản cấu hình cũ; Git bỏ qua |

## Thiết lập một lần

Mở PowerShell và chạy:

```powershell
cd 'G:\BigDATA\beverage-nutrition-bigdata'
.\setup.ps1
```

`setup.ps1` cần một Python 3.11+ có sẵn qua `py -3` để tải runtime. Có thể chỉ định interpreter khác:

```powershell
.\setup.ps1 -BootstrapPython 'C:\duong-dan\python.exe'
```

Script tải Python 3.11 độc lập và Temurin JDK 17 vào `.tools/`, đối chiếu SHA-256 với metadata nhà phát hành, tạo `.venv/` và cài `requirements.txt`. Không đổi PATH/JAVA_HOME toàn máy. Các URL và checksum chính xác đã chọn nằm trong `docs/evidence/runtime_downloads.json`; máy mới dùng lại các giá trị này. Cần mạng cho lần tải đầu và khoảng 6 GB trống ngoài dataset (runtime, PySpark và raw). Máy hiện có khoảng trống thực tế được ghi trong inventory.

Nếu PowerShell chặn script, dùng tuỳ chọn chỉ cho tiến trình đang chạy, không đổi policy toàn máy:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\setup.ps1
```

`requirements.txt` ghim PySpark 3.5.9 và Py4J 0.10.9.7. PySpark SQL ở checkpoint này không cần pandas/Arrow. [Tài liệu Apache](https://spark.apache.org/docs/3.5.9/api/python/getting_started/install.html) nêu Java 8/11/17; dự án kiểm thử tổ hợp Python 3.11/Java 17. Không cần Docker hoặc cài dịch vụ HDFS/Kafka.

## Chạy mỗi lần làm việc

`.ps1` là script điều phối PowerShell. Python trong `.venv` chạy mã ứng dụng. Không phải kích hoạt venv, và không chạy từng module được import.

```powershell
cd 'G:\BigDATA\beverage-nutrition-bigdata'

# 1. Smoke test: SparkSession local[*], count đúng 1.000.000 và thử Python worker
.\.venv\Scripts\python.exe -m src.env_check

# 2. Ingest bốn CSV mặc định, nguồn chỉ đọc
.\.venv\Scripts\python.exe -m src.ingest_file

# Hoặc chỉ ingest một CSV thật
.\.venv\Scripts\python.exe -m src.ingest_file --source datasets/usda_branded_2026/csv/branded_food.csv

# 3. Đọc raw branded_food với schema tường minh và hiện 5 dòng
.\.venv\Scripts\python.exe -m src.inspect_raw

# Gộp smoke, ingest ba lần và inspect
.\RUN_CHECKPOINT2.ps1
```

Raw nằm ở `data/raw/usda_branded_2026_04/ingest_date=<ngày UTC>/run_id=<mã lần chạy>/`. Mỗi run có ID mới. Nguồn không đổi sẽ được `SKIPPED_DUPLICATE` sau khi đối chiếu checksum toàn file nguồn/raw; không tạo thêm bản sao. Thay đổi nội dung sẽ tạo raw trong run mới. Bản raw cũ hỏng/thiếu làm run `FAILED`; không tự ghi đè. Tất cả skip và lỗi đều được ghi vào manifest và log UTC. CLI trả 0 khi batch thành công hoặc tất cả duplicate, trả 1 nếu có lỗi.

Gate kiểm tra file tồn tại, khác rỗng, đuôi `.csv`, header, độ rộng và kiểu số trong tối đa 1000 record đầu. Bản copy giữ nguyên byte, gồm BOM/line endings nếu có. Hash xác minh toàn file; type gate và Spark sample chưa kiểm tra toàn bộ record. UPC là string để giữ số 0 đầu.

## Kiểm thử và evidence

```powershell
# Fixture nhỏ, không đụng dataset gốc
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# Bộ kiểm chứng đầy đủ, cập nhật evidence: smoke + tests + 3 lần ingest + inspect + hash nguồn
.\.venv\Scripts\python.exe -m scripts.validate_checkpoint

.\.venv\Scripts\python.exe -m pip check
```

Evidence JSON do lệnh ghi thực tế; chạy lại sẽ cập nhật các bản tổng hợp. Manifest và log ingestion tiếp tục append. `inventory_before.json` là baseline gốc của lần triển khai; không chạy lại `scripts.inventory` lên tệp này nếu muốn giữ bằng chứng trước/sau. Không coi timestamp file là ngày tải dataset.

Nếu lỗi `JAVA_HOME`, kiểm tra `.tools/runtime.json` và `.tools/java17/.../bin/java.exe`. Nếu lỗi PySpark, xem `logs/checkpoint2/environment_console.log` hoặc `environment.log` và chạy `pip check`. Cảnh báo không tìm thấy native Hadoop/winutils có thể xuất hiện trên Windows; căn cứ PASS/FAIL thực tế trong `environment.json` và `raw_inspection.json`. Không tải binary winutils không rõ nguồn chỉ để xoá cảnh báo.

Nếu có `ingestion.lock` sau crash, xác nhận tiến trình ghi đã dừng rồi đọc log và đối chiếu partial/raw với manifest trước khi can thiệp. Không xoá dataset hoặc sửa manifest để bỏ qua lỗi. Xem [kiến trúc](docs/architecture.md) cho hành vi sau crash.

## Git và nộp checkpoint

Repository `.git` đã tồn tại từ trước. Không chạy `git init` lại. `.gitignore` loại datasets, raw, samples, ZIP/CSV, `.env`, `.venv`, `.tools` và logs. Manifest nhỏ và evidence JSON được giữ để review. Báo cáo Git ghi trạng thái thực tế trong `docs/evidence/git_safety.json`.

Chưa có remote ở lần kiểm kê. Sau khi tự tạo hoặc chọn repo GitHub đúng tài khoản, người dùng cấu hình:

```powershell
git remote add origin https://github.com/TAI_KHOAN_CUA_BAN/TEN_REPO.git
git remote -v
git ls-remote origin
git status --short
```

Chỉ commit/tag sau khi kiểm tra file staged không có dataset hoặc secrets. Nếu chưa có commit/tag checkpoint, chạy sau khi review:

```powershell
git add -- .gitignore README.md requirements.txt setup.ps1 RUN_CHECKPOINT2.ps1 config src scripts tests docs metadata
git diff --cached --stat
git diff --cached --check
git commit -m "Implement checkpoint 2 local Spark ingestion and raw evidence"
git tag -a checkpoint-2 -m "Checkpoint 2 verified local PySpark ingestion"
```

Khi remote và xác thực đã được kiểm tra, người dùng có thể push:

```powershell
git push -u origin main
git push origin checkpoint-2
```

Không có tài khoản/repo GitHub nào được tự chọn hoặc tạo. Lệnh commit/tag trong hướng dẫn chỉ dùng nếu chưa tồn tại, tránh trùng tag.

## Tài liệu môn học

Đã đọc hai bản gốc tại `G:\Downloads\Documents`: `BigData_Buoi06_DoAn2_Environment_Ingestion_RawStorage.pptx` và `BigData_Buoi06_DoAn2_Brief_Checkpoint_Template.docx`. Report Markdown điền đủ các trường mục 4 và Exit ticket, kèm ma trận evidence theo rubric. Thông tin tên nhóm/thành viên, owner và hạn do người dùng xác nhận sau; không tự bịa. Nguồn `.docx`/`.pptx` được giữ nguyên. Báo cáo Markdown không sao chép layout Word của toàn handout.
