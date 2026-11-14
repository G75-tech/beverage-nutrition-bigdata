# Chạy từng task và tìm kết quả Checkpoint 3

Mở PowerShell và vào thư mục:

```powershell
cd G:\BigDATA\beverage-nutrition-bigdata
```

Dùng tiếp `.venv`, Java 17, Spark local và raw của Checkpoint 2. Không cần ingest lại. Đợi `[PASS]` và đọc báo cáo mỗi task trước khi chạy bước tiếp theo. Runner tự bàn giao đường dẫn và hash đầu ra.

Mặc định `task3.basis_mode = "csv"` trong `config/checkpoint3.json`: nguồn dinh dưỡng là CSV USDA, không đọc hoặc yêu cầu dataset JSON.
Giữ nguyên amount đã chuẩn hóa theo 100 đơn vị; dùng đơn vị khẩu phần g/ml đã chuẩn hóa để tách 100 g/100 ml, kiểm tra khẩu phần hữu hạn và lớn hơn 0.
Các bước kiểm tra số hợp lệ, dữ liệu thiếu, xung đột và giá trị cần rà soát vẫn được giữ. Số 0 hợp lệ không cần mốc dương từ JSON.
Kết quả có `basis_status=documented_csv_serving_unit`, `basis_mode=csv`, `json_crosscheck_status=NOT_RUN` ở Task 3.
Đây là xác định cơ sở đo theo tài liệu và CSV, chưa xác minh chéo với dataset JSON.

Khi đổi chế độ/cấu hình, chạy Task 6 để tạo một chuỗi mới; hoặc bắt đầu lại Task 1 rồi tiếp tục các task tay.
Sau khi cập nhật JSON, đổi `basis_mode` thành `"json_crosscheck"`, cập nhật `basis_reference_zip` và `basis_reference_sha256`, rồi chạy lại Task 6.
Cấu hình lịch sử không có `basis_mode` tiếp tục dùng quy tắc JSON cũ để tránh thay đổi ý nghĩa của lần chạy đã lưu.

## Cấu trúc kết quả

```text
docs/evidence/checkpoint3/
  INDEX.md                         <- mục lục có link báo cáo/CSV
  state/                           <- trạng thái bàn giao tự động
  task01_input/
    latest.json                    <- lần PASS gần nhất
    last_attempt.json              <- lần thử gần nhất, kể cả FAIL
    runs/<run_id>/
      console.log                  <- stdout + stderr của tiến trình
      run_config.json              <- cấu hình tại thời điểm chạy
      input_contract.json          <- trạng thái, số dòng, hash, kiểm tra
      input_contract.md            <- báo cáo dễ đọc
      spark_baseline.json
      plans/*.txt                  <- explain("formatted")
  task02_beverages/runs/<run_id>/...
  task03_nutrients/runs/<run_id>/...
  task04_join/runs/<run_id>/...
  task05_kpis/runs/<run_id>/...
  task06_pipeline/runs/<run_id>/...

artifacts/checkpoint3/
  task02_beverages/runs/<run_id>/    <- CSV và schema Task 2
  task03_nutrients/runs/<run_id>/    <- CSV và schema Task 3
  task04_join/runs/<run_id>/         <- CSV và schema Task 4
  task05_kpis/runs/<run_id>/         <- CSV và schema Task 5
```

Tên task và `run_id` khớp giữa `docs` và `artifacts`. Task 1 chỉ đọc raw/kiểm tra nên không xuất CSV mới. Task 6 điều phối và đo đạc; CSV vẫn thuộc Task 2–5.

`run_id` gồm thời gian UTC và mã ngẫu nhiên. Mỗi run được giữ riêng. `latest.json` chứa `result_path`, `report_path`, `evidence_dir`, `artifacts_dir`, `console_log_path`; các đường dẫn tính từ gốc mã nguồn. `last_attempt.json` cho biết lần thử mới nhất lỗi hay thành công.

## Task 1 — đọc và kiểm tra bốn bảng raw

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 1
```

Mở `docs/evidence/checkpoint3/task01_input/latest.json`, lấy `report_path` để đọc `input_contract.md`. Xem số dòng, schema, null khóa, dung lượng và hash bốn bảng. JSON chi tiết là `input_contract.json`, kế hoạch thực thi ở `plans` cùng run.

Kết quả cần thấy: PASS, các bảng có dữ liệu và không null ở khóa được kiểm tra. Task này chưa kiểm tra toàn bộ khóa trùng hay chất lượng dinh dưỡng.

## Task 2 — lọc đồ uống

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 2
```

- Báo cáo: `task02_beverages/runs/<run_id>/filter_report.md` trong `docs/evidence/checkpoint3`.
- Xem nhanh: `preview.json`, `review_preview.json`, `category_mapping.json`, `decision_counts.json`.
- CSV trong `artifacts/checkpoint3/task02_beverages/runs/<run_id>`: `beverages.csv` (được chọn), `review.csv` (cần xem xét) và schema.
- `task3_input.json` là bàn giao tự động cho Task 3.

Kết quả cần thấy: PASS, đối chiếu số hồ sơ chọn/cần xem xét/ngoài phạm vi với input, đủ tám nhóm theo chính sách.

## Task 3 — làm sạch và kiểm tra cơ sở đo

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 3
```

- Báo cáo: `task03_nutrients/runs/<run_id>/nutrient_report.md`.
- Chi tiết: `nutrient_result.json`, `amount_quality.json`, `preview.json` và các preview rà soát.
- CSV ở nhánh artifacts cùng task/run: `nutrients_classified.csv`, `nutrients_clean.csv`, `nutrients_quarantine.csv`, `nutrients_value_review.csv`. `label_reference.csv` chỉ được tạo khi bật `json_crosscheck`.

Kết quả cần thấy: PASS; phân biệt dữ liệu sạch/cần rà soát/xung đột, cơ sở đo và điều kiện so sánh. Số 0 hợp lệ được giữ; giá trị thiếu không thay thành 0.

## Task 4 — nối bảng chi tiết

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 4
```

- Báo cáo: `task04_join/runs/<run_id>/join_report.md`.
- Xem nhanh: `preview.json`, `eligible_preview.json`; kiểm tra `join_result.json`.
- CSV ở artifacts: `beverage_nutrients.csv`, `coverage_by_nutrient.csv`, `coverage_by_group.csv` và schema.
- `task5_input.json` chốt đầu vào cho Task 5.

Kết quả cần thấy: PASS, bảng chi tiết và độ phủ sau join. Dùng `beverage_nutrients.csv` khi xem một sản phẩm cụ thể; đây chưa phải KPI trung bình theo nhóm.

## Task 5 — tổng hợp KPI

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 5
```

- Báo cáo: `task05_kpis/runs/<run_id>/kpi_report.md`.
- Xem nhanh: `preview.json`; kiểm tra trong `kpi_result.json`.
- CSV ở artifacts: `nutrition_kpis.csv`, `kpi_coverage.csv` và schema.

Kết quả cần thấy: PASS, KPI tách cơ sở 100 ml/100 g. Lần đầu `repeatability` là `NOT_RUN` vì chưa đối chiếu với lượt độc lập.

Chạy thêm một lượt trên đúng đầu vào để kiểm tra tính lặp lại:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 5 -Repeat
```

Lệnh tạo một run Task 5 mới; báo cáo phải ghi `repeatability: PASS`. Khi đó có thêm `task6_input.json`. CSV hai lần chạy được giữ riêng.

## Task 6 — chạy toàn bộ và đo baseline

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 6
```

Task 6 chạy lại **Task 1 → 2 → 3 → 4 → 5 → 5 repeat** để đo toàn chuỗi trên cùng snapshot. Nó không chỉ đọc kết quả bạn vừa chạy tay. Không bắt buộc chạy tay Task 1–5 trước. Script không có `-Task` cũng tương đương `-Task 6`.

Mở `docs/evidence/checkpoint3/task06_pipeline/latest.json` hoặc `docs/evidence/checkpoint3/latest_pipeline.json` sau PASS:

- `baseline.md`: thời gian, partition, môi trường và quality counts.
- `pipeline_result.json`: từng stage, đường dẫn log và kết quả.
- `run_config.json`: cấu hình cùng bàn giao đã chốt.
- `task7_input.json`: chuẩn bị bài nộp.
- `console.log`: log điều phối. Log Spark chi tiết ở evidence của từng task/run con; tra `stages[].log_path`.

Task con dùng cùng `run_id` với pipeline; lượt KPI thứ hai thêm `_repeat`. CSV vẫn ở nhánh artifacts của task tạo ra nó.

Baseline gồm khởi động/dừng Spark, hash, kiểm tra chất lượng, xuất/đọc lại CSV và thu kế hoạch. Cache hệ điều hành chưa được kiểm soát lạnh; partition quan sát không phải số máy.

## Task 7 — chuẩn bị báo cáo

Task 7 là tổng hợp báo cáo/slide, chưa có lệnh tự sinh bài nộp. Dùng `task7_input.json`, `baseline.md`, báo cáo Task 1–5 và `plans/*.txt` từ một pipeline PASS. Commit/push riêng khi chốt bài.

## Chọn run, chạy lại và xem log

```powershell
# Liệt kê lần thử và lần PASS mới nhất của mỗi task
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -List

# Ví dụ mở đúng evidence và artifacts của lần PASS Task 2 gần nhất
$result = Get-Content .\docs\evidence\checkpoint3\task02_beverages\latest.json -Raw | ConvertFrom-Json
Invoke-Item $result.evidence_dir
Invoke-Item $result.artifacts_dir

# Xem log lần thử mới nhất, kể cả FAIL
$attempt = Get-Content .\docs\evidence\checkpoint3\task02_beverages\last_attempt.json -Raw | ConvertFrom-Json
Get-Content $attempt.console_log_path -Tail 40
```

Mục lục [INDEX.md](evidence/checkpoint3/INDEX.md) có link trực tiếp. Để xem lần cũ, chọn `runs/<run_id>`; CSV/schema ở artifacts cùng task/run. Dùng bản sao khi sửa CSV bằng Excel vì đầu vào bước sau được kiểm tra SHA-256.

Khi chạy lại một task giữa chuỗi tay, chạy tiếp các task sau nó. Ví dụ chạy lại Task 2 thì tiếp tục 3 → 4 → 5; Task 3 bị chặn nếu Task 2 mới chưa PASS. Lịch sử PASS cũ vẫn trên đĩa và trong `latest.json`, nhưng trạng thái bàn giao không dùng nhầm đầu ra cũ. Pipeline và chuỗi tay có trạng thái riêng, dùng chung khóa chống chạy chồng.

Với snapshot/cấu hình khác, bắt đầu từ Task 1 hoặc Task 6:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1 -Task 1 -SnapshotRunId '<run_id INGESTED>'
# Truyền cùng SnapshotRunId cho các task tay tiếp theo.
# Cấu hình khác: thêm -TaskConfig '<đường dẫn JSON>' vào mỗi lệnh.
```

## Kết quả cũ

Kết quả Checkpoint 3 trước khi tổ chức lại được chuyển nguyên trạng sang:

`G:\BigDATA\checkpoint3-results-backup-20261113`

Giữ cấu trúc gốc `docs/evidence/checkpoint3`, `artifacts/checkpoint3`, `logs/checkpoint3`. `source_before` chứa bản sao các file sẽ sửa. `migration_manifest.json` ghi file, kích thước và SHA-256 đối chiếu sau di chuyển. Các đường dẫn bên trong bằng chứng cũ giữ nguyên nội dung; với đường dẫn kết quả cũ, tra từ gốc backup. Raw/dataset và kết quả Checkpoint 2 vẫn ở dự án.
