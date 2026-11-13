# Bản ghi Checkpoint 3 trước khi tổ chức kết quả

Các lệnh và đường dẫn bên dưới là lịch sử triển khai. Dùng [hướng dẫn hiện tại](RUN_TASKS_CHECKPOINT3.md) để chạy. Kết quả lịch sử nằm ở `G:\BigDATA\checkpoint3-results-backup-20261113`; JSON cũ giữ nguyên byte và đường dẫn ghi tại thời điểm chạy.

## Checkpoint 3 Task 1 khóa input

Chạy từ thư mục repo với môi trường đã thiết lập ở Checkpoint 2.

```powershell
.\.venv\Scripts\python.exe -m src.input_contract
```

`config/checkpoint3.json` cố định lần ingest `run_20261024T005433432881Z_3ba489_1`. Chương trình chỉ nhận một sự kiện `INGESTED` cho mỗi CSV, cùng nguồn, release, dataset và run. Khi dùng một bản clone có lần ingest khác, truyền `--snapshot-run-id <run_id>` của lần đã ingest đủ bốn bảng trên máy đó.

Mỗi lần chạy tạo một thư mục riêng dưới `docs/evidence/checkpoint3/input_<run>/` chứa `input_contract.json` và `input_contract.md`. JSON lưu đường dẫn, byte, SHA-256, schema, read options, grain, khóa khai báo, số dòng, null từng cột và môi trường. File Markdown trình bày bảng input và schema để review.

Lần kiểm chứng ngày 11/11/2026 cho kết quả 1.999.950 dòng ở `branded_food`, 1.999.950 ở `food`, 26.027.437 ở `food_nutrient` và 477 ở `nutrient`. Các khóa đã khai báo đều có 0 null. Xem [input contract đã chạy](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/input_20261111T150022900983Z_838568/input_contract.md).

Task 1 đo toàn bộ record bằng schema đã khai báo. Uniqueness của khóa, khớp khóa ngoại và tính hợp lý của giá trị dinh dưỡng sẽ được kiểm tra trong các task xử lý tiếp theo. Runtime Task 1 được ghi riêng với baseline của pipeline hoàn chỉnh.

## Checkpoint 3 Task 2 lọc đồ uống

```powershell
.\.venv\Scripts\python.exe -m src.filter_beverages
```

`task2` trong `config/checkpoint3.json` chỉ định input contract Task 1 và `config/beverage_scope.json`. Scope gồm Water, Soft drinks, Juice and fruit drinks, Ready-to-drink tea, Ready-to-drink coffee, Dairy milk, Plant-based milk, Sports and energy drinks.

Code chọn ứng viên bằng category rồi nối `food.description` qua `fdc_id`, kiểm tra dimension duy nhất và matched/unmatched. Quy tắc phân loại xét category, mô tả, khẩu phần và preparation_state_code. Mỗi dòng ứng viên có selection_status, selection_reason và preparation_status. Kết quả phân loại theo quy tắc; hồ sơ mơ hồ được lưu riêng để xem xét.

Các dấu hiệu bột, cà phê hạt/xay, cô đặc cần pha, sữa đặc, protein shake, breakfast shake, thực phẩm dinh dưỡng chuyên biệt và món đông lạnh được xử lý theo policy. “From concentrate” được phân biệt với sản phẩm cô đặc cần pha. Sữa dừa chưa rõ dạng sử dụng và các hỗn hợp mơ hồ được chuyển sang review.

Lần chạy đã kiểm chứng có 1,999,950 dòng input, 152,138 dòng được chọn, 34,138 dòng cần xem xét và 1,813,674 dòng ngoài phạm vi. Mọi category được đối chiếu số lượng trong category_mapping.json. Xem [báo cáo bản tám nhóm](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/beverages_20261111T164146644628Z_447a43/filter_report.md).

CSV được chọn và CSV review nằm trong thư mục `artifacts/checkpoint3/beverages_20261111T164146644628Z_447a43/`, được Git bỏ qua. Mỗi CSV có schema và được Spark đọc lại để kiểm tra counts, null khóa và fdc_id duy nhất. Hai tập include/review đã kiểm tra không giao nhau. Các cột nguồn được giữ kèm mô tả và các cột phân loại. Đơn vị khẩu phần chuẩn hóa g/ml được ghi riêng; các trạng thái kiểm tra cơ sở đo dinh dưỡng được ghi ở Task 3.

Python csv.writer ghi các dòng qua iterator Spark để tương thích môi trường Windows hiện tại. Mỗi lần chạy dùng thư mục riêng, chỉ sử dụng output có filter_result.json ghi PASS. CSV lớn được giữ local; evidence nhỏ và policy dùng để review.

`task3` trong config cố định [bộ bàn giao đầu vào](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/beverages_20261111T164146644628Z_447a43/task3_input.json) của lần chạy này. Năm nutrient ID đang dùng là 2000 Total Sugars, 1008 Energy, 1093 Sodium, 1003 Protein và 1004 Total lipid (fat), đã đối chiếu tên/đơn vị với nutrient.csv. Kết quả kiểm tra amount, khóa (fdc_id, nutrient_id) và cơ sở đo được ghi trong mục Task 3 bên dưới.

## Checkpoint 3 Task 3 chất lượng dữ liệu dinh dưỡng

```powershell
.\.venv\Scripts\python.exe -m src.clean_nutrients
```

`task3.input_handoff_path` cố định bản tám nhóm của Task 2. Chương trình kiểm tra hash CSV/schema/evidence, chỉ dùng các hồ sơ include, rồi lấy năm nutrient ID từ food_nutrient raw. Amount được đọc dưới dạng text trước khi try_cast để ghi nhận missing, invalid, nonfinite và negative. Số 0 hợp lệ được giữ; dữ liệu thiếu giữ null.

Khóa kết quả là (fdc_id, nutrient_id). Với nhiều record, ưu tiên giá trị hợp lệ hơn missing, chọn id nhỏ nhất nếu không có xung đột giá trị, và đưa toàn bộ cặp có nhiều giá trị hợp lệ khác nhau vào quarantine. Các record lỗi số được lưu riêng. Classified CSV giữ amount_raw và trạng thái để truy vết.

Lần kiểm chứng ngày 12/11/2026 lấy 698,525 record dinh dưỡng từ 26.027.437 record raw cho 152.138 hồ sơ đồ uống. Kết quả có 0 cặp trùng và 0 record lỗi số/xung đột. Giá trị lớn vượt ngưỡng cấu hình được gắn cờ riêng để rà soát, có 2,074 record thuộc diện này. Chúng vẫn có trong dữ liệu kèm value_review_required và không được đánh dấu đủ điều kiện so sánh.

| Chất | Đơn vị tử số | Có record số hợp lệ | Chưa có record |
| --- | --- | ---: | ---: |
| Total Sugars | G | 132,165 | 19,973 |
| Energy | KCAL | 142,795 | 9,343 |
| Sodium, Na | MG | 141,600 | 10,538 |
| Protein | G | 143,050 | 9,088 |
| Total lipid (fat) | G | 138,915 | 13,223 |

Cơ sở đo được đối chiếu với ZIP JSON local đã bảo toàn hash ở Checkpoint 2. Khẩu phần, đơn vị và nutrient amount phải khớp CSV; phép tính label value × 100 / serving size dùng sai số tương đối 1% hoặc tuyệt đối 0.005 để xử lý làm tròn. Record có toàn số 0 không tạo được anchor kiểm tra tỷ lệ. Các fdc_id không có hồ sơ JSON cấp cao nhất hoặc chưa đủ bằng chứng được giữ trạng thái riêng. Đây là kiểm tra tính nhất quán của bản xuất, kèm đầy đủ nguồn và dung sai trong evidence.

Có 144,482 record dinh dưỡng được đánh dấu eligible_for_comparison=true. Khi so sánh phải giữ cùng nutrient_id, unit_name và measurement_basis. Numeric valid xác nhận kiểu số hữu hạn và không âm; các cờ value_review_required và basis_status cần được xét khi tính KPI.

CSV và schema được lưu trong `artifacts/checkpoint3/nutrients_20261112T031604398765Z_5980b9/`. Các đầu ra gồm nutrients_classified.csv, nutrients_clean.csv, nutrients_quarantine.csv, nutrients_value_review.csv và label_reference.csv. Artifacts lớn được Git bỏ qua. Xem [báo cáo Task 3](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/nutrients_20261112T031604398765Z_5980b9/nutrient_report.md) và nutrient_result.json trong cùng thư mục evidence.

`task3.result_path` và result_sha256 cố định kết quả này để tiếp tục bước join. Nguồn tham chiếu https://fdc.nal.usda.gov/GBFPD_Documentation/.

## Checkpoint 3 Task 4 nối bảng và đo chất lượng

```powershell
.\.venv\Scripts\python.exe -m src.join_beverage_nutrients
```

Đầu vào là Task 2 và Task 3 đã cố định bằng đường dẫn, SHA-256, schema và snapshot trong config. Tên/mô tả từ food.csv được dùng lại qua đầu ra đã kiểm chứng của Task 2. nutrient.csv được kiểm tra khóa duy nhất và đối chiếu ID, tên, đơn vị trước khi nối.

Đầu ra chính là beverage_nutrients.csv, grain (fdc_id, nutrient_id). Mỗi hồ sơ đồ uống có một vị trí cho mỗi chỉ số yêu cầu. 152,138 hồ sơ × 5 chỉ số tạo 760,690 dòng; 698,525 dòng gắn được record clean và 62,165 vị trí chưa có record nguồn. Đây là mở rộng các vị trí thiếu có chủ đích; khóa kết quả đã được kiểm tra duy nhất.

Các phép nối trái kiểm tra kiểu khóa, null, khóa trùng phía phải, matched/unmatched theo dòng và khóa, số dòng trước/sau. Bảng giữ description, thương hiệu, nhóm đồ uống, khẩu phần, nutrient_id, nutrient_name, amount, unit_name, measurement_basis và cờ chất lượng. food_nutrient_id là id bản ghi dinh dưỡng nguồn; fdc_id là id hồ sơ đồ uống.

nutrient_record_status phân biệt valid, missing_amount, no_source_record, conflicting_duplicate và invalid_only. source_record_present và clean_record_present chỉ rõ nguồn có record hay record có vào bảng clean. Dữ liệu thiếu giữ null; số 0 hợp lệ được giữ. Các giá trị cần rà soát và chưa đủ bằng chứng cơ sở đo vẫn nằm trong bảng đầy đủ.

| Chỉ số | Có số hợp lệ | Chưa có record | Đủ điều kiện so sánh |
| --- | ---: | ---: | ---: |
| Protein | 143,050 | 9,088 | 29,032 |
| Total lipid (fat) | 138,915 | 13,223 | 29,117 |
| Energy | 142,795 | 9,343 | 29,099 |
| Sodium, Na | 141,600 | 10,538 | 29,045 |
| Total Sugars | 132,165 | 19,973 | 28,189 |

Có 144,482 dòng đủ điều kiện so sánh theo quy tắc Task 3 và 2,074 dòng cần rà soát giá trị. Khi tính KPI ở Task 5 phải chọn eligible_for_comparison=true và cùng nutrient_id, unit_name, measurement_basis. Độ phủ dùng toàn bộ hồ sơ đã chọn của mỗi nhóm/chỉ số làm mẫu số; kết quả từ tập đủ điều kiện có thể không đại diện toàn bộ nhóm.

Ba CSV và schema được lưu tại artifacts/checkpoint3/joined_20261112T045435520489Z_7b189b/: beverage_nutrients.csv, coverage_by_nutrient.csv và coverage_by_group.csv. Hai bảng coverage đo độ phủ, chưa tính KPI trung bình dinh dưỡng. Xem [báo cáo Task 4](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/joined_20261112T045435520489Z_7b189b/join_report.md), join_result.json và task5_input.json trong cùng thư mục evidence. Artifacts lớn được Git bỏ qua.

Kết quả đã đọc lại bằng Spark và kiểm tra số dòng, null khóa, khóa duy nhất, số record clean, cờ đủ điều kiện và đối chiếu coverage với Task 3. task4.result_path và task5.input_handoff_path trong config cố định lần chạy này để tiếp tục Task 5.

## Checkpoint 3 Task 5 KPI dinh dưỡng

```powershell
.\.venv\Scripts\python.exe -m src.aggregate_nutrition_kpis
```

KPI ở bước này là thống kê tổng hợp để so sánh thông tin dinh dưỡng theo nhóm đồ uống. Đầu vào là bảng Task 4 đã khóa bằng schema, SHA-256 và snapshot; chương trình đọc lại toàn bộ, kiểm tra grain và đối chiếu độ phủ trước khi tính.

Đầu ra nutrition_kpis.csv có 80 dòng, grain (beverage_group, nutrient_id, unit_name, measurement_basis): tám nhóm × năm chỉ số × hai cơ sở đo. Mỗi dòng giữ tên nhóm/chỉ số, đơn vị, cơ sở đo, n_beverages_total, n_numeric_available, n_eligible_any_basis, n_compared, coverage_pct_all_beverages, mean_amount, median_amount, min_amount, max_amount, p25_amount, p75_amount và kpi_status.

Mean là trung bình số học; median là trung vị; p25/p75 là phân vị 25% và 75% tính chính xác bằng Spark percentile có nội suy tuyến tính. Các thống kê được làm tròn đến sáu chữ số sau dấu thập phân. Mỗi hồ sơ fdc_id đủ điều kiện có trọng số bằng nhau. Cơ sở 100 ml và 100 g được tách riêng. Giá trị 0 hợp lệ tham gia tính, giá trị thiếu giữ null.

Chỉ dùng eligible_for_comparison=true và kiểm tra lại cờ rà soát, kiểu số, đơn vị/cơ sở đo. Có 144,482 record dinh dưỡng tham gia các thống kê, 80 dòng KPI có kết quả và 0 dòng chưa có mẫu đủ điều kiện. Dòng chưa có mẫu giữ n_compared=0, các thống kê null và kpi_status=NO_ELIGIBLE_RECORDS.

n_compared là số hồ sơ thực sự dùng trong cơ sở đo của dòng hiện tại. coverage_pct_all_beverages = n_compared / n_beverages_total × 100, với mẫu số toàn bộ hồ sơ trong nhóm. kpi_coverage.csv có 40 dòng, giữ chất lượng/độ phủ Task 4 và số hồ sơ dùng theo từng cơ sở đo. Kết quả mô tả tập đủ điều kiện; độ phủ cần được trình bày cùng khi so sánh. Đơn vị đếm là fdc_id; bước này chưa chọn phiên bản mới nhất theo GTIN hoặc dùng trọng số doanh số.

Ví dụ kết quả đường tổng theo 100 ml từ lần chạy đã chốt:

| Nhóm | Số hồ sơ dùng tính | Đường trung bình (g/100 ml) | Trung vị | Độ phủ (%) |
| --- | ---: | ---: | ---: | ---: |
| Dairy milk | 3916 | 6.856047 | 5.0 | 19.9714 |
| Juice and fruit drinks | 9056 | 9.13389 | 9.58 | 20.7023 |
| Plant-based milk | 1196 | 3.023503 | 2.905 | 18.1377 |
| Ready-to-drink coffee | 795 | 5.745321 | 5.64 | 21.0095 |
| Ready-to-drink tea | 2330 | 5.472584 | 5.2 | 20.9815 |
| Soft drinks | 7478 | 8.632105 | 10.83 | 22.0064 |
| Sports and energy drinks | 798 | 3.669386 | 3.8 | 21.1952 |
| Water | 2245 | 3.09314 | 2.06 | 7.5955 |

Hai lần chạy độc lập trên cùng snapshot và chính sách đã được đối chiếu trực tiếp từng ô của cả hai CSV. Khóa, count, đơn vị và trạng thái phải khớp chính xác; sai số số thực tương đối/tuyệt đối cho phép là 1e-9. Reference là kpis_20261112T045724745919Z_58b559; lần chốt là kpis_20261112T045845608995Z_696b8e. Lệnh chạy mặc định dùng reference đã ghi trong config để tiếp tục kiểm tra khả năng tái lập.

CSV và schema lưu tại artifacts/checkpoint3/kpis_20261112T045845608995Z_696b8e/. Xem [báo cáo Task 5](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/kpis_20261112T045845608995Z_696b8e/kpi_report.md), kpi_result.json, preview.json và task6_input.json trong cùng thư mục evidence. Task 6 tiếp tục đóng gói entry point và đo baseline toàn pipeline; runtime riêng Task 5 được ghi trong báo cáo này.

Trong lần triển khai này, CSV Task 4 cũ không còn khớp hash và có thay đổi định dạng mã/ngày. Đã giữ tệp đó, tái tạo bảng join vào thư mục mới từ Task 2–3 đã khóa và xác minh byte khớp bản chốt ban đầu. Xem [bằng chứng tái tạo đầu vào](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/kpis_20261112T045345969030Z_ab6ebd/input_recovery.json).

## Checkpoint 3 Task 6 chạy toàn pipeline và baseline

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\RUN_CHECKPOINT3.ps1
```

Một lệnh thực hiện Task 1–5 và chạy KPI lần hai độc lập. Mỗi lượt có run_config.json riêng, lấy đúng đầu ra mới của stage trước và kiểm tra exit code, snapshot, đường dẫn/hash bàn giao. Cấu hình gốc được giữ cho các lệnh chạy từng task. Chuỗi dừng khi có lỗi; con trỏ latest chỉ cập nhật khi toàn pipeline PASS.

Lượt baseline đã kiểm chứng: pipeline_20261112T125939670061Z_e2cfd6, tổng wall time 370.122 giây. Bốn CSV raw có 2,825,968,318 byte; ZIP JSON tham chiếu có 204,270,542 byte, ghi riêng. Đây là lượt chạy trên toàn snapshot hiện tại bằng Spark local, bao gồm kiểm tra hash/chất lượng, khởi động/dừng Spark, xuất/đọc lại CSV và đo đạc. Cache hệ điều hành không được kiểm soát về trạng thái lạnh.

| Stage | Wall time (giây) |
| --- | ---: |
| task1 | 29.389 |
| task2 | 77.527 |
| task3 | 114.233 |
| task4 | 94.126 |
| task5 | 24.433 |
| task5_repeat | 24.528 |

Đầu ra giữ 152,138 hồ sơ đồ uống, 760,690 vị trí chỉ số, 144,482 record đủ điều kiện và 80 dòng KPI. Hai lần tính KPI đạt đối chiếu trên cùng bảng join mới của lượt chạy.

Mỗi stage có spark_baseline.json và plans/*.txt chứa explain(formatted), số partition quan sát, Spark/Java/Python, master, JVM heap, parallelism và SQL configuration. Các quy tắc nghiệp vụ Task 1–5 được giữ; phần bổ sung là cấu hình bàn giao, điều phối và baseline. Phạm vi kết quả vẫn là bảng chi tiết Task 4 và KPI nhóm Task 5.

Xem [hướng dẫn chạy và đọc baseline](docs/RUN_CHECKPOINT3.md), [con trỏ kết quả mới nhất](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/latest_pipeline.json) và [báo cáo baseline](../../checkpoint3-results-backup-20261113/docs/evidence/checkpoint3/pipeline_20261112T125939670061Z_e2cfd6/baseline.md). Log chi tiết nằm trong logs/checkpoint3/pipeline_20261112T125939670061Z_e2cfd6/. Task 7 dùng task7_input.json của lượt chạy để chuẩn bị bài nộp, bằng chứng, pitch và exit ticket.
