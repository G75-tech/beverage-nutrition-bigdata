# Kiểm tra dữ liệu dinh dưỡng Checkpoint 3 Task 3

Trạng thái PASS. Snapshot `run_20261024T005433432881Z_3ba489_1`.

Một khóa kết quả là (fdc_id, nutrient_id). Giá trị thiếu giữ null. Giá trị 0 hợp lệ được giữ.
Trạng thái amount valid xác nhận kiểu số hữu hạn và không âm. Giá trị lớn được gắn cờ riêng để xem xét.
Ưu tiên giá trị hợp lệ hơn giá trị thiếu và chọn id nhỏ nhất khi giá trị không xung đột.
Nhiều giá trị hợp lệ khác nhau trong cùng khóa được đưa toàn bộ vào quarantine.

| Chất dinh dưỡng | Hồ sơ đồ uống | Có giá trị hợp lệ | Amount thiếu | Không có record | Xung đột | Chỉ có record lỗi |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Total Sugars | 152138 | 132165 | 0 | 19973 | 0 | 0 |
| Energy | 152138 | 142795 | 0 | 9343 | 0 | 0 |
| Sodium, Na | 152138 | 141600 | 0 | 10538 | 0 | 0 |
| Protein | 152138 | 143050 | 0 | 9088 | 0 | 0 |
| Total lipid (fat) | 152138 | 138915 | 0 | 13223 | 0 | 0 |

Record dinh dưỡng đủ điều kiện so sánh 690137.

| Trạng thái cơ sở đo | Số record dinh dưỡng |
| --- | ---: |
| documented_csv_serving_unit | 692209 |
| unrecognized_serving_unit | 6316 |

Nguồn tính chính: CSV USDA; đối chiếu dataset JSON: NOT_RUN (tạm tắt theo cấu hình).
amount đã chuẩn hóa trên 100 đơn vị được giữ nguyên; không chia theo khẩu phần thêm lần nữa.
Cơ sở 100 ml/100 g được xác định theo đơn vị khẩu phần CSV đã chuẩn hóa (ml/g), với khẩu phần hữu hạn và lớn hơn 0.
basis_status=documented_csv_serving_unit mô tả quy tắc theo tài liệu nguồn; không phải kết quả xác minh chéo JSON.
Số 0 hợp lệ được tính mà không cần mốc dương. Đơn vị chưa nhận diện hoặc khẩu phần không hợp lệ chưa tham gia KPI.

Dữ liệu dùng để so sánh cần eligible_for_comparison=true và cùng measurement_basis.
Các ngưỡng giá trị lớn là quy tắc sàng lọc để rà soát, được lưu trong config và evidence; dữ liệu nguồn được giữ.

Nguồn https://fdc.nal.usda.gov/GBFPD_Documentation/

clean_output `artifacts/checkpoint3/task03_nutrients/runs/pipeline_20261113T234449809822Z_e8689c/nutrients_clean.csv`, 698525 dòng.

quarantine_output `artifacts/checkpoint3/task03_nutrients/runs/pipeline_20261113T234449809822Z_e8689c/nutrients_quarantine.csv`, 0 dòng.

value_review_output `artifacts/checkpoint3/task03_nutrients/runs/pipeline_20261113T234449809822Z_e8689c/nutrients_value_review.csv`, 2074 dòng.

classified_output `artifacts/checkpoint3/task03_nutrients/runs/pipeline_20261113T234449809822Z_e8689c/nutrients_classified.csv`, 698525 dòng.

Runtime riêng Task 3 85.596 giây.
