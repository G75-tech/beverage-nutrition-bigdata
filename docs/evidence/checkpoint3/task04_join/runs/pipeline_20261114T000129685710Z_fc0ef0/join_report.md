# Checkpoint 3 Task 4: nối bảng và đo chất lượng

Trạng thái: PASS. Snapshot: `run_20261024T005433432881Z_3ba489_1`.

Grain: một dòng cho mỗi (fdc_id, nutrient_id) trong danh sách đồ uống × năm chỉ số đã chọn.
Các vị trí chưa có dữ liệu giữ amount=null, có nutrient_record_status; giá trị 0 hợp lệ được giữ.
Tên/mô tả đã được nối từ food.csv ở Task 2, tiếp tục dùng qua CSV/schema được xác minh SHA-256.
Các phép nối trái kiểm tra kiểu khóa, null, khóa trùng phía phải và số dòng trước/sau.

| Phép nối | Khóa | Trái trước | Phải trước | Sau | Khớp trái | Chưa khớp trái | Khóa phải chưa dùng |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| clean_to_nutrient | nutrient_id | 698525 | 5 | 698525 | 698525 | 0 | 0 |
| clean_to_beverage | fdc_id | 698525 | 152138 | 698525 | 698525 | 0 | 8071 |
| slots_to_source_pair_status | fdc_id, nutrient_id | 760690 | 698525 | 760690 | 698525 | 62165 | 0 |
| slots_to_clean_nutrient | fdc_id, nutrient_id | 760690 | 698525 | 760690 | 698525 | 62165 | 0 |

Số hồ sơ đồ uống được giữ: 152138.
Số vị trí chỉ số: 760690.
Có record clean: 698525; chưa có record nguồn: 62165.
Đủ điều kiện so sánh: 690137; cần rà soát giá trị: 2074.

| Chỉ số | Hồ sơ đồ uống | Có số hợp lệ | Chưa có record | Đủ điều kiện so sánh | Độ phủ đủ điều kiện (%) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Protein | 152138 | 143050 | 9088 | 141750 | 93.172 |
| Total lipid (fat) | 152138 | 138915 | 13223 | 137608 | 90.4495 |
| Energy | 152138 | 142795 | 9343 | 140648 | 92.4476 |
| Sodium, Na | 152138 | 141600 | 10538 | 140232 | 92.1742 |
| Total Sugars | 152138 | 132165 | 19973 | 129899 | 85.3824 |

Cột brand_name có thể thiếu; description được giữ từ nguồn và có thể là mô tả chung.
eligible_for_comparison kế thừa quy tắc Task 3, chưa xác minh độc lập toàn bộ cơ sở đo.
Khi tính KPI phải lọc eligible_for_comparison=true, nhóm cùng nutrient_id, unit_name và measurement_basis.
Giữ mẫu số toàn bộ hồ sơ của mỗi nhóm/chỉ số để báo độ phủ; tập đủ điều kiện có thể không đại diện toàn bộ nhóm.
Các dòng bị gắn cờ và thiếu bằng chứng cơ sở đo vẫn nằm trong bảng đầy đủ.

CSV chính: `artifacts/checkpoint3/task04_join/runs/pipeline_20261114T000129685710Z_fc0ef0/beverage_nutrients.csv`.

Runtime riêng Task 4: 107.049 giây.
