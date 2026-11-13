# Lọc đồ uống Checkpoint 3 Task 2

Trạng thái PASS. Snapshot `run_20261024T005433432881Z_3ba489_1`.

Input contract `G:\BigDATA\beverage-nutrition-bigdata\docs\evidence\checkpoint3\task01_input\runs\manual_20261113T154237605267Z_2ea579\input_contract.json`.

Input 1999950 dòng. Giữ 152138 dòng.
Category thiếu 10630 dòng trên toàn input.

Phạm vi gồm tám nhóm đồ uống không cồn dùng trực tiếp theo chính sách rtd_non_alcoholic_v2.
Cần xem xét 34138 dòng. Loại khỏi phạm vi 1813674 dòng.
Mỗi dòng có selection_status, selection_reason và preparation_status để truy vết quy tắc.
category_mapping.json ghi quyết định và số dòng cho từng category trong toàn input.
Đơn vị chuẩn hóa mô tả khẩu phần. Cơ sở đo dinh dưỡng sẽ được xác minh trong task sau.

| Nhóm đồ uống | Số dòng |
| --- | ---: |
| Water | 29557 |
| Soft drinks | 33981 |
| Juice and fruit drinks | 43744 |
| Ready-to-drink tea | 11105 |
| Ready-to-drink coffee | 3784 |
| Dairy milk | 19608 |
| Plant-based milk | 6594 |
| Sports and energy drinks | 3765 |

| Trạng thái đơn vị trong dữ liệu đã lọc | Số dòng |
| --- | ---: |
| recognized | 149835 |
| missing | 1781 |
| unrecognized | 522 |

CSV đầu ra `artifacts/checkpoint3/task02_beverages/runs/manual_20261113T154330548356Z_7a4457/beverages.csv`.

Schema đầu ra `artifacts/checkpoint3/task02_beverages/runs/manual_20261113T154330548356Z_7a4457/output_schema.json`.

Ghi bằng Python csv.writer qua iterator Spark để tương thích môi trường Windows hiện tại.
Mỗi lần chạy lưu trong một thư mục riêng. Đọc lại CSV bằng Spark để kiểm tra counts, null và đơn vị.
Chỉ dùng output của lần chạy có filter_result.json ghi PASS.

CSV cần xem xét `artifacts/checkpoint3/task02_beverages/runs/manual_20261113T154330548356Z_7a4457/review.csv`.

Quy tắc xác định dạng uống trực tiếp sử dụng category, description, khẩu phần và preparation_state_code.
Các trường hợp thiếu hoặc mâu thuẫn bằng chứng được đưa vào review thay vì tự gán dạng uống trực tiếp.
Preview nằm trong preview.json. Giá trị category và đơn vị gốc được giữ trong các cột gốc.

Runtime của riêng Task 2 105.15 giây.
