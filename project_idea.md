# ĐỀ TÀI DỰ ÁN BIG DATA: PHÂN TÍCH TÌNH HÌNH CHUYẾN BAY TẠI MỸ (18.2 TRIỆU RECORD)

## 1. Tổng quan dự án (Project Overview)
**Tên đề tài:** Khai phá dữ liệu Hàng không Mỹ: Phân tích các yếu tố ảnh hưởng đến độ trễ và hủy chuyến bay.
**Nguồn dữ liệu:** Dataset `Algo0o/flight_delay` (Hugging Face) chứa 18.2 triệu chuyến bay nội địa Mỹ.
**Công nghệ sử dụng:** Apache Spark (PySpark) để xử lý dữ liệu lớn, Python (Pandas/Matplotlib) hoặc PowerBI/Tableau để trực quan hóa.
**Mục tiêu (Problem Statement):** Pandas và Excel truyền thống không thể xử lý mượt mà tập dữ liệu 1.8GB (18 triệu dòng) này. Dự án này tận dụng sức mạnh xử lý phân tán của Big Data (PySpark) để phân tích bộ dữ liệu khổng lồ nhằm tìm ra các Insight (thông tin chi tiết) hữu ích cho hành khách: Hãng bay nào uy tín nhất, thời điểm nào dễ bị delay nhất, và những sân bay nào thường xuyên gặp sự cố.

---

## 2. Các câu hỏi phân tích chính (Business Questions / Insights)
Nhóm sẽ dùng PySpark để truy vấn 18.2 triệu dòng và trả lời 3 nhóm câu hỏi nhằm tạo ra giá trị thực tế:

**Insight 1: Đánh giá uy tín Hãng hàng không (Carrier Analysis)**
*   Hãng hàng không nào có tỷ lệ hủy chuyến trung bình cao nhất?
*   Hãng hàng không nào delay thời gian lâu nhất trung bình mỗi chuyến bay?
*   *Mục đích:* Giúp hành khách né tránh các hãng bay có lịch sử "cao su" tệ nhất.

**Insight 2: Điểm đen hàng không (Airport Analysis)**
*   Sân bay xuất phát (Origin) nào là "nỗi ác mộng" delay với tổng số phút trễ kinh khủng nhất?
*   Tuyến bay (Route: Origin -> Destination) phổ biến nào thường xuyên bị trễ giờ nhất?
*   *Mục đích:* Khuyến cáo khách hàng dự trù thêm thời gian khi bay qua các "điểm đen" này.

**Insight 3: Khung giờ & Thời điểm vàng (Temporal Analysis)**
*   Khung giờ cất cánh nào trong ngày (Sáng sơ/Trưa/Chiều tối) an toàn nhất để không bị delay dây chuyền?
*   Sự khác biệt về tỷ lệ delay giữa các tháng (Ví dụ: Tháng mùa đông tuyết rơi dày so với tháng mùa hè)?
*   *Mục đích:* Hỗ trợ khách hàng ra quyết định chọn giờ cất cánh tối ưu.

---

## 3. Phân công công việc (Team Roles - Dành cho 3 thành viên)

### Thành viên 1: Data Engineer (Kỹ sư Dữ liệu)
*   Chịu trách nhiệm về "Cơ sở hạ tầng" và làm sạch dữ liệu.
*   **Nhiệm vụ:**
    1. Viết script tải dữ liệu 18.2 triệu dòng từ Hugging Face.
    2. Nếu cần, cấu hình chuyển đổi định dạng lưu trữ (từ CSV sang Parquet) để tối ưu cho môi trường Hadoop/Spark.
    3. Tiền xử lý dữ liệu (Data Preprocessing): Loại bỏ các cột không cần thiết (chỉ giữ lại Hãng bay, Sân bay xuất phát/đến, Giờ bay dự kiến/thực tế, Số phút delay), xử lý các giá trị Null (ví dụ: Chuyến bay không delay thì cột Số phút delay = 0 thay vì Null).
*   **Điểm nhấn báo cáo:** Báo cáo về quá trình thay đổi dung lượng RAM và Tốc độ load file khi dùng định dạng tối ưu cho Big Data.

### Thành viên 2: Data Analyst (Chuyên viên Phân tích Dữ liệu) - TRỌNG TÂM
*   Chịu trách nhiệm viết code xử lý lõi Big Data.
*   **Nhiệm vụ:**
    1. Cấu hình PySpark trên Local (Máy tính cá nhân) hoặc Google Colab.
    2. Dùng Spark SQL hoặc Spark DataFrame API để thực thi các lệnh tính toán (Filter, GroupBy, Aggregate, Join) trên 18.2 triệu dòng nhằm trả lời 3 Insight ở Phần 2.
    3. Xuất kết quả đã tính toán và rút gọn (Aggregated Data - chỉ còn vài chục dòng) sang các file CSV nhỏ gửi cho Thành viên 3.
*   **Điểm nhấn báo cáo:** Chụp màn hình so sánh thời gian thực thi (Execution Time) của việc truy vấn 18 triệu dòng bằng PySpark (cực nhanh) so với công cụ khác. Giải thích cơ chế lười biếng (Lazy Evaluation) của Spark.

### Thành viên 3: Data Visualizer (Chuyên viên Trực quan hóa & Báo cáo)
*   Chịu trách nhiệm kể câu chuyện từ dữ liệu và định dạng đầu ra sản phẩm.
*   **Nhiệm vụ:**
    1. Nhận các file dữ liệu tổng hợp từ Thành viên 2.
    2. Dùng PowerBI, Tableau, hoặc Python (Matplotlib, Seaborn) thiết kế Dashboard trực quan.
    3. Vẽ biểu đồ hiển thị so sánh (Biểu đồ cột so sánh Hãng, Bản đồ nhiệt hiển thị khung giờ, Biểu đồ tròn chia tỷ lệ nguyên nhân).
    4. Tổng hợp Slide thuyết trình cho cả nhóm. Khớp nối các câu trả lời Insight để tạo thành kịch bản thuyết trình mạch lạc, thú vị.
*   **Điểm nhấn báo cáo:** Một Dashboard đẹp mắt thể hiện khả năng ứng dụng thực tế của kết quả phân tích.

---

## 4. Kế hoạch Milestones (4 Tuần)
*   **Tuần 1:** Chốt đề tài. Hoàn tất việc tải và tìm hiểu kiến trúc các cột thông tin (Schema) của bộ dữ liệu 18.2 triệu dòng. (Thành viên 1 lead).
*   **Tuần 2:** Setup môi trường PySpark; Tiến hành làm sạch dữ liệu và lập danh sách các câu lệnh truy vấn (Thành viên 1 & 2 phối hợp).
*   **Tuần 3:** Chạy Code PySpark để tính toán ra đáp án cho toàn bộ Insight; Xuất file gửi cho Team làm đồ họa. (Thành viên 2 lead).
*   **Tuần 4:** Hoàn chỉnh Dashboard, làm Slide Thuyết trình và báo cáo bản Word. Tập duyệt thuyết trình. (Thành viên 3 lead).
