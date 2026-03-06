import matplotlib.pyplot as plt
import numpy as np

def create_charts():
    print("🎨 Đang vẽ biểu đồ từ kết quả Big Data...")

    # Cài đặt font chữ hỗ trợ tiếng Việt cơ bản (tuỳ hệ thống có thể cần điều chỉnh font, ở đây dùng mặc định)
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'sans-serif']

    # =========================================================================
    # BÀI TOÁN 1: Biểu đồ Tròn (Pie Chart) - Nguyên nhân gây Delay
    # Dữ liệu từ PySpark: LATE_AIRCRAFT(81.1M), CARRIER(69.5M), NAS(46.3M), WEATHER(11M), SECURITY(0.3M)
    causes = ['Lỗi Dây Chuyền\n(Tàu bay về trễ)', 'Trách nhiệm\nHãng hàng không', 'Hệ thống\nQuản lý không lưu', 'Nguyên nhân\nThời Tiết', 'Kiểm tra\nAn ninh']
    minutes_millions = [81.1, 69.5, 46.3, 11.0, 0.3]
    colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#c2c2f0']

    plt.figure(figsize=(10, 7))
    plt.pie(minutes_millions, labels=causes, colors=colors, autopct='%1.1f%%', startangle=140)
    plt.title('Tỷ Lệ Các Nguyên Nhân Gây Delay Chuyến Bay (Tổng Hợp 15 Năm)', fontsize=15, fontweight='bold', pad=20)
    plt.axis('equal')
    plt.tight_layout()
    plt.savefig('Chart_1_Delay_Causes.png', dpi=300)
    plt.close()
    print("✅ Đã lưu Chart_1_Delay_Causes.png")

    # =========================================================================
    # BÀI TOÁN 2: Biểu đồ Cột Ngang - Top 10 Sân Bay Ác Mộng Nhất
    # Dữ liệu từ PySpark: MDW, OAK, DAL, HOU, EWR, BWI, LAS, STL, DEN, MCO
    airports = ['Chicago (MDW)', 'Ontario (OAK)', 'Dallas (DAL)', 'Houston (HOU)', 'Newark (EWR)', 
                'Baltimore (BWI)', 'Las Vegas (LAS)', 'St. Louis (STL)', 'Denver (DEN)', 'Orlando (MCO)']
    avg_delay_origin = [15.26, 12.18, 12.00, 11.21, 11.12, 10.95, 10.63, 10.58, 10.42, 10.37]

    plt.figure(figsize=(12, 6))
    y_pos = np.arange(len(airports))
    
    # Vẽ biểu đồ cột ngang
    bars = plt.barh(y_pos, avg_delay_origin, color='#ff6666', edgecolor='black')
    
    plt.yticks(y_pos, airports, fontsize=11)
    plt.gca().invert_yaxis()  # Đảo ngược trục Y để sân bay delay nhiều nhất lên đầu
    plt.xlabel('Số phút trễ trung bình mỗi chuyến', fontsize=12, fontweight='bold')
    plt.title('Top 10 Sân Bay Xuất Phát "Ác Mộng" Nhất Nước Mỹ (>50,000 chuyến)', fontsize=15, fontweight='bold', pad=20)
    
    # Thêm giá trị trên từng cột
    for bar in bars:
        plt.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2, 
                 f'{bar.get_width():.2f}', 
                 ha='left', va='center', fontsize=11)

    plt.tight_layout()
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.savefig('Chart_2_Worst_Origin_Airports.png', dpi=300)
    plt.close()
    print("✅ Đã lưu Chart_2_Worst_Origin_Airports.png")

    # =========================================================================
    # BÀI TOÁN 3: Biểu đồ Đường (Line Chart) - Xu hướng Delay theo Tháng
    # Dữ liệu từ PySpark sắp xếp theo thứ tự Tháng từ 1 đến 12 
    months = ['Thg 1', 'Thg 2', 'Thg 3', 'Thg 4', 'Thg 5', 'Thg 6', 
              'Thg 7', 'Thg 8', 'Thg 9', 'Thg 10', 'Thg 11', 'Thg 12']
    # Dữ liệu tương ứng với tháng 1 -> 12 từ bảng kết quả
    avg_delays_month = [7.28, 3.44, 6.97, 6.66, 9.40, 13.87, 17.30, 9.50, 3.16, -0.06, -0.79, 3.86]

    plt.figure(figsize=(12, 6))
    plt.plot(months, avg_delays_month, marker='o', markersize=10, linestyle='-', linewidth=3, color='#3399ff')
    
    # Tô màu đỏ vùng có Delay cao (Mùa hè Tháng 6,7,8)
    plt.axvspan(4.5, 7.5, color='red', alpha=0.1, label='Đỉnh điểm nghỉ Hè')
    
    plt.axhline(y=0, color='black', linestyle='-', linewidth=1) # Đường số 0
    plt.fill_between(months, avg_delays_month, 0, where=(np.array(avg_delays_month) > 0), alpha=0.3, color='#3399ff')
    plt.fill_between(months, avg_delays_month, 0, where=(np.array(avg_delays_month) <= 0), alpha=0.3, color='green')

    plt.title('Biến Động Trễ Chuyến Bay Dựa Theo Các Tháng Trong Năm', fontsize=15, fontweight='bold', pad=20)
    plt.ylabel('Trung Bình Số Phút Trễ / Chuyến', fontsize=12, fontweight='bold')
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()
    
    # Ghi chú giá trị trên từng điểm
    for i, txt in enumerate(avg_delays_month):
        plt.text(i, avg_delays_month[i] + 0.5, f'{txt}', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig('Chart_3_Monthly_Trends.png', dpi=300)
    plt.close()
    print("✅ Đã lưu Chart_3_Monthly_Trends.png")

    print("\n🚀 QUÁ TRÌNH KHAI THÁC LÕI DỮ LIỆU VÀ TRỰC QUAN HÓA HOÀN TẤT TUYỆT ĐỐI!")
    print("👉 Hãy mở thư mục FinalBigData trên máy Mac để lấy 3 bức ảnh kết quả chèn vào Slide nhé!")

if __name__ == "__main__":
    create_charts()
