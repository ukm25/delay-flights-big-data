from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, sum, round, month
import time

def main():
    start_time = time.time()
    print("🚀 Đang khởi động lõi PySpark để thực hiện 3 Phân Tích Nâng Cao...")
    
    # Khởi tạo Spark Session, cấu hình sử dụng số lượng lõi CPU tối đa của máy Mac
    spark = SparkSession.builder \
        .appName("Hadoop_Advanced_Flight_Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memory", "10g") \
        .getOrCreate()

    # Tắt log cảnh báo rườm rà
    spark.sparkContext.setLogLevel("ERROR")

    # ĐỌC DỮ LIỆU
    print("📥 Đang nạp 3.4GB dữ liệu chuyến bay vào bộ nhớ...")
    file_path = "/Users/quangnm/MasterFSB/Big Data/FinalBigData/flight_delay_18M.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    print("\n=======================================================")
    print("🔥 PHÂN TÍCH 1: TỔNG THỜI GIAN TRỄ DO TỪNG NGUYÊN NHÂN")
    # Tính tổng số phút delay của từng nhóm nguyên nhân trên toàn bộ 18 triệu dòng
    delay_causes = df.select(
        sum("LATE_AIRCRAFT_DELAY").alias("Lỗi_Dây_Chuyền(Phút)"),
        sum("CARRIER_DELAY").alias("Lỗi_Hãng_Bay(Phút)"),
        sum("NAS_DELAY").alias("Lỗi_Không_Lưu(Phút)"),
        sum("WEATHER_DELAY").alias("Lỗi_Thời_Tiết(Phút)"),
        sum("SECURITY_DELAY").alias("Lỗi_An_Ninh(Phút)")
    )
    delay_causes.show()


    print("=======================================================")
    print("🔥 PHÂN TÍCH 2: BẢNG XẾP HẠNG TOP 10 SÂN BAY XUẤT PHÁT ÁC MỘNG NHẤT (Nhiều Delay Nhất)")
    # Nhóm theo Sân bay xuất phát, tính trung bình mức độ trễ cất cánh (Chỉ xét các sân bay lớn có > 50.000 chuyến bay)
    origin_delay = df.filter(col("DEP_DELAY").isNotNull()) \
        .groupBy("ORIGIN", "ORIGIN_CITY_NAME") \
        .agg(
            count("*").alias("Tổng_Số_Chuyến"),
            round(avg("DEP_DELAY"), 2).alias("Trung_Bình_Trễ_Cất_Cánh")
        ) \
        .filter(col("Tổng_Số_Chuyến") > 50000) \
        .orderBy(desc("Trung_Bình_Trễ_Cất_Cánh"))
    origin_delay.show(10)


    print("=======================================================")
    print("🔥 PHÂN TÍCH 3: THÁNG NÀO TRONG NĂM KHÁCH HÀNG BỊ HÀNH HẠ NHIỀU NHẤT")
    # Trích xuất tháng từ cột Ngày bay (FL_DATE), rồi tính Trung bình Delay
    month_delay = df.filter(col("ARR_DELAY").isNotNull()) \
        .withColumn("Tháng_Bay", month(col("FL_DATE"))) \
        .groupBy("Tháng_Bay") \
        .agg(
            count("*").alias("Tổng_Số_Chuyến"),
            round(avg("ARR_DELAY"), 2).alias("Trung_Bình_Trễ_Hạ_Cánh")
        ) \
        .orderBy(desc("Trung_Bình_Trễ_Hạ_Cánh"))
    month_delay.show(12)
    print("=======================================================\n")

    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"⏱️ TỔNG THỜI GIAN XỬ LÝ 3 BÀI TOÁN PHÂN TÍCH TRÊN 18 TRIỆU DÒNG (3.4GB): {execution_time:.2f} giây")
    print("🎉 Hoàn thành xuất sắc!")
    
    spark.stop()

if __name__ == "__main__":
    main()
