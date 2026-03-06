from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, sum, round
import time

def main():
    start_time = time.time()
    print("🚀 Đang khởi động lõi PySpark...")
    
    # Khởi tạo Spark Session. Sử dụng local[*] để tận dụng tối đa tất cả các nhân CPU trên máy Mac M
    spark = SparkSession.builder \
        .appName("Hadoop_Flight_Delay_Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    # Tắt log cảnh báo rườm rà của Spark
    spark.sparkContext.setLogLevel("ERROR")

    # 1. ĐỌC DỮ LIỆU
    print("📥 Đang nạp 3.4GB dữ liệu chuyến bay vào bộ nhớ PySpark...")
    file_path = "hdfs://namenode:9000/input/flight_delay_18M.csv"
    
    # Đọc CSV. Sử dụng inferSchema=True để Spark tự đoán kiểu dữ liệu
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    total_rows = df.count()
    print(f"✅ Đã tải thành công {total_rows:,} dòng dữ liệu!")

    # 2. PHÂN TÍCH DỮ LIỆU: Bóc tách song song các Hãng hàng không trễ nhất
    print("\n⏳ Đang phân tán tính toán: Phân tích độ trễ của các hãng hàng không...")
    
    # Filter các chuyến không bị hủy và có ARR_DELAY (Phút trễ khi đến)
    delay_analysis = df.filter(col("ARR_DELAY").isNotNull()) \
        .groupBy("OP_UNIQUE_CARRIER") \
        .agg(
            count("*").alias("Thống_Kê_Số_Chuyến"),
            round(avg("ARR_DELAY"), 2).alias("Trung_Bình_Phút_Trễ")
        ) \
        .orderBy(desc("Trung_Bình_Phút_Trễ"))
        
    print("\n🏆 KẾT QUẢ TOP 15 HÃNG HÀNG KHÔNG DELAY NHIỀU NHẤT:")
    delay_analysis.show(15)

    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"\n⏱️ TỔNG THỜI GIAN XỬ LÝ 18 TRIỆU DÒNG (3.4GB): {execution_time:.2f} giây")
    print("\n🎉 Hoàn thành xử lý Big Data bằng PySpark!")
    spark.stop()

if __name__ == "__main__":
    main()
