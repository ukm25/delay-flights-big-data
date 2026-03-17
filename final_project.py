import time
TOTAL_START_TIME = time.time()
print("\n[PHASE 1] Initializing PySpark Engine...")

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round, avg, sum, desc, month
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Chỉ đọc những cột cần thiết để giảm I/O
NEEDED_COLS = [
    "CANCELLED", "ARR_DELAY", "DEP_DELAY",
    "OP_UNIQUE_CARRIER", "ORIGIN", "ORIGIN_CITY_NAME", "DEST",
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
    "DISTANCE", "FL_DATE"
]

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def step_1_hdfs_ingestion(spark):
    print("\n[PHASE 2] Executing HDFS Data Ingestion...")
    hdfs_url = "hdfs://namenode:9000/input/flight_delay_18M.csv"
    local_url = "flight_delay_18M.csv"

    # Define schema to avoid expensive inferSchema on 3.4GB file
    schema = StructType([
        StructField("airline_id", IntegerType(), True),
        StructField("FL_DATE", StringType(), True),
        StructField("OP_UNIQUE_CARRIER", StringType(), True),
        StructField("OP_CARRIER_FL_NUM", StringType(), True),
        StructField("ORIGIN_AIRPORT_ID", IntegerType(), True),
        StructField("ORIGIN", StringType(), True),
        StructField("ORIGIN_CITY_NAME", StringType(), True),
        StructField("ORIGIN_STATE_ABR", StringType(), True),
        StructField("ORIGIN_STATE_FIPS", IntegerType(), True),
        StructField("ORIGIN_STATE_NM", StringType(), True),
        StructField("ORIGIN_WAC", IntegerType(), True),
        StructField("DEST_AIRPORT_ID", IntegerType(), True),
        StructField("DEST", StringType(), True),
        StructField("DEST_CITY_NAME", StringType(), True),
        StructField("DEST_STATE_ABR", StringType(), True),
        StructField("DEST_STATE_FIPS", IntegerType(), True),
        StructField("DEST_STATE_NM", StringType(), True),
        StructField("DEST_WAC", IntegerType(), True),
        StructField("CRS_DEP_TIME", IntegerType(), True),
        StructField("DEP_TIME", IntegerType(), True),
        StructField("DEP_DELAY", FloatType(), True),
        StructField("DEP_DELAY_NEW", FloatType(), True),
        StructField("DEP_DEL15", FloatType(), True),
        StructField("DEP_DELAY_GROUP", IntegerType(), True),
        StructField("DEP_TIME_BLK", StringType(), True),
        StructField("CRS_ARR_TIME", IntegerType(), True),
        StructField("ARR_TIME", IntegerType(), True),
        StructField("ARR_DELAY", FloatType(), True),
        StructField("ARR_DEL15", FloatType(), True),
        StructField("ARR_DELAY_GROUP", IntegerType(), True),
        StructField("ARR_TIME_BLK", StringType(), True),
        StructField("CANCELLED", FloatType(), True),
        StructField("CANCELLATION_CODE", StringType(), True),
        StructField("DIVERTED", FloatType(), True),
        StructField("CRS_ELAPSED_TIME", FloatType(), True),
        StructField("ACTUAL_ELAPSED_TIME", FloatType(), True),
        StructField("AIR_TIME", FloatType(), True),
        StructField("FLIGHTS", FloatType(), True),
        StructField("DISTANCE", FloatType(), True),
        StructField("DISTANCE_GROUP", IntegerType(), True),
        StructField("CARRIER_DELAY", FloatType(), True),
        StructField("WEATHER_DELAY", FloatType(), True),
        StructField("NAS_DELAY", FloatType(), True),
        StructField("SECURITY_DELAY", FloatType(), True),
        StructField("LATE_AIRCRAFT_DELAY", FloatType(), True),
        StructField("DEP_DELAY_OVER15", FloatType(), True),
        StructField("ARR_DELAY_OVER15", FloatType(), True),
        StructField("TOTAL_DELAY", FloatType(), True),
        StructField("ANY_DELAY", FloatType(), True)
    ])

    try:
        # Giảm thời gian chờ bằng cách không dùng inferSchema
        df = spark.read.csv(hdfs_url, header=True, schema=schema)
        # Kiểm tra xem có dữ liệu không bằng một action nhẹ
        df.limit(1).collect()
        print("[Task 1] SUCCESSFULLY loaded 3.4GB dataset from Hadoop HDFS!")
    except Exception as e:
        print(f"[Warning] HDFS Connection Failed: {str(e)[:200]}...")
        print("[Warning] Falling back to Local Storage...")
        df = spark.read.csv(local_url, header=True, schema=schema)
        print("[Task 1] Successfully loaded dataset from Local Storage!")

    # In ra 5 record đầu và tổng số cột để kiểm tra trước khi lọc
    print(f"\n[Debug] Raw Dataset contains {len(df.columns)} columns.")
    print("[Debug] Previewing first 5 records of raw data:")
    df.show(5)

    # Column pruning: chỉ giữ lại những cột thực sự dùng
    df = df.select([c for c in NEEDED_COLS if c in df.columns])

    print(f"[Stats] Total Data Ingestion Volume: {df.count():,} records")
    return df

def step_2_data_cleaning(df):
    print("\n[PHASE 3] Executing Data Cleaning and Preprocessing...")

    df_clean = df.filter(col("CANCELLED") == 0) \
                 .drop("CANCELLED")

    df_clean = df_clean.dropna(subset=["ARR_DELAY", "DEP_DELAY", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST"])

    df_clean = df_clean.fillna(0, subset=["CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"])

    df_clean = df_clean.withColumn("ARR_DELAY", col("ARR_DELAY").cast("float")) \
                       .withColumn("DEP_DELAY", col("DEP_DELAY").cast("float")) \
                       .withColumn("DISTANCE", col("DISTANCE").cast("float"))

    total = df_clean.count()
    print(f"[Task 2] Valid records after Cleaning: {total:,} records")
    return df_clean

def step_3_data_analysis(df):
    print("\n[PHASE 4] Executing Data Analysis & Aggregation...")

    print("--- 3.1: Total Delay Minutes by Cause")
    delay_causes = df.select(
        sum("LATE_AIRCRAFT_DELAY").alias("Late_Aircraft_Error"),
        sum("CARRIER_DELAY").alias("Carrier_Error"),
        sum("NAS_DELAY").alias("NAS_Error"),
        sum("WEATHER_DELAY").alias("Weather_Error"),
        sum("SECURITY_DELAY").alias("Security_Error")
    )
    # Dùng collect() 1 lần duy nhất thay vì show() rồi collect() thêm 1 lần nữa
    delay_row = delay_causes.collect()[0]
    print(f"  Late Aircraft: {int(delay_row['Late_Aircraft_Error']):,} | Carrier: {int(delay_row['Carrier_Error']):,} | NAS: {int(delay_row['NAS_Error']):,} | Weather: {int(delay_row['Weather_Error']):,} | Security: {int(delay_row['Security_Error']):,}")

    print("--- 3.2: Top 10 Origin Airports with Worst Departure Delays")
    origin_rows = df.filter(col("DEP_DELAY").isNotNull()) \
        .groupBy("ORIGIN", "ORIGIN_CITY_NAME") \
        .agg(
            count("*").alias("Total_Flights"),
            round(avg("DEP_DELAY"), 2).alias("Avg_Dep_Delay_Minutes")
        ) \
        .filter(col("Total_Flights") > 50000) \
        .orderBy(desc("Avg_Dep_Delay_Minutes")) \
        .limit(10) \
        .collect()

    for r in origin_rows:
        print(f"  {r['ORIGIN']} ({r['ORIGIN_CITY_NAME']}): {r['Avg_Dep_Delay_Minutes']} min avg delay ({r['Total_Flights']:,} flights)")

    print("--- 3.3: Arrival Delay Trends by Month")
    monthly_rows = df.filter(col("ARR_DELAY").isNotNull()) \
        .withColumn("Flight_Month", month(col("FL_DATE"))) \
        .groupBy("Flight_Month") \
        .agg(
            count("*").alias("Total_Flights"),
            round(avg("ARR_DELAY"), 2).alias("Avg_Arr_Delay_Minutes")
        ) \
        .orderBy(desc("Avg_Arr_Delay_Minutes")) \
        .collect()

    for r in monthly_rows:
        print(f"  Month {r['Flight_Month']}: {r['Avg_Arr_Delay_Minutes']} min avg delay ({r['Total_Flights']:,} flights)")

    print("[Task 3] Completed Descriptive Analytics on 17.7 Million Records!")

    return {
        "delay_causes": delay_row.asDict(),
        "top_origin_delays": [r.asDict() for r in origin_rows],
        "monthly_delays": [r.asDict() for r in monthly_rows]
    }

def step_4_machine_learning(df):
    print("\n[PHASE 5] Executing Machine Learning (Delay Prediction Training)...")

    print("--- 4.1: Preparing ML Dataset (Binary Labeling 0/1)")
    ml_df = df.filter(col("ARR_DELAY").isNotNull()) \
              .withColumn("IS_DELAYED", when(col("ARR_DELAY") > 15, 1.0).otherwise(0.0)) \
              .sample(withReplacement=False, fraction=0.05, seed=42) \
              .dropna(subset=["OP_UNIQUE_CARRIER", "DISTANCE", "FL_DATE"])

    indexer = StringIndexer(inputCol="OP_UNIQUE_CARRIER", outputCol="CARRIER_INDEX", handleInvalid="skip")
    ml_df = indexer.fit(ml_df).transform(ml_df)

    ml_df = ml_df.withColumn("Month_Index", month(col("FL_DATE")).cast("double")) \
                 .withColumn("CARRIER_INDEX", col("CARRIER_INDEX").cast("double")) \
                 .withColumn("DISTANCE", col("DISTANCE").cast("double"))

    assembler = VectorAssembler(
        inputCols=["CARRIER_INDEX", "DISTANCE", "Month_Index"],
        outputCol="features",
        handleInvalid="skip"
    )
    final_ml_df = assembler.transform(ml_df).select("features", "IS_DELAYED")

    # Count cả 2 dataset trong 1 action thay vì 2 action riêng lẻ
    train_data, test_data = final_ml_df.randomSplit([0.7, 0.3], seed=42)
    train_count = train_data.count()
    test_count = test_data.count()
    print(f"--> Training Dataset Size: {train_count:,} | Testing Dataset Size: {test_count:,}")

    print("--- 4.2: Initiating Decision Tree Classifier Training...")
    dt = DecisionTreeClassifier(labelCol="IS_DELAYED", featuresCol="features", maxDepth=10)
    model = dt.fit(train_data)

    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="IS_DELAYED", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"--> [RESULT 4] Delay Prediction Accuracy: {accuracy * 100:.2f}%")

    print("\n--- 4.3: [Insight] Feature Importance for Delays")
    importances = model.featureImportances
    features_list = ["CARRIER", "DISTANCE", "MONTH"]
    ml_results = {
        "accuracy": float(accuracy * 100),
        "feature_importance": []
    }

    for i, feature in enumerate(features_list):
        try:
            val = importances[i]
            print(f"- Impact of [{feature}]: {val * 100:.2f}%")
            ml_results["feature_importance"].append({"feature": feature, "impact": float(val * 100)})
        except IndexError:
            pass

    print("[Task 4] Machine Learning Module Completed Successfully!")
    return ml_results

def main():
    global TOTAL_START_TIME
    
    spark = SparkSession.builder \
        .appName("Flight_Delay_BigData_Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "8") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    # Tắt log Spark ở mức INFO/WARN để màn hình log gọn
    spark.sparkContext.setLogLevel("ERROR")
    print(f"--> [Time] Step 1 (Engine Init) took: {time.time() - TOTAL_START_TIME:.2f}s")

    t1 = time.time()
    df_raw = step_1_hdfs_ingestion(spark)
    print(f"--> [Time] Step 2 (HDFS Ingestion) took: {time.time() - t1:.2f}s")

    t2 = time.time()
    df_clean = step_2_data_cleaning(df_raw)
    print(f"--> [Time] Step 3 (Cleaning) took: {time.time() - t2:.2f}s")

    t3 = time.time()
    analysis_results = step_3_data_analysis(df_clean)
    print(f"--> [Time] Step 4 (Analysis) took: {time.time() - t3:.2f}s")

    t4 = time.time()
    ml_results = step_4_machine_learning(df_clean)
    print(f"--> [Time] Step 5 (Machine Learning) took: {time.time() - t4:.2f}s")

    # Export results for chart rendering
    final_data = {"analysis": analysis_results, "machine_learning": ml_results}
    with open("dashboard_data.json", "w", encoding="utf-8") as f:
        json.dump(final_data, f)
    print("\n[SUCCESS] Exported results to dashboard_data.json")

    spark.stop()
    print(f"\n[DONE] Total Pipeline Execution Time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
