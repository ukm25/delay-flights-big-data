import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round, avg, sum, desc, month
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def step_1_data_ingestion(spark):
    print("\n[PHASE 1] Executing Data Ingestion from HDFS Storage...")
    hdfs_url = "hdfs://namenode:9000/input/flight_delay_18M.csv"
    local_url = "/Users/quangnm/MasterFSB/Big Data/FinalBigData/flight_delay_18M.csv"
    
    try:
        df = spark.read.csv(hdfs_url, header=True, inferSchema=True)
        print("[Task 1] SUCCESSFULLY loaded 3.4GB dataset from Hadoop HDFS!")
    except Exception as e:
        print("[Warning] Hadoop Cluster is offline, falling back to Local Storage...")
        df = spark.read.csv(local_url, header=True, inferSchema=True)
        print("[Task 1] Successfully loaded dataset from Local Storage!")
        
    print(f"[Stats] Total Data Ingestion Volume: {df.count():,} records")
    return df

def step_2_data_cleaning(df):
    print("\n[PHASE 2] Executing Data Cleaning and Preprocessing...")
    
    df_clean = df.filter(col("CANCELLED") == 0)
    
    df_clean = df_clean.dropna(subset=["ARR_DELAY", "DEP_DELAY", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST"])
    
    df_clean = df_clean.fillna(0, subset=["CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"])
    
    df_clean = df_clean.withColumn("ARR_DELAY", col("ARR_DELAY").cast("float")) \
                       .withColumn("DEP_DELAY", col("DEP_DELAY").cast("float")) \
                       .withColumn("DISTANCE", col("DISTANCE").cast("float"))
    
    print(f"[Task 2] Valid records after Cleaning: {df_clean.count():,} records")
    return df_clean

def step_3_data_analysis(df):
    print("\n[PHASE 3] Executing Data Analysis & Aggregation...")
    
    print("--- 3.1: Total Delay Minutes by Cause")
    delay_causes = df.select(
        sum("LATE_AIRCRAFT_DELAY").alias("Late_Aircraft_Error"),
        sum("CARRIER_DELAY").alias("Carrier_Error"),
        sum("NAS_DELAY").alias("NAS_Error"),
        sum("WEATHER_DELAY").alias("Weather_Error"),
        sum("SECURITY_DELAY").alias("Security_Error")
    )
    delay_causes.show()

    print("--- 3.2: Top 10 Origin Airports with Worst Departure Delays")
    origin_delay = df.filter(col("DEP_DELAY").isNotNull()) \
        .groupBy("ORIGIN", "ORIGIN_CITY_NAME") \
        .agg(
            count("*").alias("Total_Flights"),
            round(avg("DEP_DELAY"), 2).alias("Avg_Dep_Delay_Minutes")
        ) \
        .filter(col("Total_Flights") > 50000) \
        .orderBy(desc("Avg_Dep_Delay_Minutes"))
    origin_delay.show(10)

    print("--- 3.3: Arrival Delay Trends by Month")
    month_delay = df.filter(col("ARR_DELAY").isNotNull()) \
        .withColumn("Flight_Month", month(col("FL_DATE"))) \
        .groupBy("Flight_Month") \
        .agg(
            count("*").alias("Total_Flights"),
            round(avg("ARR_DELAY"), 2).alias("Avg_Arr_Delay_Minutes")
        ) \
        .orderBy(desc("Avg_Arr_Delay_Minutes"))
    month_delay.show(12)
    
    print("[Task 3] Completed Descriptive Analytics on 17.7 Million Records!")
    
    # Gói kết quả để ném ra Dashboard
    return {
        "delay_causes": delay_causes.collect()[0].asDict(),
        "top_origin_delays": [row.asDict() for row in origin_delay.collect()],
        "monthly_delays": [row.asDict() for row in month_delay.collect()]
    }

def step_4_machine_learning(df):
    print("\n[PHASE 4] Executing Machine Learning (Delay Prediction Training)...")
    
    print("--- 4.1: Preparing ML Dataset (Binary Labeling 0/1)")
    ml_df = df.filter(col("ARR_DELAY").isNotNull()) \
              .withColumn("IS_DELAYED", when(col("ARR_DELAY") > 15, 1.0).otherwise(0.0))
              
    ml_df = ml_df.sample(withReplacement=False, fraction=0.05, seed=42)

    ml_df = ml_df.dropna(subset=["OP_UNIQUE_CARRIER", "DISTANCE", "FL_DATE"])

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

    train_data, test_data = final_ml_df.randomSplit([0.7, 0.3], seed=42)
    print(f"--> Training Dataset Size: {train_data.count():,} | Testing Dataset Size: {test_data.count():,}")

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
    start_time = time.time()
    
    spark = SparkSession.builder \
        .appName("Flight_Delay_BigData_Pipeline") \
        .getOrCreate()
        
    df_raw = step_1_data_ingestion(spark)
    df_clean = step_2_data_cleaning(df_raw)
    
    # 3 & 4. Lấy kết quả ra Biến
    analysis_results = step_3_data_analysis(df_clean)
    ml_results = step_4_machine_learning(df_clean)
    
    # Ghi toàn bộ tinh hoa PySpark ra file JSON để Dashboard lấy vẽ Biểu đồ
    final_dashboard_data = {
        "analysis": analysis_results,
        "machine_learning": ml_results
    }
    with open("dashboard_data.json", "w", encoding="utf-8") as f:
        json.dump(final_dashboard_data, f, indent=4)
        
    print("\n[SUCCESS] Exported all PySpark results to dashboard_data.json cleanly!")

    spark.stop()
    print(f"\n[DONE] Total Pipeline Execution Time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
