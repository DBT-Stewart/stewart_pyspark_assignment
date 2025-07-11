from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, current_date, datediff, col, count
from src.question_3.utils import create_log_dataframe, rename_columns

# Initialize Spark session
spark = SparkSession.builder \
    .config("spark.hadoop.io.native.lib.available",False) \
    .appName("UserActivityLogProcessing") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Create DataFrame with custom schema
df = create_log_dataframe(spark)

# 2. Rename columns dynamically
df_renamed = rename_columns(df)

# 3. Convert timestamp to login_date
df_with_date = df_renamed.withColumn("login_date", to_date("time_stamp", "yyyy-MM-dd HH:mm:ss"))

# 4. Filter last 7 days
last_7_days_df = df_with_date.filter(datediff(current_date(), col("login_date")) <= 7)

# 5. Count actions per user
user_action_count_df = last_7_days_df.groupBy("user_id").agg(count("user_activity").alias("action_count"))

# === Output Display ===
print("=== Original DataFrame ===")
df_renamed.show(truncate=False)

print("=== Data from Last 7 Days ===")
last_7_days_df.show(truncate=False)

print("=== Action Count per User (Last 7 Days) ===")
user_action_count_df.show(truncate=False)

# 6. Write CSV (Use absolute path on Windows to avoid Hadoop native errors)
csv_path = "file:///C:/Users/StewartPrincePM/PycharmProjects/stewart_pyspark_assignment/output/user_logs_csv"
last_7_days_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .csv("test_file.csv")

# 7. Save as managed table
spark.sql("CREATE DATABASE IF NOT EXISTS user")
last_7_days_df.write.mode("overwrite").saveAsTable("user.login_details")

# Stop Spark
spark.stop()
