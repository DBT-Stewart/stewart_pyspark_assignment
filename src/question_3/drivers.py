from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.question_3.utils import (
    create_log_dataframe,
    rename_columns,
    add_login_date,
    filter_last_7_days,
    count_actions_last_7_days
)

def main():
    spark = SparkSession.builder \
        .appName("UserActivityLogProcessing") \
        .enableHiveSupport() \
        .getOrCreate()

    # Step 1: Create DataFrame
    df = create_log_dataframe(spark)

    # Step 2: Rename columns
    df_renamed = rename_columns(df)

    # Step 3: Add login_date column
    df_with_date = add_login_date(df_renamed)

    # Step 4: Filter for last 7 days
    filtered_df = filter_last_7_days(df_with_date)

    # Step 5: Count actions per user
    action_count_df = count_actions_last_7_days(filtered_df)

    # Display
    print("=== Renamed DataFrame ===")
    df_renamed.show(truncate=False)

    print("=== Data from Last 7 Days ===")
    filtered_df.show(truncate=False)

    print("=== Action Count per User (Last 7 Days) ===")
    action_count_df.show(truncate=False)

    # Step 6: Write CSV with options
    filtered_df.write.mode("overwrite") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("quote", "\"") \
        .csv("file:///C:/Users/StewartPrincePM/PycharmProjects/stewart_pyspark_assignment/output/user_logs_csv")

    # Step 7: Save as managed table
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    filtered_df.write.mode("overwrite").saveAsTable("user.login_details")

    spark.stop()

if __name__ == "__main__":
    main()
