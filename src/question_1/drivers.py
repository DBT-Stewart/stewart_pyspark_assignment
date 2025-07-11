from pyspark.sql import SparkSession
from src.question_1.utils import (
    create_dataframes,
    customers_with_only_iphone13,
    customers_upgraded_iphone13_to_14,
    customers_bought_all_products
)

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Question 1 Driver") \
        .master("local[*]") \
        .getOrCreate()

    # Create DataFrames
    purchase_df, product_df = create_dataframes(spark)

    print("=== Purchase Data ===")
    purchase_df.show(truncate=False)

    print("=== Product Data ===")
    product_df.show(truncate=False)

    # 1. Customers with only iPhone 13
    print("=== Customers with ONLY iPhone 13 ===")
    result1 = customers_with_only_iphone13(purchase_df)
    result1.show()

    # 2. Customers upgraded from iPhone 13 to iPhone 14
    print("=== Customers who upgraded from iPhone 13 to iPhone 14 ===")
    result2 = customers_upgraded_iphone13_to_14(purchase_df)
    result2.show()

    # 3. Customers who bought all products
    print("=== Customers who bought ALL products ===")
    result3 = customers_bought_all_products(purchase_df, product_df)
    result3.show()

    # Stop Spark session
    spark.stop()
