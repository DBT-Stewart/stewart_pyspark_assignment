from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, collect_set, array_contains, size

def create_dataframes(spark):
    # Purchase schema and data
    purchase_schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])

    purchase_data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    purchase_df = spark.createDataFrame(purchase_data, schema=purchase_schema)

    # Product schema and data
    product_schema = StructType([
        StructField("product_model", StringType(), True)
    ])

    product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    product_df = spark.createDataFrame(product_data, schema=product_schema)

    return purchase_df, product_df

def customers_with_only_iphone13(purchase_df):
    grouped = purchase_df.groupBy("customer").agg(
        collect_set("product_model").alias("products")
    )
    return grouped.filter((size("products") == 1) & array_contains("products", "iphone13")) \
                  .select("customer")

def customers_upgraded_iphone13_to_14(purchase_df):
    iphone13_df = purchase_df.filter(col("product_model") == "iphone13")
    iphone14_df = purchase_df.filter(col("product_model") == "iphone14")

    return iphone13_df.join(iphone14_df, on="customer", how="inner") \
                      .select("customer").distinct()

def customers_bought_all_products(purchase_df, product_df):
    all_products = [row['product_model'] for row in product_df.collect()]
    total_products = len(all_products)

    customer_products = purchase_df.groupBy("customer").agg(
        collect_set("product_model").alias("products")
    )

    return customer_products.filter(size(col("products")) == total_products) \
                            .select("customer")
