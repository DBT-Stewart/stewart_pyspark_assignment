from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_date, current_date, datediff, col
from datetime import datetime

def create_log_dataframe(spark):
    # Static sample data
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00'),
    ]

    schema = StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    return spark.createDataFrame(data, schema=schema)

def rename_columns(df):
    rename_map = {
        "log id": "log_id",
        "user$id": "user_id",
        "action": "user_activity",
        "timestamp": "time_stamp"
    }
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df

def add_login_date(df):
    return df.withColumn("login_date", to_date("time_stamp", "yyyy-MM-dd HH:mm:ss"))

def filter_last_7_days(df):
    return df.filter(datediff(current_date(), col("login_date")) <= 7)

def count_actions_last_7_days(df):
    return df.groupBy("user_id").agg(col("user_id"), col("user_activity").count().alias("action_count"))
