from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime, timedelta

def create_log_dataframe(spark):
    # Generate recent timestamps (within last 7 days)
    today = datetime.today()
    date_fmt = "%Y-%m-%d %H:%M:%S"
    data = [
        (1, 101, 'login', (today - timedelta(days=1)).strftime(date_fmt)),
        (2, 102, 'click', (today - timedelta(days=2)).strftime(date_fmt)),
        (3, 101, 'click', (today - timedelta(days=3)).strftime(date_fmt)),
        (4, 103, 'login', (today - timedelta(days=4)).strftime(date_fmt)),
        (5, 102, 'logout', (today - timedelta(days=5)).strftime(date_fmt)),
        (6, 101, 'click', (today - timedelta(days=6)).strftime(date_fmt)),
        (7, 103, 'click', (today - timedelta(days=7)).strftime(date_fmt)),
        (8, 102, 'click', (today - timedelta(days=8)).strftime(date_fmt))  # Outside 7 days
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
    for old_name, new_name in rename_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
