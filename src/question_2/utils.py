from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create credit card dataframe with sample values
def create_credit_card_df(spark):
    schema = StructType([
        StructField("card_number", StringType(), True)
    ])

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    return spark.createDataFrame(data, schema)

# Get number of partitions in the dataframe
def get_partition_count(df):
    return df.rdd.getNumPartitions()

# Repartition the dataframe to desired number of partitions
def repartition_df(df, num):
    return df.repartition(num)

# Coalesce the dataframe to fewer number of partitions
def coalesce_df(df, num):
    return df.coalesce(num)

# Masking logic: Show only last 4 digits
def mask_card_number(card_number):
    return "*" * 12 + card_number[-4:]

# Register UDF
mask_udf = udf(mask_card_number, StringType())

# Apply the masking UDF to the card_number column
def apply_mask(df):
    return df.withColumn("masked_card_number", mask_udf(col("card_number")))
