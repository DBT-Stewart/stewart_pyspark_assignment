# src/question_4/util.py
from pyspark.sql.functions import col, explode, explode_outer, posexplode, current_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, StructType

def read_json_file(spark, path):
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("properties", StructType([
            StructField("name", StringType()),
            StructField("storeSize", StringType())
        ])),
        StructField("employees", ArrayType(StructType([
            StructField("empId", IntegerType()),
            StructField("empName", StringType())
        ])))
    ])
    return spark.read.schema(schema).json(path)

def flatten_df(df):
    return df.select(
        "id",
        col("properties.name").alias("properties_name"),
        col("properties.storeSize").alias("properties_storeSize"),
        "employees"
    )

def count_records(original_df, flattened_df):
    return original_df.count(), flattened_df.count()

def use_explode_variants(df):
    exploded = df.withColumn("exploded_employees", explode("employees"))
    exploded_outer = df.withColumn("exploded_employees", explode_outer("employees"))

    posexploded = df.selectExpr("*", "posexplode(employees) as (pos, employee)")

    return exploded, exploded_outer, posexploded

def filter_id(df):
    return df.filter(col("id") == 1001)

def convert_column_names_to_snake(df):
    for col_name in df.columns:
        snake_case = col_name[0].lower() + ''.join(['_' + c.lower() if c.isupper() else c for c in col_name[1:]])
        df = df.withColumnRenamed(col_name, snake_case)
    return df

def add_load_date_and_parts(df):
    df = df.withColumn("load_date", current_date())
    df = df.withColumn("year", year(col("load_date")))
    df = df.withColumn("month", month(col("load_date")))
    df = df.withColumn("day", dayofmonth(col("load_date")))
    return df

def write_partitioned(df):
    df.write.mode("overwrite")\
        .format("json")\
        .partitionBy("year", "month", "day")\
        .option("path", "./output/employee_details")\
        .saveAsTable("employee.employee_details")
