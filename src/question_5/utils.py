from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_employee_df(spark):
    schema = StructType([
        StructField("employee_id", IntegerType()),
        StructField("employee_name", StringType()),
        StructField("department", StringType()),
        StructField("State", StringType()),
        StructField("salary", IntegerType()),
        StructField("Age", IntegerType())
    ])
    data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]
    return spark.createDataFrame(data, schema)

def create_department_df(spark):
    schema = StructType([
        StructField("dept_id", StringType()),
        StructField("dept_name", StringType())
    ])
    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]
    return spark.createDataFrame(data, schema)

def create_country_df(spark):
    schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())
    ])
    data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]
    return spark.createDataFrame(data, schema)
