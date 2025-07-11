import pytest
from pyspark.sql import SparkSession
from src.question_1.utils import create_dataframes
from src.question_1 import drivers

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

@pytest.fixture(scope="module")
def dataframes(spark):
    return create_dataframes(spark)

def test_customers_with_only_iphone13(dataframes):
    purchase_df, _ = dataframes
    result_df = drivers.customers_with_only_iphone13(purchase_df)
    result = [row.customer for row in result_df.collect()]
    assert result == [4]

def test_customers_upgraded(dataframes):
    purchase_df, _ = dataframes
    result_df = drivers.customers_upgraded_iphone13_to_14(purchase_df)
    result = sorted([row.customer for row in result_df.collect()])
    assert result == [1, 3]

def test_customers_bought_all(dataframes):
    purchase_df, product_df = dataframes
    result_df = drivers.customers_bought_all_products(purchase_df, product_df)
    result = [row.customer for row in result_df.collect()]
    assert result == [1]
