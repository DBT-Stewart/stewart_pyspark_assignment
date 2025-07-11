import pytest
from pyspark.sql import SparkSession
from src.question_2.utils import create_credit_card_df
from src.question_2 import drivers

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-test-q2").getOrCreate()

@pytest.fixture(scope="module")
def df(spark):
    return create_credit_card_df(spark)

def test_partition_counts(df):
    original_count = drivers.get_partition_count(df)
    df_repartitioned = drivers.repartition_df(df, 5)
    repartitioned_count = drivers.get_partition_count(df_repartitioned)
    assert repartitioned_count == 5

    df_coalesced = drivers.coalesce_df(df_repartitioned, original_count)
    coalesced_count = drivers.get_partition_count(df_coalesced)
    assert coalesced_count == original_count

def test_card_masking(df):
    masked_df = drivers.apply_mask(df)
    masked = masked_df.select("masked_card_number").rdd.flatMap(lambda x: x).collect()

    for m in masked:
        assert m.startswith("************") and len(m) == 16
