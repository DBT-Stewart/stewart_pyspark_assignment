import pytest
from pyspark.sql import SparkSession
from src.question_3.utils import (
    create_log_dataframe,
    rename_columns,
    add_login_date,
    filter_last_7_days,
    count_actions_last_7_days
)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test-q3") \
        .enableHiveSupport() \
        .getOrCreate()

@pytest.fixture(scope="module")
def log_df(spark):
    df = create_log_dataframe(spark)
    renamed = rename_columns(df)
    with_date = add_login_date(renamed)
    return with_date

def test_column_renaming(log_df):
    expected_cols = {"log_id", "user_id", "user_activity", "time_stamp", "login_date"}
    assert set(log_df.columns) == expected_cols

def test_login_date_type(log_df):
    assert log_df.schema["login_date"].dataType.simpleString() == "date"

def test_filter_last_7_days(log_df):
    filtered_df = filter_last_7_days(log_df)
    today = log_df.sql_ctx.sparkSession.sql("SELECT current_date()").collect()[0][0]
    for row in filtered_df.select("login_date").collect():
        assert (today - row.login_date).days <= 7

def test_action_count(log_df):
    filtered = filter_last_7_days(log_df)
    result_df = count_actions_last_7_days(filtered)
    result_dict = {row["user_id"]: row["action_count"] for row in result_df.collect()}
    for count in result_dict.values():
        assert isinstance(count, int)
