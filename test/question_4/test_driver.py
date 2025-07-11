import pytest
from pyspark.sql import SparkSession
from src.question_4.utils import *

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test JSON Flattening") \
        .master("local[*]") \
        .getOrCreate()

def test_json_workflow(spark):
    json_path = "src/question_4/data/input.json"
    df = read_json_file(spark, json_path)
    assert df.count() == 1

    flat_df = flatten_df(df)
    assert flat_df.count() == 1

    exploded, exploded_outer, posexploded = use_explode_variants(flat_df)
    assert exploded.count() == 3
    assert exploded_outer.count() == 3
    assert posexploded.count() == 3

    filtered_df = filter_id(flat_df, 1001)
    assert filtered_df.count() == 1

    snake_df = convert_column_names_to_snake(flat_df)
    assert "properties_name" in snake_df.columns
    assert "properties_store_size" in snake_df.columns

    enriched_df = add_load_date_and_parts(snake_df)
    assert "load_date" in enriched_df.columns
    assert "year" in enriched_df.columns
    assert "month" in enriched_df.columns
    assert "day" in enriched_df.columns
