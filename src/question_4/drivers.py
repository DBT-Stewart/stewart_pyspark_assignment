from pyspark.sql import SparkSession
from src.question_4.utils import (
    read_json_file, flatten_df, count_records,
    use_explode_variants, filter_id,
    convert_column_names_to_snake, add_load_date_and_parts, write_partitioned
)

def main():
    spark = SparkSession.builder \
        .appName("JSON Flatteninga") \
        .enableHiveSupport() \
        .getOrCreate()

    json_path = "nested_json_file.json"

    df = read_json_file(spark, json_path)
    df.printSchema()
    df.show(truncate=False)

    flat_df = flatten_df(df)
    flat_df.show(truncate=False)

    original_count, flattened_count = count_records(df, flat_df)
    print(f"Original count: {original_count}, Flattened count: {flattened_count}")

    exploded, exploded_outer, posexploded = use_explode_variants(flat_df)
    exploded.show(truncate=False)
    exploded_outer.show(truncate=False)
    posexploded.show(truncate=False)

    filtered_df = filter_id(flat_df, 1001)
    filtered_df.show()

    snake_df = convert_column_names_to_snake(flat_df)
    snake_df.show()

    enriched_df = add_load_date_and_parts(snake_df)
    enriched_df.show()

    spark.sql("CREATE DATABASE IF NOT EXISTS employee")
    write_partitioned(enriched_df)

    spark.stop()

if __name__ == "__main__":
    main()
