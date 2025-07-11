# import pytest
# from pyspark.sql.functions import col
# from src.question_5 import utils, drivers
#
# @pytest.fixture(scope="module")
# def setup_dataframes(spark):
#     employee_df = utils.create_employee_df(spark)
#     department_df = utils.create_department_df(spark)
#     country_df = utils.create_country_df(spark)
#     return employee_df, department_df, country_df
#
# def test_avg_salary(setup_dataframes):
#     employee_df, _, _ = setup_dataframes
#     result = drivers.avg_salary_by_department(employee_df)
#     assert "avg_salary" in result.columns
#
# def test_bonus_column(setup_dataframes):
#     employee_df, _, _ = setup_dataframes
#     df = drivers.add_bonus_column(employee_df)
#     assert "bonus" in df.columns
#
# def test_column_reorder(setup_dataframes):
#     employee_df, _, _ = setup_dataframes
#     df = drivers.reorder_employee_columns(employee_df)
#     assert df.columns == ["employee_id", "employee_name", "salary", "State", "Age", "department"]
#
# def test_employees_starting_with_m(setup_dataframes):
#     employee_df, department_df, _ = setup_dataframes
#     df = drivers.employees_starting_with_m(employee_df, department_df)
#     assert "employee_name" in df.columns
#     assert df.filter(col("employee_name").startswith("m")).count() > 0
#
# def test_country_replace(setup_dataframes):
#     employee_df, _, country_df = setup_dataframes
#     df = drivers.replace_state_with_country(employee_df, country_df)
#     assert "State" in df.columns
#     assert df.filter(col("State") == "newyork").count() > 0
#
# def test_lowercase_and_date(setup_dataframes):
#     employee_df, _, country_df = setup_dataframes
#     df = drivers.replace_state_with_country(employee_df, country_df)
#     df = drivers.lowercase_columns_with_date(df)
#     assert "load_date" in df.columns
#     assert "employee_id" in df.columns  # now lowercase
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.question_5.utils import create_employee_df, create_department_df, create_country_df


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("PySpark Unit Test") \
        .master("local[*]") \
        .getOrCreate()


def test_employee_df_schema_and_count(spark):
    df = create_employee_df(spark)
    assert df.count() == 7
    expected_columns = ["employee_id", "employee_name", "department", "State", "salary", "Age"]
    assert df.columns == expected_columns


def test_department_df_values(spark):
    df = create_department_df(spark)
    assert df.count() == 5
    assert df.filter(col("dept_name") == "hr").count() == 1


def test_country_df_mapping(spark):
    df = create_country_df(spark)
    assert df.count() == 3
    ca_row = df.filter(col("country_code") == "ca").collect()[0]
    assert ca_row["country_name"] == "California"
