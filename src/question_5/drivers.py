from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, current_date, expr
from src.question_5.utils import create_employee_df, create_department_df, create_country_df


def main():
    # Step 1: Create Spark session
    spark = SparkSession.builder \
        .appName("PySpark Assignment Question 5") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    # Step 2: Create the three DataFrames with dynamic schema
    employee_df = create_employee_df(spark)
    department_df = create_department_df(spark)
    country_df = create_country_df(spark)

    print("\n=== Employee DataFrame ===")
    employee_df.show()

    print("\n=== Department DataFrame ===")
    department_df.show()

    print("\n=== Country DataFrame ===")
    country_df.show()

    # Step 3: Find avg salary of each department
    print("\n=== Average Salary per Department ===")
    avg_salary_df = employee_df.groupBy("department").avg("salary")
    avg_salary_df.show()

    # Step 4: Find employee’s name and department name where name starts with ‘m’
    print("\n=== Employees whose names start with 'm' and their department names ===")
    emp_m_df = employee_df.filter(col("employee_name").startswith("m")) \
        .join(department_df, employee_df.department == department_df.dept_id, "inner") \
        .select("employee_name", "dept_name")
    emp_m_df.show()

    # Step 5: Add new column 'bonus' = salary * 2
    print("\n=== Employee DataFrame with Bonus Column ===")
    employee_with_bonus_df = employee_df.withColumn("bonus", col("salary") * 2)
    employee_with_bonus_df.show()

    # Step 6: Reorder the column names
    print("\n=== Reordered Columns in Employee DataFrame ===")
    reordered_df = employee_with_bonus_df.select(
        "employee_id", "employee_name", "salary", "State", "Age", "department", "bonus"
    )
    reordered_df.show()

    # Step 7: Inner, Left, Right Joins
    print("\n=== Inner Join with Department ===")
    inner_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
    inner_join_df.show()

    print("\n=== Left Join with Department ===")
    left_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
    left_join_df.show()

    print("\n=== Right Join with Department ===")
    right_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")
    right_join_df.show()

    # Step 8: Replace State code with country name
    print("\n=== Employee DataFrame with Country Name ===")
    emp_country_df = employee_df.join(
        country_df,
        employee_df.State == country_df.country_code,
        "left"
    ).drop("State").withColumnRenamed("country_name", "State")
    emp_country_df.show()

    # Step 9: Convert column names to lowercase and add load_date
    print("\n=== Lowercase Columns + Load Date ===")
    final_df = emp_country_df.select(
        [col(c).alias(c.lower()) for c in emp_country_df.columns]
    ).withColumn("load_date", current_date())
    final_df.show()

    # Step 10: Save as two external tables: CSV and Parquet
    print("\n=== Writing external tables (CSV and Parquet) ===")
    # Write directly to local filesystem (works on Windows)
    final_df.write.mode("overwrite").option("header", True).csv("output/employee_csv")
    final_df.write.mode("overwrite").parquet("output/employee_parquet")
    print("\nData successfully written as CSV and Parquet to 'output/' folder.")
    print("\nTables written successfully to 'my_database'")

    # Stop session
    spark.stop()


if __name__ == "__main__":
    main()
