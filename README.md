# PySpark Assignments Repository

This repository contains solutions for **PySpark Assignments 1 to 5**, focusing on real-world big data processing tasks using Apache Spark (PySpark). Each assignment is designed to practice essential PySpark concepts including DataFrame creation, transformations, joins, aggregations, partitioning, schema handling, and writing data in various formats.

---

## Technologies Used

- Apache Spark (PySpark)
- Python 3.x
- Spark SQL
- Hive Tables (Managed & External)
- Parquet & CSV formats
- Custom Schemas (StructType, StructField)

---

## Assignment Summaries

### **Assignment 1: Customer and Product Analysis**
- Created DataFrames: `purchase_data_df` and `product_data_df` using custom schemas.
- Performed queries to:
  - Identify customers who bought **only iPhone 13**.
  - Find customers who **upgraded from iPhone 13 to iPhone 14**.
  - List customers who have bought **all models** listed in the product data.

---

### **Assignment 2: Credit Card Data Masking & Partitioning**
- Created DataFrame `credit_card_df` using multiple read techniques.
- Performed partition operations:
  - Displayed current number of partitions.
  - Increased partitions to 5.
  - Reverted to the original number of partitions.
- Implemented a UDF to **mask credit card numbers** (showing only last 4 digits).
- Produced final DataFrame with: `card_number`, `masked_card_number`.

---

### **Assignment 3: User Activity Analysis**
- Created DataFrame using **StructType** and **StructField**.
- Dynamically renamed columns to:
  - `log_id`, `user_id`, `user_activity`, `time_stamp`.
- Calculated number of actions per user **within the last 7 days**.
- Converted timestamp to `login_date` (`yyyy-MM-dd` format, `date` type).
- Wrote final DataFrame to:
  - CSV file with multiple write options.
  - Managed Hive table: `user.login_details`.

---

### **Assignment 4: Nested JSON Processing & Flattening**
- Read nested JSON using a dynamic function.
- Flattened nested structure and validated:
  - **Record count before and after flattening**.
- Applied:
  - `explode`, `explode_outer`, and `posexplode` for nested array processing.
- Filtered records where `id = 1001`.
- Converted column names from **camelCase to snake_case**.
- Added:
  - `load_date`
  - Partition columns: `year`, `month`, `day`

---

### **Assignment 5: Employee Data Analysis & Joins**
- Created DataFrames:
  - `employee_df`
  - `department_df`
  - `country_df`
- Performed transformations:
  - Calculated **average salary** by department.
  - Filtered employees whose names **start with 'm'**.
  - Added `bonus` column (calculated as `salary * 2`).
  - Reordered columns dynamically.
- Performed joins:
  - **Inner, left, and right joins** between employee and department.
  - Replaced state codes with **country names** using a join.
- Converted all column names to **lowercase**.
- Added `load_date`.
- Wrote the final DataFrame to **external tables** in:
  - Parquet format
  - CSV format  
  under the same database.

---

## How to Run

1. **Clone the repository**  
   `git clone https://github.com/DBT-Stewart/stewart_pyspark_assignment.git`

2. **Create virtual environment**  
   `python -m venv .venv && .venv\Scripts\activate` (Windows)

3. **Install dependencies**  
   `pip install -r requirements.txt`

4. **Run driver files**  
   Example:  
   `python src/question_3/drivers.py`

5. **Run tests**  
   `pytest test/`

---

## Author

**Stewart Prince P M**  
_Data Engineer | PySpark Developer | Big Data Enthusiast_

---

Feel free to the repository if you find it useful!
