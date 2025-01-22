
# Window Functions in SQL and PySpark: A Comprehensive Guide

Window functions are powerful tools for performing calculations across a set of table rows related to the current row. They enable advanced analytics without collapsing data, making them indispensable in data engineering and analytics. This guide covers key window functions, including LEAD, LAG, and aggregate functions like SUM, AVG, and ROW_NUMBER.

## Syntax in SQL

```sql
SELECT column_name, 
       window_function() OVER (
           PARTITION BY column_name
           ORDER BY column_name
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS alias_name
FROM table_name;
```

## Syntax in PySpark

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag, sum, avg, row_number

window_spec = Window.partitionBy("column_name").orderBy("column_name")
df = df.withColumn("new_column", function("column_name").over(window_spec))
```

## Key Functions

### 1. LEAD
Fetches the next row’s value relative to the current row.

**SQL Example:**
```sql
SELECT id, value, 
       LEAD(value, 1) OVER (PARTITION BY category ORDER BY date) AS next_value
FROM sales;
```

**PySpark Example:**
```python
df = df.withColumn("next_value", lead("value", 1).over(window_spec))
```

### 2. LAG
Fetches the previous row’s value relative to the current row.

**SQL Example:**
```sql
SELECT id, value, 
       LAG(value, 1) OVER (PARTITION BY category ORDER BY date) AS previous_value
FROM sales;
```

**PySpark Example:**
```python
df = df.withColumn("previous_value", lag("value", 1).over(window_spec))
```

### 3. ROW_NUMBER
Assigns a unique number to rows within a partition based on the specified order.

**SQL Example:**
```sql
SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rank
FROM sales;
```

**PySpark Example:**
```python
df = df.withColumn("rank", row_number().over(window_spec))
```

### 4. DENSE_RANK
Assigns a rank to rows within a partition, with no gaps in ranking values.

**SQL Example:**
```sql
SELECT id, DENSE_RANK() OVER (PARTITION BY category ORDER BY sales DESC) AS dense_rank
FROM sales;
```

**PySpark Example:**
```python
from pyspark.sql.functions import dense_rank
df = df.withColumn("dense_rank", dense_rank().over(window_spec))
```

### 5. ROWS BETWEEN
Specifies the range of rows to include in a window frame.

**SQL Example:**
```sql
SELECT id, SUM(sales) OVER (
       PARTITION BY category
       ORDER BY date
       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_sum
FROM sales;
```

**PySpark Example:**
```python
window_spec = window_spec.rowsBetween(-2, 0)
df = df.withColumn("moving_sum", sum("sales").over(window_spec))
```

## Example Problems

### Problem 1: Identify the sales difference between consecutive days for each category.

**SQL Solution:**
```sql
SELECT category, date, sales, 
       sales - LAG(sales, 1) OVER (PARTITION BY category ORDER BY date) AS sales_diff
FROM sales_data;
```

**PySpark Solution:**
```python
df = df.withColumn("sales_diff", (col("sales") - lag("sales", 1).over(window_spec)))
```

### Problem 2: Calculate the cumulative total sales for each category.

**SQL Solution:**
```sql
SELECT category, date, 
       SUM(sales) OVER (PARTITION BY category ORDER BY date) AS cumulative_sales
FROM sales_data;
```

**PySpark Solution:**
```python
df = df.withColumn("cumulative_sales", sum("sales").over(window_spec))
```

### Problem 3: Rank products by sales within each category.

**SQL Solution:**
```sql
SELECT category, product, 
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rank
FROM product_data;
```

**PySpark Solution:**
```python
df = df.withColumn("rank", row_number().over(window_spec))
```

---

Window functions offer immense flexibility for analytical tasks. Mastering these functions empowers you to extract valuable insights from data efficiently.
