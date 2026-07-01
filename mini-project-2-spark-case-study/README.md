# Mini Project 2 - Spark Case Study

This project compares Spark RDD, DataFrame, and Spark SQL APIs using a large dataset.

## Requirements

- Dataset with at least 1 million records
- At least 8 columns
- 10 analytical queries
- RDD implementation
- DataFrame implementation
- Spark SQL implementation
- Execution plan and performance comparison

## Dataset

Place your dataset here:

```text
data/raw/dataset.csv
source ../../.venv/bin/activate

spark-submit src/dataframe/queries_dataframe.py
spark-submit src/sql/queries_sql.py
spark-submit src/rdd/queries_rdd.py

Create DataFrame script:

```bash
cat > mini-project-2-spark-case-study/src/dataframe/queries_dataframe.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import time

spark = SparkSession.builder \
    .appName("Mini Project 2 - DataFrame Queries") \
    .getOrCreate()

DATA_PATH = "data/raw/dataset.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)

df.printSchema()

columns = df.columns

if len(columns) < 2:
    raise ValueError("Dataset must contain at least two columns.")

c0 = columns[0]
c1 = columns[1]

results = []

def run_query(name, func):
    print("=" * 100)
    print(name)
    start = time.time()
    result = func()
    result.explain(True)
    result.show(20, truncate=False)
    end = time.time()
    duration = end - start
    results.append((name, duration))
    print(f"Execution Time: {duration:.4f} seconds")

run_query("Q1_Filter_Non_Null", lambda: df.filter(col(c0).isNotNull()))

run_query("Q2_Count_By_Category", lambda: df.groupBy(c0).agg(count("*").alias("total_count")))

run_query("Q3_Average_By_Category", lambda: df.groupBy(c0).agg(avg(c1).alias("average_value")))

run_query("Q4_Max_By_Category", lambda: df.groupBy(c0).agg(spark_max(c1).alias("max_value")))

run_query("Q5_Min_By_Category", lambda: df.groupBy(c0).agg(spark_min(c1).alias("min_value")))

run_query("Q6_Sort_And_Rank", lambda: df.groupBy(c0).count().orderBy(col("count").desc()))

window_spec = Window.partitionBy(c0).orderBy(col(c1).desc())

run_query("Q7_Window_Ranking", lambda: df.withColumn("rank_value", rank().over(window_spec)))

run_query("Q8_Nested_Query_Equivalent", lambda: df.groupBy(c0).count().filter(col("count") > 10))

run_query("Q9_Caching_Test", lambda: df.cache().groupBy(c0).count())

run_query("Q10_Repartition_Test", lambda: df.repartition(8).groupBy(c0).count())

print("=" * 100)
print("Performance Summary")
for name, duration in results:
    print(f"{name}: {duration:.4f} seconds")

spark.stop()
