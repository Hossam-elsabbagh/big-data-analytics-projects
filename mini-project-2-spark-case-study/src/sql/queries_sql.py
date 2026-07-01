from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Mini Project 2 - Spark SQL Queries") \
    .getOrCreate()

DATA_PATH = "data/raw/dataset.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)
df.createOrReplaceTempView("dataset")

columns = df.columns

if len(columns) < 2:
    raise ValueError("Dataset must contain at least two columns.")

c0 = columns[0]
c1 = columns[1]

queries = {
    "Q1_Filter": f"SELECT * FROM dataset WHERE `{c0}` IS NOT NULL",
    "Q2_Count": f"SELECT `{c0}`, COUNT(*) AS total_count FROM dataset GROUP BY `{c0}`",
    "Q3_Average": f"SELECT `{c0}`, AVG(`{c1}`) AS average_value FROM dataset GROUP BY `{c0}`",
    "Q4_Max": f"SELECT `{c0}`, MAX(`{c1}`) AS max_value FROM dataset GROUP BY `{c0}`",
    "Q5_Min": f"SELECT `{c0}`, MIN(`{c1}`) AS min_value FROM dataset GROUP BY `{c0}`",
    "Q6_Sort": f"SELECT `{c0}`, COUNT(*) AS total_count FROM dataset GROUP BY `{c0}` ORDER BY total_count DESC",
    "Q7_Window": f"""
        SELECT *, RANK() OVER (PARTITION BY `{c0}` ORDER BY `{c1}` DESC) AS rank_value
        FROM dataset
    """,
    "Q8_Subquery": f"""
        SELECT *
        FROM (
            SELECT `{c0}`, COUNT(*) AS total_count
            FROM dataset
            GROUP BY `{c0}`
        ) temp
        WHERE total_count > 10
    """,
    "Q9_Distinct": f"SELECT DISTINCT `{c0}` FROM dataset",
    "Q10_Non_Null_Count": f"SELECT COUNT(*) AS non_null_count FROM dataset WHERE `{c0}` IS NOT NULL"
}

results = []

for name, query in queries.items():
    print("=" * 100)
    print(name)
    start = time.time()
    result = spark.sql(query)
    result.explain(True)
    result.show(20, truncate=False)
    end = time.time()
    duration = end - start
    results.append((name, duration))
    print(f"Execution Time: {duration:.4f} seconds")

print("=" * 100)
print("Performance Summary")
for name, duration in results:
    print(f"{name}: {duration:.4f} seconds")

spark.stop()
