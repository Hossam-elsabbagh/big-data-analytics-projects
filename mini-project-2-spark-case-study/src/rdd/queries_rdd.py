from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Mini Project 2 - RDD Queries") \
    .getOrCreate()

sc = spark.sparkContext

DATA_PATH = "data/raw/dataset.csv"

raw = sc.textFile(DATA_PATH)
header = raw.first()
data = raw.filter(lambda row: row != header).map(lambda row: row.split(","))

def safe_get(row, index):
    return row[index] if len(row) > index else ""

def run_query(name, func):
    print("=" * 100)
    print(name)
    start = time.time()
    result = func()
    for row in result.take(20):
        print(row)
    end = time.time()
    print(f"Execution Time: {end - start:.4f} seconds")

run_query("Q1_Filter_Non_Empty", lambda: data.filter(lambda row: safe_get(row, 0) != ""))

run_query("Q2_Count_By_First_Column", lambda: data.map(lambda row: (safe_get(row, 0), 1)).reduceByKey(lambda a, b: a + b))

run_query("Q3_Sort_Counts", lambda: data.map(lambda row: (safe_get(row, 0), 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False))

run_query("Q4_Distinct_First_Column", lambda: data.map(lambda row: safe_get(row, 0)).distinct().map(lambda x: (x, 1)))

run_query("Q5_Top_Categories", lambda: data.map(lambda row: (safe_get(row, 0), 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False))

spark.stop()
