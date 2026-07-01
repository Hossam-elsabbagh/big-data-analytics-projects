from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("Mini Project 3 - ALS Training") \
    .getOrCreate()

DATA_PATH = "data/raw/ratings.csv"
MODEL_PATH = "data/models/als_model"

ratings = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)

ratings = ratings.selectExpr(
    "CAST(user_id AS INT) AS user_id",
    "CAST(item_id AS INT) AS item_id",
    "CAST(rating AS FLOAT) AS rating",
    "timestamp"
).dropna()

train, test = ratings.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    rank=10,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop",
    nonnegative=True
)

model = als.fit(train)

predictions = model.transform(test)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse}")

if rmse > 1.5:
    print("RMSE is above 1.5. Tune rank, regParam, or maxIter.")

model.write().overwrite().save(MODEL_PATH)
print(f"Model saved to {MODEL_PATH}")

top5 = model.recommendForAllUsers(5)
top5.show(10, truncate=False)

spark.stop()
