from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        (1, "bronze"),
        (2, "silver"),
        (3, "gold"),
    ],
    ["id", "layer"],
)

df.write.mode("overwrite").parquet("s3a://spark/demo/example_job")

print("Wrote parquet dataset to s3a://spark/demo/example_job")

spark.stop()
