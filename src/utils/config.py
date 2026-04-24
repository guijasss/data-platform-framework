from pyspark.sql import SparkSession


SPARK: SparkSession = SparkSession.builder.getOrCreate()
WATERMARK_COLUMN = "processed_at"