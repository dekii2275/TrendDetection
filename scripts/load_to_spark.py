from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType

spark = SparkSession.builder.appName("NewsStreamTest").getOrCreate()

schema = StructType() \
    .add("title", StringType()) \
    .add("timestamp", LongType()) \
    .add("topic", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news_raw") \
    .option("startingOffsets", "earliest") \
    .load()

# FIXED: đặt alias để Spark parse được JSON
df = df.selectExpr("CAST(value AS STRING) as json")

news_df = df.select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

query = news_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
