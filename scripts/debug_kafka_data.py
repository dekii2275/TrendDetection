#!/usr/bin/env python3
"""
Script debug để kiểm tra dữ liệu từ Kafka và timestamp formatting
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    to_timestamp,
    date_format,
)
from pyspark.sql.types import StructType, StringType, LongType
import time

# Schema giống với process_stream.py
schema = (
    StructType()
    .add("title", StringType())
    .add("timestamp", LongType())
    .add("topic", StringType())
)

spark = (
    SparkSession.builder.appName("DebugKafkaData")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("🔍 Debug: Đọc dữ liệu từ Kafka...")

# Đọc một batch dữ liệu từ Kafka (không phải stream)
try:
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "news_raw")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    
    print(f"📊 Tổng số message trong Kafka: {df.count()}")
    
    # Parse JSON
    parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    
    print("📋 Schema của dữ liệu đã parse:")
    parsed.printSchema()
    
    print("📋 Sample dữ liệu:")
    parsed.show(5, truncate=False)
    
    # Test timestamp conversion
    normalized_ts = col("timestamp") / 1000
    parsed_with_time = parsed.withColumn("event_time", to_timestamp(from_unixtime(normalized_ts)))
    
    print("📋 Dữ liệu sau khi convert timestamp:")
    parsed_with_time.select("title", "timestamp", "event_time", "topic").show(5, truncate=False)
    
    # Kiểm tra phân bố theo topic
    print("📊 Phân bố theo topic:")
    parsed_with_time.groupBy("topic").count().show()
    
    # Kiểm tra phạm vi thời gian
    print("📊 Phạm vi thời gian:")
    parsed_with_time.select(
        date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("formatted_time")
    ).agg(
        {"formatted_time": "min"},
        {"formatted_time": "max"}
    ).show()

except Exception as e:
    print(f"❌ Lỗi khi đọc từ Kafka: {e}")
    print("💡 Hãy kiểm tra:")
    print("   - Kafka server có đang chạy không?")
    print("   - Topic 'news_raw' có tồn tại không?")
    print("   - Có dữ liệu trong topic không?")

finally:
    spark.stop()