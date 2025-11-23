from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    to_timestamp,
    window,
    collect_list,
    date_format,
    count,
)
from pyspark.sql.types import StructType, StringType, LongType
import subprocess
import json

# Schema stream
schema = (
    StructType()
    .add("title", StringType())
    .add("timestamp", LongType())
    .add("topic", StringType())
)

spark = (
    SparkSession.builder.appName("SimpleTrendingNews")
    .config("spark.sql.shuffle.partitions", "2")  # Giảm partition
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("🚀 Bắt đầu Spark Streaming...")

# READ FROM KAFKA STREAM
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "news_raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON
parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Chuẩn hóa timestamp - timestamp đã ở dạng milliseconds
parsed_with_time = parsed.withColumn("event_time", to_timestamp(from_unixtime(col("timestamp") / 1000)))

# Filter dữ liệu hợp lệ
filtered = parsed_with_time.filter(
    col("event_time").isNotNull() & 
    col("title").isNotNull() & 
    col("topic").isNotNull()
)

# Windowed aggregation đơn giản
windowed = (
    filtered.withWatermark("event_time", "5 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes"),  # Window 10 phút
        col("topic"),
    )
    .agg(
        collect_list("title").alias("docs"),
        count("*").alias("doc_count")
    )
    .select(
        col("topic"),
        col("docs"),
        col("doc_count"),
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("win_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("win_end"),
    )
    .filter(col("doc_count") >= 2)  # Chỉ cần 2 bài trở lên
)

def simple_process_window(batch_df, batch_id):
    print(f"\n🔥 === BATCH {batch_id} ===")
    
    if batch_df.count() == 0:
        print("→ Batch rỗng, bỏ qua")
        return
    
    rows = batch_df.collect()
    print(f"→ Có {len(rows)} nhóm topic")
    
    for row in rows:
        topic = row["topic"]
        docs = row["docs"]
        doc_count = row["doc_count"]
        win_start = row["win_start"]
        win_end = row["win_end"]

        print(f"\n📰 Topic: {topic}")
        print(f"📊 Số bài: {doc_count}")
        print(f"⏰ Window: {win_start} → {win_end}")
        
        if doc_count >= 2:
            print("✅ Đủ điều kiện! Sample titles:")
            for i, title in enumerate(docs[:3]):
                print(f"   {i+1}. {title}")
            
            # Có thể gọi generative_topic.py ở đây nếu cần
            # print("→ [Sẽ gọi generative_topic.py]")
        else:
            print("→ Không đủ bài, bỏ qua")

# Start streaming
query = (
    windowed.writeStream
    .foreachBatch(simple_process_window)
    .option(
        "checkpointLocation",
        "/home/lok/dev/projects/Parallel_computing/TrendDetection/scripts/checkpoint_simple"
    )
    .outputMode("update")
    .trigger(processingTime='30 seconds')  # Trigger mỗi 30 giây
    .start()
)

print("📡 Streaming đang chạy... Nhấn Ctrl+C để dừng")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Dừng streaming...")
    query.stop()
    spark.stop()