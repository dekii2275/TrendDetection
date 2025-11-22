from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    to_timestamp,
    window,
    collect_list,
    date_format,
    when,
)
from pyspark.sql.types import StructType, StringType, LongType
import subprocess
import json

# ============================================================
# 1. SCHEMA STREAM
# ============================================================
schema = (
    StructType()
    .add("title", StringType())
    .add("timestamp", LongType())  # Unix time (seconds hoặc milliseconds)
    .add("topic", StringType())
)

spark = (
    SparkSession.builder.appName("WindowedTrendingNews")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2. READ FROM KAFKA STREAM
# ============================================================
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "news_raw")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# ============================================================
# 2.1 CHUẨN HÓA event_time (chống lỗi year 57861)
#      - Nếu timestamp > 10^12: coi là milliseconds, chia 1000
#      - Ngược lại: coi là seconds
# ============================================================
normalized_ts = when(
    col("timestamp") > 1000000000000,  # ~ 2001-09-09 nếu tính mili giây
    col("timestamp") / 1000,
).otherwise(col("timestamp"))

parsed = parsed.withColumn("event_time", to_timestamp(from_unixtime(normalized_ts)))

# ============================================================
# 3. WINDOWED AGGREGATION
#    - Window 30 phút
#    - Slide 5 phút
#    - Lấy list title theo (topic, window)
#    - CAST window.start/end sang STRING để tránh lỗi datetime Python
# ============================================================
windowed = (
    parsed.withWatermark("event_time", "30 minutes")
    .groupBy(
        window(col("event_time"), "30 minutes", "5 minutes"),  # window rolling
        col("topic"),
    )
    .agg(collect_list("title").alias("docs"))
    .select(
        col("topic"),
        col("docs"),
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("win_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("win_end"),
    )
)

# ============================================================
# 4. PROCESS MỖI WINDOW (foreachBatch)
# ============================================================


def process_window(batch_df, batch_id):
    print(f"\n================= WINDOW BATCH {batch_id} =================")

    rows = batch_df.collect()
    if not rows:
        print("→ Batch rỗng")
        return

    for row in rows:
        topic = row["topic"]
        docs = row["docs"]
        win_start = row["win_start"]
        win_end = row["win_end"]

        print("\n--- WINDOW ---")
        print(f"Topic: {topic}")
        print(f"Số bài trong cửa sổ: {len(docs)}")
        print(f"Cửa sổ: {win_start} → {win_end}")

        # BỎ QUA TOPIC ÍT BÀI
        if len(docs) < 5:
            print("→ Bỏ qua (quá ít bài)")
            continue

        print("→ Đủ bài! Gọi generative_topic.py ...")

        # Gọi script BERTopic + Gemini để đặt tên xu hướng
        cmd = [
            "python3",
            "/home/lok/dev/projects/Parallel_computing/TrendDetection/scripts/generative_topic.py",
            json.dumps(docs),
            topic,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # In stdout / stderr của generative_topic.py
        if result.stdout:
            print(">>> STDOUT từ generative_topic.py:")
            print(result.stdout)

        if result.stderr:
            print(">>> STDERR từ generative_topic.py:")
            print(result.stderr)


# ============================================================
# 5. GẮN FOREACHBATCH + CHECKPOINT
# ============================================================
query = (
    windowed.writeStream.foreachBatch(process_window)
    .option(
        "checkpointLocation",
        "/home/lok/dev/projects/Parallel_computing/TrendDetection/scripts/checkpoint_windowed",
    )
    .outputMode("update")  # dùng update cho window + watermark
    .start()
)

query.awaitTermination()
