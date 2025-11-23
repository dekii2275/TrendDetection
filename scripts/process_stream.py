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
    count,
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
    .add("timestamp", LongType())
    .add("topic", StringType())
)

spark = (
    SparkSession.builder.appName("WindowedTrendingNews")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2. READ FROM KAFKA STREAM - ĐÃ FIX
# ============================================================
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "news_raw")
    .option("startingOffsets", "latest")
    # 🔥 FIX: Thêm option này để không fail khi mất data
    .option("failOnDataLoss", "false")
    # 🔥 FIX: Tăng timeout nếu Kafka chậm
    .option("kafkaConsumer.pollTimeoutMs", "10000")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# ============================================================
# 2.1 CHUẨN HÓA event_time - FIX LOGIC
# ============================================================
# Timestamp trong data đã ở dạng milliseconds, cần chia cho 1000 để có seconds
normalized_ts = col("timestamp") / 1000

parsed = parsed.withColumn("event_time", to_timestamp(from_unixtime(normalized_ts)))

# ============================================================
# 3. WINDOWED AGGREGATION - CẢI THIỆN LOGIC
# ============================================================
# Thêm filter để chỉ lấy dữ liệu hợp lệ
filtered_parsed = parsed.filter(
    col("event_time").isNotNull() & 
    col("title").isNotNull() & 
    col("topic").isNotNull()
)

windowed = (
    filtered_parsed.withWatermark("event_time", "10 minutes")  # Giảm watermark
    .groupBy(
        window(col("event_time"), "15 minutes", "5 minutes"),  # Giảm window size
        col("topic"),
    )
    .agg(
        collect_list("title").alias("docs"),
        count("*").alias("doc_count")  # Thêm count để debug
    )
    .select(
        col("topic"),
        col("docs"),
        col("doc_count"),
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("win_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("win_end"),
    )
    .filter(col("doc_count") > 0)  # Chỉ lấy window có dữ liệu
)

# ============================================================
# 4. PROCESS MỖI WINDOW (foreachBatch)
# ============================================================


def process_window(batch_df, batch_id):
    print(f"\n================= WINDOW BATCH {batch_id} =================")

    # Debug: Kiểm tra batch_df trước khi collect
    print(f"Batch DataFrame có {batch_df.count()} dòng")
    
    rows = batch_df.collect()
    if not rows:
        print("→ Batch rỗng sau collect()")
        return

    print(f"→ Có {len(rows)} nhóm topic trong batch")

    for row in rows:
        topic = row["topic"]
        docs = row["docs"]
        doc_count = row["doc_count"]
        win_start = row["win_start"]
        win_end = row["win_end"]

        print(f"\n--- WINDOW TOPIC: {topic} ---")
        print(f"Số bài trong cửa sổ: {doc_count} (docs list length: {len(docs) if docs else 0})")
        print(f"Cửa sổ: {win_start} → {win_end}")

        # Kiểm tra docs không rỗng và có đủ bài
        if not docs or len(docs) < 3:  # Giảm threshold từ 5 xuống 3
            print(f"→ Bỏ qua topic {topic} (quá ít bài: {len(docs) if docs else 0})")
            continue

        print(f"→ Topic {topic} có đủ bài! Gọi generative_topic.py ...")
        print(f"→ Danh sách các title: {docs[:3]}...")  # In vài title đầu để debug

        try:
            cmd = [
                "python3",
                "/home/lok/dev/projects/Parallel_computing/TrendDetection/scripts/generative_topic.py",
                json.dumps(docs, ensure_ascii=False),  # Thêm ensure_ascii=False cho tiếng Việt
                topic,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                print("✅ Xử lý thành công!")
                if result.stdout:
                    print(">>> OUTPUT:")
                    print(result.stdout)
            else:
                print(f"❌ Lỗi với exit code: {result.returncode}")
                if result.stderr:
                    print(">>> STDERR:")
                    print(result.stderr)

        except subprocess.TimeoutExpired:
            print("⏰ Timeout khi chạy generative_topic.py")
        except Exception as e:
            print(f"❌ Lỗi exception: {e}")


# ============================================================
# 5. GẮN FOREACHBATCH + CHECKPOINT
# ============================================================
query = (
    windowed.writeStream.foreachBatch(process_window)
    .option(
        "checkpointLocation",
        "/home/lok/dev/projects/Parallel_computing/TrendDetection/scripts/checkpoint_windowed",
    )
    .outputMode("update")
    .start()
)

query.awaitTermination()
