import json
from kafka import KafkaProducer
import glob
import os
import time

RAW_DIR = "../data"

def push_latest_json_to_kafka(topic="news_raw", delay_ms=100):
    """
    Push data to Kafka with delay between messages
    
    Args:
        topic: Kafka topic name
        delay_ms: Delay in milliseconds between messages (simulate real-time)
    """
    # Kafka producer with better configs
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Thêm config để đảm bảo message được gửi thành công
        acks='all',  # Đợi tất cả replica confirm
        retries=3,   # Retry 3 lần nếu fail
        max_in_flight_requests_per_connection=1,  # Đảm bảo thứ tự message
    )

    # Tìm file JSON mới nhất
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")))
    if not files:
        print("❌ Không có file JSON nào!")
        return

    latest = files[-1]
    print(f"📄 Đang push file: {latest}")
    print(f"⏱️  Delay giữa các message: {delay_ms}ms")

    # Đọc file JSON
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Nếu data là list → gửi từng bài báo
    if isinstance(data, list):
        total = len(data)
        print(f"📊 Tổng số record: {total}")
        
        for idx, item in enumerate(data, 1):
            # Gửi message
            future = producer.send(topic, value=item)
            
            # Đợi confirm từ Kafka
            try:
                record_metadata = future.get(timeout=10)
                if idx % 10 == 0:  # Print mỗi 10 record
                    print(f"✔ Đã push {idx}/{total} record (offset: {record_metadata.offset})")
            except Exception as e:
                print(f"❌ Lỗi khi push record {idx}: {e}")
            
            # Delay để simulate real-time stream
            if delay_ms > 0 and idx < total:
                time.sleep(delay_ms / 1000.0)
        
        producer.flush()
        print(f"✅ Đã push xong {total} records!")

    elif isinstance(data, dict):
        future = producer.send(topic, value=data)
        record_metadata = future.get(timeout=10)
        producer.flush()
        print(f"✔ Push 1 object JSON vào Kafka (offset: {record_metadata.offset})")

    producer.close()
    print("🎉 DONE! Connection closed.")


if __name__ == "__main__":
    # Push với delay 100ms giữa các message (giống real-time hơn)
    push_latest_json_to_kafka(delay_ms=100)