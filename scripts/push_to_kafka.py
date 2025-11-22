import json
from kafka import KafkaProducer
import glob
import os

RAW_DIR = "../data"  # Thay đổi đường dẫn nếu cần

def push_latest_json_to_kafka(topic="news_raw"):
    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # tìm file JSON mới nhất
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")))
    if not files:
        print("❌ Không có file JSON nào!")
        return

    latest = files[-1]
    print(f"📄 Đang push file: {latest}")

    # đọc file JSON
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    # nếu data là list → gửi từng bài báo
    if isinstance(data, list):
        for idx, item in enumerate(data):
            producer.send(topic, value=item)
            print(f"✔ Push item {idx+1}/{len(data)} vào Kafka")
        producer.flush()

    elif isinstance(data, dict):
        producer.send(topic, value=data)
        producer.flush()
        print("✔ Push 1 object JSON vào Kafka")

    print("🎉 DONE! Đã đẩy xong vào Kafka")


if __name__ == "__main__":
    push_latest_json_to_kafka()
