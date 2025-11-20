# scripts/bucketize_and_count.py
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from preprocess_data import PROJECT_ROOT

# Thư mục chứa các file đã xử lý
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Kích thước cửa sổ thời gian (phút)
WINDOW_MINUTES = 10


def load_processed_articles() -> list[dict]:
    """
    Đọc tất cả file JSON trong data/processed,
    trả về list các record có timestamp + processed_text.
    """
    if not PROCESSED_DIR.is_dir():
        raise FileNotFoundError(f"Không tìm thấy thư mục {PROCESSED_DIR}")

    all_records: list[dict] = []
    files = sorted(PROCESSED_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"Không có file JSON nào trong {PROCESSED_DIR}")

    print(f"[INFO] Tìm thấy {len(files)} file trong {PROCESSED_DIR}")

    for f in files:
        print(f"  - Đọc: {f.name}")
        with f.open("r", encoding="utf-8") as fp:
            data = json.load(fp)

        for rec in data:
            if not isinstance(rec, dict):
                continue

            ts = rec.get("timestamp")
            text = rec.get("processed_text", "")
            if ts is None or not text:
                continue

            all_records.append(rec)

    # Sắp xếp theo thời gian
    all_records.sort(key=lambda r: r["timestamp"])
    print(f"[INFO] Tổng số bản ghi hợp lệ: {len(all_records)}")
    return all_records


def get_bucket_id(ts_ms: int, window_minutes: int = WINDOW_MINUTES) -> int:
    """
    Tính bucket_id từ timestamp (epoch milliseconds).
    """
    window_ms = window_minutes * 60 * 1000
    return ts_ms // window_ms


def build_time_buckets(
    articles: list[dict], window_minutes: int = WINDOW_MINUTES
) -> list[dict]:
    """
    Từ list bài báo -> tạo các "bucket" thời gian.
    Mỗi bucket chứa:
      - bucket_id
      - start_ts (epoch ms)
      - doc_indices: index các bài trong list articles
      - term_counts: Counter(term -> freq) trong bucket
    """
    window_ms = window_minutes * 60 * 1000
    buckets: dict[int, dict] = {}

    for idx, rec in enumerate(articles):
        ts = int(rec["timestamp"])
        bucket_id = ts // window_ms

        if bucket_id not in buckets:
            start_ts = bucket_id * window_ms
            buckets[bucket_id] = {
                "bucket_id": bucket_id,
                "start_ts": start_ts,
                "doc_indices": [],
                "term_counts": Counter(),
            }

        b = buckets[bucket_id]
        b["doc_indices"].append(idx)

        # processed_text đã token hóa, chỉ cần split
        tokens = str(rec.get("processed_text", "")).split()
        b["term_counts"].update(tokens)

    # Trả về list đã sort theo thời gian
    bucket_list = sorted(buckets.values(), key=lambda x: x["bucket_id"])
    return bucket_list


def print_bucket_summary(
    buckets: list[dict], n_buckets: int = 5, top_k: int = 10
) -> None:
    """
    In thử vài bucket đầu để xem dữ liệu ổn chưa.
    """
    print(f"\n[INFO] Có tổng cộng {len(buckets)} bucket thời gian.")
    for b in buckets[:n_buckets]:
        dt = datetime.fromtimestamp(b["start_ts"] / 1000.0)
        print(
            f"\n=== Bucket {b['bucket_id']} | "
            f"start = {dt} | docs = {len(b['doc_indices'])}"
        )
        for term, cnt in b["term_counts"].most_common(top_k):
            print(f"  {term:20s} {cnt}")


def main():
    articles = load_processed_articles()
    buckets = build_time_buckets(articles, WINDOW_MINUTES)
    print_bucket_summary(buckets, n_buckets=5, top_k=10)


if __name__ == "__main__":
    main()
