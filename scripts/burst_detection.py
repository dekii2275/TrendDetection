# scripts/burst_detection.py
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from bucketize_and_count import (
    load_processed_articles,
    build_time_buckets,
    WINDOW_MINUTES,
)

# Một tập stopword riêng cho BURST (không đụng gì tới preprocess_text)
BURST_STOPWORDS = {
    "là", "và", "của", "có", "những", "một", "các", "trong", "khi", "đã",
    "tại", "từ", "này", "đó", "nên", "thì", "đang", "rất", "rằng",
    "cũng", "nhưng", "hay", "hoặc", "nếu", "sau", "trước", "với",
    "cho", "được", "bằng", "đến", "như", "ở", "nơi",
    "tôi", "mình", "bạn", "chúng_tôi", "chúng_ta", "anh", "chị", "em",
}

HISTORY_WINDOWS = 6    # dùng 6 cửa sổ trước làm “nền”
MIN_COUNT = 3          # ít nhất xuất hiện 3 lần trong bucket
Z_THRESHOLD = 3.0      # Z-score > 3 coi là bursty


def compute_bursty_keywords(
    buckets: List[dict],
    history_windows: int = HISTORY_WINDOWS,
    min_count: int = MIN_COUNT,
    z_threshold: float = Z_THRESHOLD,
) -> List[dict]:
    """
    Với mỗi bucket, tính các từ 'bursty' dựa trên Z-score của tần suất
    so với lịch sử các bucket trước.

    Trả về list:
      {
        'bucket_id': ...,
        'start_ts': ...,
        'bursty': [(term, freq, z_score), ...]  # sort theo z giảm dần
      }
    """
    # history[term] = list các f_t(term) ở các bucket trước (tối đa history_windows)
    history: Dict[str, List[int]] = {}
    results: List[dict] = []

    for b in buckets:
        term_counts = b["term_counts"]
        bursty_terms: List[Tuple[str, int, float]] = []

        # 1. Tính Z-score cho từng term trong bucket hiện tại
        for term, f_t in term_counts.items():
            # Bỏ các từ siêu thường / stopword khi xét burst
            if term in BURST_STOPWORDS:
                continue
            if len(term) <= 1:
                continue

            hist = history.get(term, [])

            if len(hist) >= 2:
                mu = sum(hist) / len(hist)
                var = sum((x - mu) ** 2 for x in hist) / len(hist)
                sigma = var ** 0.5
            else:
                mu = 0.0
                sigma = 1.0  # tránh chia 0, coi nền ~0

            if sigma == 0:
                sigma = 1.0

            if f_t >= min_count:
                z = (f_t - mu) / sigma
                if z >= z_threshold and f_t > mu:
                    bursty_terms.append((term, f_t, z))

        # 2. Cập nhật history sau khi đã tính xong bucket
        for term, f_t in term_counts.items():
            hist = history.setdefault(term, [])
            hist.append(f_t)
            if len(hist) > history_windows:
                hist.pop(0)  # giữ tối đa history_windows cửa sổ gần nhất

        # Sort bursty terms theo Z-score giảm dần
        bursty_terms.sort(key=lambda x: x[2], reverse=True)

        results.append(
            {
                "bucket_id": b["bucket_id"],
                "start_ts": b["start_ts"],
                "bursty": bursty_terms,
            }
        )

    return results


def print_bursty_summary(bursty_by_bucket: List[dict], top_k: int = 10) -> None:
    """
    In ra các từ bursty của từng bucket (nếu có).
    """
    for b in bursty_by_bucket:
        if not b["bursty"]:
            continue

        dt = datetime.fromtimestamp(b["start_ts"] / 1000.0)
        print(f"\n=== Bucket {b['bucket_id']} | {dt} ===")
        for term, freq, z in b["bursty"][:top_k]:
            print(f"  {term:20s} f={freq:3d}  z={z:5.2f}")


def main():
    print("[*] Đang load articles & build buckets...")
    articles = load_processed_articles()
    buckets = build_time_buckets(articles, WINDOW_MINUTES)

    print(f"[*] Có {len(buckets)} bucket, bắt đầu tính bursty keywords...")
    bursty_by_bucket = compute_bursty_keywords(buckets)

    print_bursty_summary(bursty_by_bucket, top_k=10)


if __name__ == "__main__":
    main()
