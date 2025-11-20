import os
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# --------- 0. Cấu hình nguồn RSS các trang báo --------- #

NEWS_FEEDS = [
    {
        "source": "vnexpress",
        "rss": "https://vnexpress.net/rss/tin-moi-nhat.rss",
    },
    {
        "source": "tuoitre",
        "rss": "https://tuoitre.vn/rss/tin-moi-nhat.rss",
    },
    {
        "source": "thanhnien",
        "rss": "https://thanhnien.vn/rss/home.rss",
    },
    {
        "source": "vietnamnet",
        "rss": "http://vietnamnet.vn/home.rss",
    },
]

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TrendDetectionBot/1.0; +https://example.com)"
}

# Thư mục NiFi sẽ đọc file từ đây
OUTPUT_DIR = "/home/dekii2275/TrendDectection/data/input_nifi"


# --------- 1. Hàm tiện ích chuyển thời gian --------- #

def rss_pubdate_to_epoch_ms(pub_str: str | None) -> int:
    """
    Chuyển pubDate (chuỗi RSS, ví dụ: 'Thu, 14 Nov 2025 09:30:00 +0700')
    thành epoch milliseconds. Nếu không parse được thì dùng thời gian hiện tại.
    """
    if not pub_str:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    try:
        dt = parsedate_to_datetime(pub_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


# --------- 1b. Đọc URL đã crawl trong file gần nhất --------- #

def load_seen_urls_from_latest_file(output_dir: str) -> set[str]:
    """
    Tìm file news_*.json mới nhất trong output_dir,
    đọc danh sách bài báo và lấy ra tập các URL đã xuất hiện.
    Nếu không có file nào hoặc lỗi đọc -> trả về set() rỗng.
    """
    if not os.path.isdir(output_dir):
        return set()

    files = [
        f for f in os.listdir(output_dir)
        if f.startswith("news_") and f.endswith(".json")
    ]
    if not files:
        return set()

    latest_file = max(files)  # vì tên có dạng YYYYMMDD_HHMMSS nên sort theo tên = sort theo thời gian
    latest_path = os.path.join(output_dir, latest_file)

    print(f"Đang nạp URL cũ từ file gần nhất: {latest_path}")
    try:
        with open(latest_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        seen = set()
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "url" in item:
                    seen.add(item["url"])
        print(f"  -> Đã nạp {len(seen)} URL từ file gần nhất.")
        return seen
    except Exception as e:
        print(f"  [Cảnh báo] Không đọc được file gần nhất {latest_path}: {e}")
        return set()


# --------- 2. Lấy danh sách bài viết từ RSS --------- #

def get_articles_from_rss(rss_url: str, max_items: int = 15):
    """
    Đọc RSS feed, trả về list các dict: {title, link, published}
    Đã sort theo thời gian (mới nhất trước), rồi cắt lấy max_items.
    """
    print(f"Đang đọc RSS: {rss_url}")
    feed = feedparser.parse(rss_url)

    articles = []
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        pub_raw = entry.get("published", "") or entry.get("updated", "")
        pub_ts = rss_pubdate_to_epoch_ms(pub_raw)

        articles.append(
            {
                "title": title,
                "link": link,
                "published": pub_ts,
            }
        )

    # Sắp xếp theo thời gian giảm dần: mới nhất đứng đầu
    articles.sort(key=lambda x: x["published"], reverse=True)

    # Chỉ lấy max_items bài mới nhất
    if max_items is not None:
        articles = articles[:max_items]

    print(f"  -> Lấy được {len(articles)} bài (mới nhất) từ RSS.")
    return articles


# --------- 3. Lấy nội dung bài viết từ HTML trang báo --------- #

def get_article_content(url: str) -> str:
    """
    Lấy nội dung text chính của bài báo từ URL.
    Ở đây dùng BeautifulSoup, gom các thẻ <p> (generic).
    """
    try:
        resp = requests.get(url, headers=BASE_HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    [Lỗi] Không lấy được nội dung từ {url}: {e}")
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    paragraphs = []
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if len(text) >= 30:
            paragraphs.append(text)

    content = "\n".join(paragraphs)
    return content


# --------- 4. Main: crawl mới và ghi 1 file JSON riêng cho NiFi --------- #

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Bắt đầu crawl dữ liệu từ các trang báo (mỗi lần chạy -> 1 file JSON mới)...")

    # 4.0. Nạp URL đã crawl ở lần gần nhất để tránh trùng
    seen_urls = load_seen_urls_from_latest_file(OUTPUT_DIR)

    all_records = []
    # seen_urls hiện giờ đã chứa cả URL cũ (lần trước) + sẽ thêm URL mới trong lần chạy này

    # 4.1. Crawl bài mới từ từng nguồn
    for feed_cfg in NEWS_FEEDS:
        source_name = feed_cfg["source"]
        rss_url = feed_cfg["rss"]

        print(f"\nNguồn: {source_name}")
        articles = get_articles_from_rss(rss_url, max_items=15)

        for idx, art in enumerate(articles, start=1):
            url = art["link"]

            # Nếu URL đã thấy (trong file gần nhất hoặc trong batch hiện tại) -> bỏ qua
            if url in seen_urls:
                print(f"  [{idx}/{len(articles)}] Bỏ qua (trùng URL đã tồn tại): {art['title']}")
                continue

            print(f"  [{idx}/{len(articles)}] Lấy bài mới: {art['title']}")
            text = get_article_content(url)

            record = {
                "source": source_name,        # giữ 'source' để sau này group theo báo
                "url": url,
                "title": art["title"],
                "timestamp": art["published"],  # epoch ms
                "text": text if text else art["title"],
            }

            all_records.append(record)
            seen_urls.add(url)   # đánh dấu URL này đã được crawl trong lần chạy hiện tại
            time.sleep(0.2)

    # 4.2. Ghi ra 1 file JSON mới (mảng các bài báo)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_path   = os.path.join(OUTPUT_DIR, f"news_{ts}.json.tmp")
    final_path = os.path.join(OUTPUT_DIR, f"news_{ts}.json")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    os.replace(tmp_path, final_path)
    print(f"\nĐã lưu {len(all_records)} bản ghi vào {final_path}")


if __name__ == "__main__":
    main()
