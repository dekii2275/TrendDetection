import os
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
import feedparser
from bs4 import BeautifulSoup

# ==========================
# 0. CONFIG
# ==========================

NEWS_FEEDS = [
    {"source": "vnexpress",  "rss": "https://vnexpress.net/rss/tin-moi-nhat.rss"},
    {"source": "tuoitre",    "rss": "https://tuoitre.vn/rss/tin-moi-nhat.rss"},
    {"source": "thanhnien",  "rss": "https://thanhnien.vn/rss/home.rss"},
    {"source": "vietnamnet", "rss": "http://vietnamnet.vn/home.rss"},
]

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TrendDetectionBot/1.0)"
}

OUTPUT_DIR = "../data/"


# ==========================
# 1. Utility: convert pubDate → epoch ms
# ==========================

def rss_pubdate_to_epoch_ms(pub_str: str | None) -> int:
    if not pub_str:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    try:
        dt = parsedate_to_datetime(pub_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


# ==========================
# 2. Extract topic
# ==========================

def extract_topic_from_html(soup: BeautifulSoup) -> str | None:
    # 1. meta property="article:section"
    meta1 = soup.find("meta", {"property": "article:section"})
    if meta1 and meta1.get("content"):
        return meta1["content"].strip()

    # 2. meta name="section"
    meta2 = soup.find("meta", {"name": "section"})
    if meta2 and meta2.get("content"):
        return meta2["content"].strip()

    # 3. breadcrumb dạng phổ biến
    crumb = soup.select_one("ul.breadcrumb a, div.breadcrumb a")
    if crumb:
        return crumb.get_text(strip=True)

    # 4. Tuổi Trẻ
    crumb2 = soup.select_one("nav.breadcrumb a, ol.breadcrumb a")
    if crumb2:
        return crumb2.get_text(strip=True)

    # 5. Thanh Niên
    crumb3 = soup.select_one("div.bread-crumb a")
    if crumb3:
        return crumb3.get_text(strip=True)

    return None


# ==========================
# 3. GET html → extract topic
# ==========================

def get_topic(url: str):
    try:
        resp = requests.get(url, headers=BASE_HEADERS, timeout=10)
        resp.raise_for_status()
    except:
        print(f"  [Lỗi] Không truy cập URL: {url}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    return extract_topic_from_html(soup)


# ==========================
# 4. MAIN CRAWLER (3 trường)
# ==========================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_records = []

    print("BẮT ĐẦU CRAWL DỮ LIỆU (3 trường: timestamp, title, topic)\n")

    for feed in NEWS_FEEDS:
        print(f"----- {feed['source']} -----")
        feed_data = feedparser.parse(feed["rss"])

        for entry in feed_data.entries[:100]:  # lấy 100 bài mới nhất
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            pub_raw = entry.get("published", "") or entry.get("updated", "")
            pub_ts = rss_pubdate_to_epoch_ms(pub_raw)

            print(f"Lấy bài: {title}")

            topic = get_topic(link)
            if not topic:
                print("  → Bỏ qua (không có topic)\n")
                continue

            all_records.append({
                "timestamp": pub_ts,
                "title": title,
                "topic": topic
            })

            time.sleep(0.2)

    # Lưu file
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(OUTPUT_DIR, f"news_slim_{ts}.json")

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"\nĐã lưu {len(all_records)} bài → {out_file}")


if __name__ == "__main__":
    main()