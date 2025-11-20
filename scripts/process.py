# scripts/process.py
import json
from pathlib import Path

from preprocess_data import preprocess_text, PROJECT_ROOT

INPUT_DIR = PROJECT_ROOT / "data" / "input_nifi"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def get_latest_input_file(input_dir: Path) -> Path:
    files = sorted(input_dir.glob("news_*.json"))
    if not files:
        raise FileNotFoundError(f"Không tìm thấy file 'news_*.json' trong {input_dir}")
    return files[-1]


def process_one_file(input_path: Path) -> Path:
    if not input_path.is_file():
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}")

    print(f"[INFO] Đang xử lý file: {input_path}")

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("File JSON phải chứa một list các record (bài báo).")

    processed_records = []
    for rec in data:
        if not isinstance(rec, dict):
            continue

        title = rec.get("title", "")
        body = rec.get("text", "")
        raw_text = (title + ". " + body).strip()

        processed_text = preprocess_text(raw_text)

        new_rec = dict(rec)
        new_rec["processed_text"] = processed_text
        processed_records.append(new_rec)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / input_path.name

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(processed_records, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Đã xử lý {len(processed_records)} record.")
    print(f"[INFO] File đã lưu tại: {out_path}")
    return out_path


def main(input_file: str | None = None):
    if input_file is None:
        input_path = get_latest_input_file(INPUT_DIR)
    else:
        input_path = Path(input_file).expanduser().resolve()

    process_one_file(input_path)


if __name__ == "__main__":
    main()
