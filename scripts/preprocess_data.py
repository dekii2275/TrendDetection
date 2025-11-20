# scripts/preprocess_data.py
from pathlib import Path
import re
import unicodedata
from underthesea import word_tokenize

# 1. Gốc project (dùng cho các script khác)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 2. Regex phụ trợ (để dành nếu sau này cần)
URL_RE = re.compile(r"https?://\S+")
EMAIL_RE = re.compile(r"\S+@\S+")
NUM_RE = re.compile(r"\d+([\.,]\d+)*")

# Regex loại bỏ dấu câu: . , ; : ! ? " ' … ( ) [ ] { } - / v.v.
PUNCT_RE = re.compile(r"[.,;:!?\"'“”‘’()\[\]{}\-\–—…/]")

# 3. Pattern nhận diện phần “đuôi báo / UI” (Tuổi Trẻ)
TT_FOOTER_PATTERNS = [
    "Chuyển sao tặng cho thành viên",
    "Tuổi Trẻ Online sẽ gởi đến bạn",
    "Hiện chưa có bình luận nào, hãy là người đầu tiên bình luận",
    "Giấy phép hoạt động báo điện tử",
    "© Copyright",
    "TuoiTre Online, All rights reserved",
    "Tuổi Trẻ Online giữ bản quyền",
    "Địa chỉ:",
    "Hotline:",
    "Phòng Quảng Cáo Báo Tuổi Trẻ",
    "Đăng ký email",
    "Thông tin đăng nhập không đúng",
    "Tài khoản bị khóa",
    "Có lỗi phát sinh",
    "Mật khẩu phải có ít nhất",
    "Vui lòng nhập thông tin và ý kiến của bạn",
    "Tuổi Trẻ Sao",
    "Thêm chuyên mục, tăng trải nghiệm vớiTuổi Trẻ Sao",
    "Tuổi Trẻ Saonhằm từng bước nâng cao",
]


# -------- Bước 0: Cắt bỏ footer / boilerplate -------- #
def strip_boilerplate_lines(text: str) -> str:
    """
    Loại bỏ các dòng footer/UI (copyright, Tuổi Trẻ Sao, thông báo lỗi...).
    Nếu gặp dòng chứa pattern footer thì dừng luôn (xem như hết bài).
    """
    if not isinstance(text, str):
        return ""

    lines = text.splitlines()
    cleaned_lines = []
    seen_lines = set()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Nếu line chứa 1 trong các pattern footer -> coi như hết bài
        if any(pat in line for pat in TT_FOOTER_PATTERNS):
            break

        # Bỏ dòng lặp y hệt (câu UI lặp lại nhiều lần)
        if line in seen_lines:
            continue
        seen_lines.add(line)

        cleaned_lines.append(line)

    return "\n".join(cleaned_lines)


# -------- Bước 1: Chuẩn hóa unicode -------- #
def normalize_unicode(text: str) -> str:
    """
    Chuẩn hóa unicode về dạng chuẩn NFC (kết hợp dấu + ký tự).
    Giúp thống nhất cách mã hóa dấu tiếng Việt.
    """
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize("NFC", text)


# -------- Bước 2: Chuẩn hóa cách gõ dấu tiếng Việt -------- #
def normalize_vietnamese_diacritics(text: str) -> str:
    """
    Chuẩn hóa một số cách gõ dấu tiếng Việt không chuẩn (nếu có).
    Ở đây tạm thời dùng unicode NFC là đủ cho đa số trường hợp.
    Nếu phát hiện pattern sai (vd: 'òa' -> 'oà') thì có thể bổ sung mapping ở đây.
    """
    # Hiện tại chỉ trả lại text, vì normalize_unicode đã xử lý cơ bản.
    return text


# -------- Bước 3: Chuẩn hóa chữ viết thường -------- #
def normalize_case(text: str) -> str:
    """
    Đưa toàn bộ text về chữ thường.
    """
    return text.lower()


# -------- Bước 4 & 5: Tách từ, đưa về 1 dòng -------- #
def clean_and_tokenize(text: str) -> str:
    """
    - Đưa cả đoạn văn về trên 1 dòng
    - Xóa các khoảng cách thừa
    - Loại bỏ dấu câu (. , ; : ! ? …, ngoặc, gạch nối, ...)
    - Tách từ tiếng Việt

    KHÔNG xóa số, KHÔNG lọc stopword.
    Mục tiêu: chỉ chuẩn hóa, không làm mất thông tin nội dung.
    """
    # Chuyển xuống dòng, tab thành space
    text = re.sub(r"[\r\n\t]", " ", text)

    # Loại bỏ dấu câu: thay bằng space
    text = PUNCT_RE.sub(" ", text)

    # Gom nhiều khoảng trắng thành 1
    text = re.sub(r"\s+", " ", text).strip()

    # Tách từ tiếng Việt (giữ nguyên số, chữ, dấu)
    tokens = word_tokenize(text)

    processed_tokens = []
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue

        # Không lọc số, không lọc stopword
        processed_tokens.append(tok)

    # Đưa về 1 dòng: các token cách nhau 1 space
    return " ".join(processed_tokens)


# -------- Hàm chính dùng trong toàn project -------- #
def preprocess_text(raw_text: str) -> str:
    """
    Pipeline tiền xử lý hoàn chỉnh, theo 5 bước:

    0. (Thêm) Cắt bỏ footer/UI của báo (Tuổi Trẻ).
    1. Chuẩn hóa unicode
    2. Chuẩn hóa cách gõ dấu tiếng Việt
    3. Chuẩn hóa chữ viết thường
    4. Tách từ tiếng Việt
    5. Đưa cả đoạn văn về trên 1 dòng, xóa các khoảng cách thừa
       + loại bỏ dấu chấm, dấu phẩy, dấu câu.
    """
    # Bước 0: bỏ footer/UI
    text = strip_boilerplate_lines(raw_text)

    # Bước 1: unicode NFC
    text = normalize_unicode(text)

    # Bước 2: chuẩn hóa cách gõ dấu (hiện chưa sửa gì thêm)
    text = normalize_vietnamese_diacritics(text)

    # Bước 3: chữ thường
    text = normalize_case(text)

    # Bước 4 + 5: tách từ, đưa về 1 dòng (đã bỏ dấu câu)
    text = clean_and_tokenize(text)

    return text
