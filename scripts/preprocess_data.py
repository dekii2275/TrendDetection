# scripts/preprocess_data.py
from pathlib import Path
import re
import unicodedata
import os

# Nếu sau này vẫn muốn fallback sang underthesea thì giữ lại import này
# còn hiện tại ta dùng VnCoreNLP là chính.
# from underthesea import word_tokenize

import py_vncorenlp

# scripts/preprocess_data.py (đặt gần đầu file)

VI_STOPWORDS = {
    # từ chức năng rất chung
    "và", "trong", "với", "của", "là", "được", "tại", "từ", "cho", "đến",
    "này", "kia", "đó", "này", "ấy", "này", "sẽ", "đã", "đang", "cũng",
    "nhưng", "hay", "hoặc", "nếu", "thì", "rằng", "vì", "do", "khi",
    "trên", "dưới", "giữa", "sau", "trước", "nơi", "nơi_đây", "nơi_này",

    # đại từ / từ rất chung trong tin tức
    "người", "ông", "bà", "anh", "chị", "họ", "chúng_ta", "chúng_tôi",
    "một", "hai", "ba", "nhiều", "ít", "các", "những", "nhiều",
    "năm", "tháng", "ngày", "hôm_nay", "hôm_qua",

    # em có thể thêm/bớt dần khi xem kết quả
}


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


# ================= VnCoreNLP word segmentation ================= #

SEGMENTER = None  # sẽ load lazy, chỉ 1 lần


def get_vncorenlp_segmenter():
    """
    Khởi tạo VnCoreNLP (chỉ 1 lần), dùng annotator wseg (word segmentation).
    Dùng luôn model đã có sẵn trong PROJECT_ROOT / 'vncorenlp'.
    """
    global SEGMENTER
    if SEGMENTER is None:
        # 🔹 CHỈNH Ở ĐÂY: dùng thư mục vncorenlp nằm TRONG project
        save_dir = PROJECT_ROOT / "vncorenlp"

        # Nếu muốn an toàn, có thể tạo thư mục (nếu em chắc chắn đã có rồi thì dòng này không bắt buộc)
        os.makedirs(save_dir, exist_ok=True)

        # Không cần download_model nữa vì em đã có jar + models
        # Nếu muốn vẫn có thể bật để tự tải khi thiếu:
        # py_vncorenlp.download_model(save_dir=str(save_dir))

        SEGMENTER = py_vncorenlp.VnCoreNLP(
            annotators=["wseg"],
            save_dir=str(save_dir),
        )
    return SEGMENTER


def word_segment_tokens(text: str) -> list[str]:
    """
    Tách từ tiếng Việt bằng VnCoreNLP.

    Trả về: list token phẳng, ví dụ:
    "Mình quê ở Tiền Giang." ->
        ["Mình", "quê", "ở", "Tiền_Giang", "."]
    (dấu câu sau đó sẽ bị PUNCT_RE xử lý)
    """
    segmenter = get_vncorenlp_segmenter()
    # VnCoreNLP trả về list các câu, mỗi câu là chuỗi có token cách nhau bởi space
    sentences = segmenter.word_segment(text)
    tokens: list[str] = []
    for sent in sentences:
        tokens.extend(sent.split())
    return tokens


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
    - Tách từ tiếng Việt bằng VnCoreNLP

    KHÔNG xóa số, KHÔNG lọc stopword.
    Mục tiêu: chỉ chuẩn hóa, không làm mất thông tin nội dung.
    """
    # Chuyển xuống dòng, tab thành space
    text = re.sub(r"[\r\n\t]", " ", text)

    # Tách từ trước rồi mới xử lý dấu câu cho chắc chắn
    # (vì VnCoreNLP dùng dấu chấm để phân câu)
    tokens = word_segment_tokens(text)

    # Bỏ dấu câu khỏi từng token (nếu muốn giữ số, chữ)
    cleaned_tokens: list[str] = []
    for tok in tokens:
        # thay dấu câu trong token bằng space rồi gom lại
        tok_no_punct = PUNCT_RE.sub(" ", tok)
        # có thể sinh ra nhiều space -> tách lại
        for sub in tok_no_punct.split():
            cleaned_tokens.append(sub)

    # Gom nhiều khoảng trắng bằng cách join lại = 1 space
    return " ".join(cleaned_tokens)


# -------- Hàm chính dùng trong toàn project -------- #
def preprocess_text(raw_text: str) -> str:
    """
    Pipeline tiền xử lý hoàn chỉnh, theo 5 bước:

    0. (Thêm) Cắt bỏ footer/UI của báo (Tuổi Trẻ).
    1. Chuẩn hóa unicode
    2. Chuẩn hóa cách gõ dấu tiếng Việt
    3. Chuẩn hóa chữ viết thường
    4. Tách từ tiếng Việt (VnCoreNLP)
    5. Đưa cả đoạn văn về trên 1 dòng, xóa các khoảng cách thừa,
       loại bỏ dấu câu.
    """
    # Bước 0: bỏ footer/UI
    text = strip_boilerplate_lines(raw_text)

    # Bước 1: unicode NFC
    text = normalize_unicode(text)

    # Bước 2: chuẩn hóa cách gõ dấu (hiện chưa sửa gì thêm)
    text = normalize_vietnamese_diacritics(text)

    # Bước 3: chữ thường
    text = normalize_case(text)

    # Bước 4 + 5: tách từ + bỏ dấu câu + gom 1 dòng
    text = clean_and_tokenize(text)

    return text
