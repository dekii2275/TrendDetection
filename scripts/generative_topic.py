import os
import google.generativeai as genai
from bertopic import BERTopic
from bertopic.representation import BaseRepresentation
from sentence_transformers import SentenceTransformer
import pandas as pd
import time

# ==========================================
# CẤU HÌNH API
# ==========================================
GOOGLE_API_KEY = "AIzaSyCwV0bYTBiFyRawjexIzy3PdLmgUeQ0Gkg"

# ==========================================
# 1. CLASS GEMINI BACKEND (PHIÊN BẢN FIX LỖI KEYERROR)
# ==========================================
import time
import google.generativeai as genai
from bertopic.representation import BaseRepresentation
from bertopic.representation._utils import truncate_document  # dùng cho cắt bớt text

from typing import Mapping, List, Tuple
from scipy.sparse import csr_matrix
import pandas as pd


class GeminiBackend(BaseRepresentation):
    def __init__(
        self,
        client: str,
        model_name: str,
        prompt: str,
        nr_docs: int = 4,
        diversity: float | None = None,
        doc_length: int | None = 200,
        tokenizer: str | None = "whitespace",
        **kwargs,
    ):
        """
        client: chính là API_KEY chuỗi string
        model_name: tên model Gemini (vd: 'gemini-1.5-flash')
        prompt: prompt có chứa [KEYWORDS] và [DOCUMENTS]
        """
        super().__init__(**kwargs)
        self.api_key = client
        self.model_name = model_name
        self.prompt = prompt
        self.nr_docs = nr_docs
        self.diversity = diversity
        self.doc_length = doc_length
        self.tokenizer = tokenizer

        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)

    def extract_topics(
        self,
        topic_model,
        documents: pd.DataFrame,
        c_tf_idf: csr_matrix,
        topics: Mapping[int, List[Tuple[str, float]]],
    ) -> Mapping[int, List[Tuple[str, float]]]:
        """
        Hàm này được BERTopic tự động gọi để fine-tune representation.
        Phải trả về dict: {topic_id: [(text, score), ...]}
        """
        print(f"\n--- BẮT ĐẦU GỌI GEMINI (Tìm thấy {len(topics)} nhóm) ---")

        # 1. Lấy document đại diện cho từng topic (chuẩn BERTopic)
        repr_docs_mappings, _, _, _ = topic_model._extract_representative_docs(
            c_tf_idf,
            documents,
            topics,
            500,              # max doc length để chọn representative docs
            self.nr_docs,     # số doc đại diện
            self.diversity,   # độ đa dạng
        )

        updated_topics: dict[int, List[Tuple[str, float]]] = {}

        # 2. Lặp qua từng topic và gọi Gemini
        for topic_id, docs in repr_docs_mappings.items():
            if topic_id == -1:
                # -1 là topic rác → bỏ qua
                continue

            print(f"   > Đang xử lý Topic {topic_id}...")

            # 2.1 Lấy keywords từ topics (đã được BERTopic tính sẵn)
            topic_words = topics[topic_id]  # list[(word, score)]
            keywords = [w for w, _ in topic_words]
            keywords_text = ", ".join(keywords[:10])

            # 2.2 Chuẩn bị docs text (cắt bớt cho gọn)
            truncated_docs = [
                truncate_document(topic_model, self.doc_length, self.tokenizer, doc)
                for doc in docs
            ]
            docs_text = "\n".join(f"- {d}" for d in truncated_docs)

            # 2.3 Thay [KEYWORDS] & [DOCUMENTS] trong prompt
            current_prompt = self.prompt.replace("[KEYWORDS]", keywords_text)
            current_prompt = current_prompt.replace("[DOCUMENTS]", docs_text)

            # 2.4 Gọi Gemini
            label = "Lỗi API"
            try:
                time.sleep(1)  # tránh spam API quá nhanh
                response = self.model.generate_content(current_prompt)

                if getattr(response, "text", None):
                    label = (
                        response.text.strip()
                        .replace('"', "")
                        .replace("'", "")
                        .replace("\n", " ")
                    )
                    print(f"     ✅ Gemini đặt tên: {label}")
                else:
                    print("     ⚠️ Gemini trả về rỗng.")
            except Exception as e:
                print(f"     ❌ Lỗi kết nối API: {e}")

            # 2.5 Trả về representation đúng format:
            #     phần tử đầu tiên là label, sau đó đến các keyword cũ.
            #     Mỗi phần tử là tuple (text, score)
            new_repr: List[Tuple[str, float]] = [(label, 1.0)] + topic_words

            # Cho chắc ăn thì padding cho đủ 10 phần tử (giống các model khác)
            if len(new_repr) < 10:
                new_repr += [("", 0.0)] * (10 - len(new_repr))

            updated_topics[topic_id] = new_repr

        return updated_topics

# ==========================================
# 2. DỮ LIỆU & MAIN (GIỮ NGUYÊN)
# ==========================================
docs = [
    "Giá vàng SJC hôm nay tăng vọt vượt mốc 80 triệu đồng.",
    "Gia vàng thế giới leo thang do lo ngại lạm phát toàn cầu.",
    "Thị trường vàng trong nước biến động mạnh theo đà thế giới.",
    "Người dân xếp hàng dài chờ mua vàng nhẫn tròn trơn.",
    "Tỷ giá đô la Mỹ ngân hàng tăng nhẹ, giá vàng quay đầu giảm.",
    "Chuyên gia dự báo giá vàng sẽ còn phá đỉnh vào cuối năm.",
    "Trí tuệ nhân tạo AI đang thay đổi cách lập trình viên làm việc.",
    "ChatGPT và Gemini cạnh tranh khốc liệt trong mảng AI tạo sinh.",
    "Nvidia trở thành công ty giá trị nhất nhờ cơn sốt chip AI.",
    "Ứng dụng AI vào y tế giúp chẩn đoán bệnh ung thư sớm.",
    "Học máy và Deep Learning là nền tảng cốt lõi của AI hiện đại."
]

def run_full_pipeline_test():
    print("\n--- [TEST 2] CHẠY PIPELINE (Debug Mode) ---")
    
    # Tải model Embedding
    print("1. Đang tải model Embedding...")
    embedding_model = SentenceTransformer("VoVanPhuc/sup-SimCSE-VietNamese-phobert-base")
    
    # Cấu hình Prompt
    prompt = """
    Bạn là biên tập viên báo kinh tế – công nghệ tại Việt Nam.

    Nhiệm vụ: ĐẶT MỘT TIÊU ĐỀ XU HƯỚNG cho nhóm bài báo dưới đây.

    YÊU CẦU:
    - Viết hoàn toàn bằng tiếng Việt có dấu.
    - Ngắn gọn, tự nhiên, giống tiêu đề mục tin trên báo (3–7 từ).
    - Ưu tiên cụm danh từ: ví dụ “Biến động giá vàng trong nước”, 
    “Bùng nổ trí tuệ nhân tạo”, “Ứng dụng AI trong y tế”.
    - KHÔNG dùng markdown (**, #, `), KHÔNG dùng ngoặc kép.
    - Hạn chế ghi số cụ thể (như 80, 90, 100…), chỉ dùng số khi thật sự cần thiết.
    - Không nhắc lại các từ “xu hướng”, “nhóm tin”, “bài báo”.

    Từ khóa: [KEYWORDS]

    Một vài đoạn tin tiêu biểu:
    [DOCUMENTS]

    Chỉ in RA DUY NHẤT tiêu đề xu hướng:
    """
    
    # Khởi tạo Backend
    print("2. Đang khởi tạo Gemini Backend...")
    representation_model = GeminiBackend(
        client=GOOGLE_API_KEY,
        model_name='gemini-2.5-flash',
        prompt=prompt
    )

    # Chạy BERTopic
    print("3. Đang training mô hình...")
    topic_model = BERTopic(
        embedding_model=embedding_model,
        representation_model=representation_model,
        min_topic_size=2, # Quan trọng cho tập dữ liệu nhỏ
        verbose=True
    )

    topics, probs = topic_model.fit_transform(docs)
    
    print("\n" + "="*40)
    print("KẾT QUẢ CUỐI CÙNG")
    print("="*40)
    
    topic_info = topic_model.get_topic_info()
    
    # Hiển thị cột Name (Tên do Gemini đặt) và Representation (Từ khóa gốc)
    if 'Name' in topic_info.columns:
        print(topic_info[['Topic', 'Count', 'Name']])
    else:
        print(topic_info)

if __name__ == "__main__":
    if "DIEN_API_KEY" in GOOGLE_API_KEY:
        print("❌ LỖI: Bạn chưa điền API Key vào code.")
    else:
        run_full_pipeline_test()