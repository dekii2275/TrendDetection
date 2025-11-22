import sys
import json
import pandas as pd
import google.generativeai as genai
from bertopic import BERTopic
from bertopic.representation import BaseRepresentation
from bertopic.representation._utils import truncate_document
from typing import Mapping, List, Tuple
from sentence_transformers import SentenceTransformer
from scipy.sparse import csr_matrix


GOOGLE_API_KEY = "AIzaSyDr4lo9fMpkgBbl0a8rj7dFDFDeQXdIwks"


# ============================================================
# 1) GEMINI BACKEND
# ============================================================
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

        print(f"\n--- GỌI GEMINI ({len(topics)} topic) ---")

        repr_docs_mappings, _, _, _ = topic_model._extract_representative_docs(
            c_tf_idf, documents, topics,
            500, self.nr_docs, self.diversity
        )

        updated_topics = {}

        for topic_id, docs in repr_docs_mappings.items():
            if topic_id == -1:
                continue

            topic_words = topics[topic_id]
            keywords_text = ", ".join([w for w, _ in topic_words][:10])

            truncated_docs = [
                truncate_document(topic_model, self.doc_length, self.tokenizer, doc)
                for doc in docs
            ]
            docs_text = "\n".join(f"- {d}" for d in truncated_docs)

            final_prompt = (
                self.prompt
                .replace("[KEYWORDS]", keywords_text)
                .replace("[DOCUMENTS]", docs_text)
            )

            # Gọi Gemini
            label = "Không rõ"
            try:
                res = self.model.generate_content(final_prompt)
                if getattr(res, "text", None):
                    label = res.text.strip().replace("\n", " ")
                print("  → Gemini đặt tên:", label)
            except Exception as e:
                print("  ❌ Lỗi Gemini:", e)

            updated_topics[topic_id] = [(label, 1.0)] + topic_words

        return updated_topics


# ============================================================
# 2) PIPELINE CHÍNH
# ============================================================
def run_pipeline(docs: list[str], topic_name: str):

    print(f"\n🔥 CHẠY PIPELINE CHO TOPIC: {topic_name}")
    print(f" → Số bài báo: {len(docs)}")

    # =====================================================
    #  SKIP TỪ TRONG generative_topic.py
    # =====================================================
    if len(docs) <= 5:
        print(f"❌ BỎ QUA TOPIC '{topic_name}' — QUÁ ÍT BÀI ({len(docs)} / 5)")
        return None

    # Load embedding model
    try:
        embedding_model = SentenceTransformer("VoVanPhuc/sup-SimCSE-VietNamese-phobert-base")
    except:
        print("⚠ Không tải được model tiếng Việt → fallback sang all-MiniLM-L6-v2")
        embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

    prompt = """
    Bạn là biên tập viên báo tại Việt Nam.

    Nhiệm vụ: ĐẶT MỘT TIÊU ĐỀ XU HƯỚNG cho nhóm bài báo dưới đây.

    YÊU CẦU:
    - Tiếng Việt, có dấu.
    - Ngắn gọn (3–7 từ).
    - Dạng danh từ.
    - Không dùng markdown hoặc ngoặc kép.

    Từ khóa: [KEYWORDS]
    Một vài đoạn tin tiêu biểu:
    [DOCUMENTS]

    Chỉ in RA DUY NHẤT tiêu đề xu hướng:
    """

    backend = GeminiBackend(
        client=GOOGLE_API_KEY,
        model_name="gemini-2.5-flash",
        prompt=prompt
    )

    # UMAP sẽ lỗi nếu docs quá ít → tắt giảm chiều
    model = BERTopic(
        embedding_model=embedding_model,
        representation_model=backend,
        umap_model=None,       # xử lý ít sample rất an toàn
        hdbscan_model=None,    # dùng KMeans thay cho HDBSCAN để không lỗi
        min_topic_size=2
    )

    topics, _ = model.fit_transform(docs)

    info = model.get_topic_info()
    main_topic = info.iloc[1]   # hàng 0 = outlier -1

    trend_title = main_topic["Name"]

    print(f"\n🎯 KẾT QUẢ CUỐI — TOPIC: {topic_name}")
    print(" → Tiêu đề xu hướng:", trend_title)

    return trend_title


# ============================================================
# 3) MAIN — NHẬN INPUT TỪ SPARK
# ============================================================
if __name__ == "__main__":
    docs = json.loads(sys.argv[1])
    topic_name = sys.argv[2]
    run_pipeline(docs, topic_name)
