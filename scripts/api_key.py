import google.generativeai as genai

# ĐIỀN KEY CỦA BẠN VÀO ĐÂY
GOOGLE_API_KEY = "AIzaSyCwV0bYTBiFyRawjexIzy3PdLmgUeQ0Gkg"

genai.configure(api_key=GOOGLE_API_KEY)

print("Đang lấy danh sách model được hỗ trợ...")
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"- Tên model: {m.name}")
except Exception as e:
    print(f"Lỗi: {e}")