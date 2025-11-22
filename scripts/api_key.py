import google.generativeai as genai

# ĐIỀN KEY CỦA BẠN VÀO ĐÂY
GOOGLE_API_KEY = "AIzaSyDr4lo9fMpkgBbl0a8rj7dFDFDeQXdIwks"

genai.configure(api_key=GOOGLE_API_KEY)

print("Đang lấy danh sách model được hỗ trợ...")
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"- Tên model: {m.name}")
except Exception as e:
    print(f"Lỗi: {e}")