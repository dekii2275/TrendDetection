import py_vncorenlp
import os

# 1. Define a real path inside your current project
# This creates a folder named 'vncorenlp' inside your TrendDetection folder
save_dir = os.path.join(os.getcwd(), 'vncorenlp')

# 2. Create the directory if it doesn't exist (to avoid FileNotFoundError)
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# 3. Download the model (only if not already downloaded)
# Note: You might want to wrap this in a try/except block or check if files exist
# because calling download_model repeatedly might raise an error if folders exist.
if not os.path.exists(os.path.join(save_dir, 'models')):
    py_vncorenlp.download_model(save_dir=save_dir)

# 4. Load VnCoreNLP
model = py_vncorenlp.VnCoreNLP(save_dir=save_dir)

# 5. Annotate text
text = "Ông Nguyễn Khắc Chúc đang làm việc tại Đại học Quốc gia Hà Nội. Bà Lan, vợ ông Chúc, cũng làm việc tại đây."
output = model.annotate_text(text)

# Print the results
model.print_out(output)