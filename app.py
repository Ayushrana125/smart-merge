from flask import Flask, request, send_file
from flask_cors import CORS
import pandas as pd
import tempfile
import os

app = Flask(__name__)
CORS(app)  # ✅ Allow frontend (different port/origin) to talk to backend


@app.route("/merge", methods=["POST"])
def merge_files():
    files = request.files.getlist("files")
    if not files:
        return {"error": "No files uploaded"}, 400

    merged_df = pd.DataFrame()

    # Read and merge all Excel files
    for f in files:
        df = pd.read_excel(f)
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    # Save to a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp_file.close()  # close handle before pandas writes
    merged_df.to_excel(temp_file.name, index=False)

    # Send back merged file
    return send_file(temp_file.name, as_attachment=True, download_name="merged.xlsx")


@app.route("/", methods=["GET"])
def home():
    return "✅ Merge API is running!"


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
