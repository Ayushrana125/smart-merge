from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import tempfile
import os

app = Flask(__name__)
CORS(app)  # ✅ Allow frontend (different port/origin) to talk to backend

# --- SMART MERGER ROUTES --- #

@app.route("/merge", methods=["POST"])
def merge_files():
    files = request.files.getlist("files")
    if not files:
        return {"error": "No files uploaded"}, 400

    dfs = []
    seen_extra_cols = []
    base_order = None

    for i, f in enumerate(files):
        try:
            # Try Excel first
            df = pd.read_excel(f, engine="openpyxl")
        except Exception:
            # Reset pointer and try CSV
            f.seek(0)
            chunks = []
            for chunk in pd.read_csv(f, chunksize=100000):  # stream in 100k rows
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)

        dfs.append(df)

        # Base order = first file headers
        if i == 0:
            base_order = list(df.columns)

        # Track new extra columns
        for col in df.columns:
            if col not in base_order and col not in seen_extra_cols:
                seen_extra_cols.append(col)

    # Final column order = base headers first, then extras
    final_cols = list(base_order)
    final_cols.extend([c for c in seen_extra_cols if c not in final_cols])

    # Align all DataFrames
    aligned = [df.reindex(columns=final_cols) for df in dfs]

    # Merge safely
    merged_df = pd.concat(aligned, ignore_index=True, sort=False)
    merged_df = merged_df.fillna("")  # blanks instead of NaN

    # Save to temp Excel file for user
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp_file.close()
    merged_df.to_excel(temp_file.name, index=False)

    return send_file(temp_file.name, as_attachment=True, download_name="merged.xlsx")



# --- SMART FORMAT CHANGER ROUTES --- #

@app.route("/generate-hm", methods=["POST"])
def generate_hm():
    """
    Generate a Header Matching file (HM).
    Columns:
    - Base Header (from base file)
    - Matched Input Header (if present in input file)
    - Unmatched Input Headers (extra input columns not in base, one per row)
    """
    try:
        input_file = request.files.get("input_file")
        base_file = request.files.get("base_file")

        if not input_file or not base_file:
            return jsonify({"error": "Both Input File and Base Structure File are required"}), 400

        # Read both files
        input_df = pd.read_excel(input_file)
        base_df = pd.read_excel(base_file)

        base_headers = base_df.columns.tolist()
        input_headers = input_df.columns.tolist()

        # Direct matches
        matched = [col if col in input_headers else "" for col in base_headers]

        # Unmatched headers = input headers not in base
        unmatched_headers = [col for col in input_headers if col not in base_headers]

        # Row-wise alignment
        max_len = max(len(base_headers), len(unmatched_headers))
        base_extended = base_headers + [""] * (max_len - len(base_headers))
        matched_extended = matched + [""] * (max_len - len(matched))
        unmatched_extended = unmatched_headers + [""] * (max_len - len(unmatched_headers))

        hm_df = pd.DataFrame({
            "Base Header": base_extended,
            "Matched Input Header": matched_extended,
            "Unmatched Input Headers": unmatched_extended
        })

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        hm_df.to_excel(temp_file.name, index=False)

        return send_file(temp_file.name, as_attachment=True, download_name="Header_Matching_File.xlsx")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/transform", methods=["POST"])
def transform_file():
    """
    Transform Input File into Base Structure using Header Matching File.
    - If Matched Input Header matches a real column → map values.
    - If Matched Input Header is in quotes ("...") → use it as a static value.
    - If Matched Input Header is blank → keep column empty.
    """
    try:
        input_file = request.files.get("input_file")
        base_file = request.files.get("base_file")
        hm_file = request.files.get("hm_file")

        if not input_file or not base_file or not hm_file:
            return jsonify({"error": "Input, Base, and HM files are all required"}), 400

        input_df = pd.read_excel(input_file)
        base_df = pd.read_excel(base_file)
        hm_df = pd.read_excel(hm_file)

        base_headers = base_df.columns.tolist()
        transformed_df = pd.DataFrame()

        for _, row in hm_df.iterrows():
            base_col = str(row.get("Base Header", "")).strip()
            matched_col = str(row.get("Matched Input Header", "")).strip()

            if not base_col:  # skip empty rows
                continue

            if matched_col in input_df.columns:
                transformed_df[base_col] = input_df[matched_col]

            elif matched_col.startswith('"') and matched_col.endswith('"'):
                static_value = matched_col.strip('"')
                transformed_df[base_col] = [static_value] * len(input_df)

            elif matched_col == "" or matched_col.lower() == "nan":
                transformed_df[base_col] = [""] * len(input_df)

            else:
                transformed_df[base_col] = [""] * len(input_df)

        # Ensure all base headers exist
        for col in base_headers:
            if col not in transformed_df.columns:
                transformed_df[col] = ""

        transformed_df = transformed_df[base_headers]

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        transformed_df.to_excel(temp_file.name, index=False)

        return send_file(temp_file.name, as_attachment=True, download_name="Transformed_File.xlsx")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
