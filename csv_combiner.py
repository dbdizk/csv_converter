import pandas as pd
import os
from glob import glob
import chardet

INPUT_FOLDER = "csvs_to_combine"
OUTPUT_FILE = "combined_output.csv"
DELIMITER = ";"

# Encodings to try in fallback order
PREFERRED_ENCODINGS = ["utf-8-sig", "ISO-8859-1", "cp1252", "windows-1252", "EUC-KR"]

def detect_encoding(file_path, n_bytes=10000):
    """Detect encoding using chardet on the first chunk of the file."""
    with open(file_path, "rb") as f:
        raw_data = f.read(n_bytes)
    result = chardet.detect(raw_data)
    return result['encoding'] if result['encoding'] else 'utf-8-sig'

# Gather all CSV files in the folder
csv_files = glob(os.path.join(INPUT_FOLDER, "*.csv"))

# Read and combine all into one DataFrame
frames = []

for file in csv_files:
    encoding_guess = detect_encoding(file)
    encodings_to_try = [encoding_guess] + [e for e in PREFERRED_ENCODINGS if e != encoding_guess]

    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file, sep=DELIMITER, encoding=encoding)
            used_encoding = encoding
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"[!] Skipped: {os.path.basename(file)} — {e}")
            df = None
            break
    else:
        print(f"[!] Skipped: {os.path.basename(file)} — could not decode with common encodings")
        continue

    if df is None:
        continue

    if 'Product_code' not in df.columns:
        print(f"[!] Skipped: {os.path.basename(file)} — Missing 'Product_code' column")
        continue

    # Clean and filter Product_code
    df['Product_code'] = (
        df['Product_code']
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )

    # Drop rows with missing, empty, or "nan" codes
    df = df[~df['Product_code'].str.lower().isin(["", "nan"])]

    frames.append(df)
    print(f"[+] Included: {os.path.basename(file)} with encoding '{used_encoding}' and {len(df)} valid rows")

# Concatenate all frames if any were found
if not frames:
    raise RuntimeError("No valid CSV files found in folder.")

combined_df = pd.concat(frames, ignore_index=True)

# Save the combined file
combined_df.to_csv(OUTPUT_FILE, index=False, sep=DELIMITER, encoding="utf-8-sig")
print(f"\n🌟 Combined {len(frames)} files into '{OUTPUT_FILE}' with {len(combined_df)} total rows.")
