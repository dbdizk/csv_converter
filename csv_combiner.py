import pandas as pd
import os
from glob import glob

INPUT_FOLDER = "csvs_to_combine"
OUTPUT_FILE = "combined_output.csv"
ENCODING = "utf-8-sig"
DELIMITER = ";"

# Gather all CSV files in the folder
csv_files = glob(os.path.join(INPUT_FOLDER, "*.csv"))

# Read and combine all into one DataFrame
frames = []
for file in csv_files:
    try:
        df = pd.read_csv(file, sep=DELIMITER, encoding=ENCODING)

        if 'Product_code' not in df.columns:
            print(f"[!] Skipped: {os.path.basename(file)} — Missing 'Product_code' column")
            continue

        # Clean and filter Product_code
        df['Product_code'] = df['Product_code'].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

        # Drop rows with missing, empty, or "nan" codes
        df = df[~df['Product_code'].str.lower().isin(["", "nan"])]

        frames.append(df)
        print(f"[+] Included: {os.path.basename(file)} with {len(df)} valid rows")

    except Exception as e:
        print(f"[!] Skipped: {os.path.basename(file)} — {e}")


# Concatenate all frames if any were found
if not frames:
    raise RuntimeError("No valid CSV files found in folder.")

combined_df = pd.concat(frames, ignore_index=True)

# Save the combined file
combined_df.to_csv(OUTPUT_FILE, index=False, sep=DELIMITER, encoding=ENCODING)
print(f"\n Combined {len(csv_files)} files into '{OUTPUT_FILE}'")
