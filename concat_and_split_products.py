import pandas as pd
import os
from glob import glob

# Config
INPUT_FOLDER = "input_csvs"
OUTPUT_OVER = "over_29.csv"
OUTPUT_UNDER = "under_29.csv"
MAX_LEN = 29
USE_COLUMNS = ['Product_code', 'Product_description', 'Product_description2']

# Padding to 29 characters
def pad_to_29(text):
    return str(text).ljust(MAX_LEN)[:MAX_LEN]

# Safe reader that reads only needed columns
def try_read_csv(file):
    try:
        df = pd.read_csv(file, encoding='utf-8', sep=';')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(file, encoding='latin1', sep=';')
        except Exception as e:
            print(f"[ERROR] Could not read {file} with latin1: {e}")
            return None
    except pd.errors.ParserError as e:
        print(f"[ERROR] Parsing issue in {file}: {e}")
        return None

    df.columns = df.columns.str.strip()  # Remove any spaces around headers
    if all(col in df.columns for col in USE_COLUMNS):
        return df[USE_COLUMNS]
    else:
        print(f"[WARNING] Skipping {file}: missing required columns. Found: {list(df.columns)}")
        return None



# Load valid files
all_files = glob(os.path.join(INPUT_FOLDER, "*.csv"))
frames = []
for file in all_files:
    df = try_read_csv(file)
    if df is not None:
        frames.append(df)
    else:
        print(f"[SKIPPED] {file} due to read error or missing columns.")

if not frames:
    raise RuntimeError("No valid CSV files found.")

df_all = pd.concat(frames, ignore_index=True)

# Filter and formatting WIP
mask_over = (df_all['Product_description'].astype(str).str.len() > MAX_LEN) | \
            (df_all['Product_description2'].astype(str).str.len() > MAX_LEN)

df_over = df_all[mask_over].copy()
df_under = df_all[~mask_over].copy()

df_under['Formatted'] = df_under.apply(
    lambda row: pad_to_29(row['Product_description']) + pad_to_29(row['Product_description2']),
    axis=1
)

# Save results
df_over.to_csv(OUTPUT_OVER, index=False)
df_under.to_csv(OUTPUT_UNDER, index=False)

print(f"Done! {len(df_under)} rows → '{OUTPUT_UNDER}', {len(df_over)} rows → '{OUTPUT_OVER}'")
