import pandas as pd
import os
from glob import glob
from rapidfuzz import fuzz
from sentence_transformers import SentenceTransformer, util
from deep_translator import GoogleTranslator
import time


# Config
INPUT_FOLDER = "input_csvs"
OUTPUT_OVER = "over_29.csv"
OUTPUT_UNDER = "under_29.csv"
OUTPUT_TOTAL_UNDER = "total_under_29.csv"
MAX_LEN = 29
ENCODING = "utf-8-sig"
DELIMITER = ";"

USE_COLUMNS = [
    'Product_code', 'Product_description', 'Product_description2',
    'Product_group_code', 'Supplier_origin', 'Supplier_note',
    'Product_unit', 'Product_unit_amount', 'Product_unit_default_amount',
    'Product_unit_use_price_bit', 'Product_unit_use_stock_bit', 'Product_unit_use_sales_bit', 'Product_unit_use_purchase_bit',
    'Sales_account', 'Purchase_account',
    'Translate_language_code', 'Translate_product_description', 'Translate_product_description2',
    'Supplier_unit'
]

model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

# Padding

def pad_to_29(text):
    return str(text).ljust(MAX_LEN)[:MAX_LEN]

# CSV Reader

def try_read_csv(file):
    try:
        df = pd.read_csv(file, encoding='utf-8', sep=DELIMITER)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(file, encoding='latin1', sep=DELIMITER)
        except Exception as e:
            print(f"[ERROR] Could not read {file}: {e}")
            return None
    except pd.errors.ParserError as e:
        print(f"[ERROR] Parsing issue in {file}: {e}")
        return None

    df.columns = df.columns.str.strip()
    missing_cols = [col for col in USE_COLUMNS if col not in df.columns]
    for col in missing_cols:
        df[col] = pd.NA

    if all(col in df.columns for col in USE_COLUMNS):
        return df[USE_COLUMNS]
    else:
        print(f"[WARNING] Skipping {file}: missing required columns.")
        return None

# Load files

all_files = glob(os.path.join(INPUT_FOLDER, "*.csv"))
frames = [try_read_csv(file) for file in all_files]
frames = [df for df in frames if df is not None and not df.empty]

if not frames:
    raise RuntimeError("No valid CSV files found.")

# Combine

df_all = pd.concat(frames, ignore_index=True)

# Duplicate Handling

def are_fuzzy_similar(a, b, threshold=80):
    if not a or not b:
        return False
    return fuzz.partial_ratio(str(a).lower(), str(b).lower()) >= threshold


def handle_duplicates(df):
    dupes = df[df.duplicated(subset=['Product_code'], keep=False)]

    if dupes.empty:
        return df

    groups = dupes.groupby('Product_code')
    rows_to_keep = []

    for code, group in groups:
        if len(group) == 2:
            row1, row2 = group.iloc[0], group.iloc[1]

            desc1, desc2 = str(row1['Product_description']), str(row2['Product_description'])
            trans1, trans2 = str(row1['Translate_product_description']), str(row2['Translate_product_description'])

            # Prefer if description matches translation
            if are_fuzzy_similar(desc1, trans2) or are_fuzzy_similar(desc2, trans1):
                rows_to_keep.append(row1.name)  # Keep first
                print(f"[AUTO] Duplicate {code} auto-handled by translation match.")
                continue

            # If descriptions are very close
            if are_fuzzy_similar(desc1, desc2):
                rows_to_keep.append(row1.name)  # Keep first
                print(f"[AUTO] Duplicate {code} auto-handled by description match.")
                continue

        # Manual decision
        print(f"\n[MANUAL] Duplicate for Product_code: {code}")
        for idx, (_, row) in enumerate(group.iterrows(), start=1):
            print(f"{idx}) Product_description: {row['Product_description']}, Product_description2: {row['Product_description2']}, Product_unit: {row['Product_unit']}")


        valid = False
        while not valid:
            choice = input("Which row to keep? (number): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(group):
                selected_idx = group.iloc[int(choice)-1].name
                rows_to_keep.append(selected_idx)
                valid = True
            else:
                print("Invalid choice. Try again.")

    kept_dupes = df.loc[rows_to_keep].copy()
    non_dupes = df[~df.index.isin(dupes.index)]
    return pd.concat([non_dupes, kept_dupes], ignore_index=True)

# Dedupe First!

df_all = handle_duplicates(df_all)

# Translation Phase

def translate_missing(row):
    if pd.isna(row['Translate_product_description']) or not str(row['Translate_product_description']).strip():
        text = str(row['Product_description'])
        if len(text.strip()) > 2:
            try:
                start_time = time.time()
                translated = GoogleTranslator(source='fi', target='en').translate(text)
                elapsed = time.time() - start_time
                
                if elapsed > 5:  # If it took too long (over 5 seconds)
                    print(f"[WARNING] Translation too slow for text: '{text[:30]}...' (skipped)")
                    return row

                row['Translate_product_description'] = translated

            except Exception as e:
                print(f"[WARNING] Translation failed for text '{text[:30]}...': {e}")
                # Simply leave the row unchanged
    return row

df_all = df_all.apply(translate_missing, axis=1)

# Split over/under

mask_over = (df_all['Product_description'].astype(str).str.len() > MAX_LEN) | \
            (df_all['Product_description2'].astype(str).str.len() > MAX_LEN)

mask_total_under = (
    df_all['Product_description'].astype(str).str.len() +
    df_all['Product_description2'].astype(str).str.len()
) < MAX_LEN

mask_under = ~mask_over & ~mask_total_under


# Prepare DataFrames

df_over = df_all[mask_over].copy()
df_under = df_all[mask_under].copy()
df_total_under = df_all[mask_total_under].copy()


df_under['Product_description'] = df_under.apply(
    lambda row: pad_to_29(row['Product_description']) + pad_to_29(row['Product_description2']), axis=1)

df_total_under['Product_description'] = df_total_under.apply(
    lambda row: (str(row['Product_description']) + ' ' + str(row['Product_description2'])).ljust(MAX_LEN), axis=1)

# Only keep output columns
df_under = df_under[USE_COLUMNS]
df_over = df_over[USE_COLUMNS]
df_total_under = df_total_under[USE_COLUMNS]

# Save

df_over.to_csv(OUTPUT_OVER, index=False, sep=DELIMITER, encoding=ENCODING)
df_under.to_csv(OUTPUT_UNDER, index=False, sep=DELIMITER, encoding=ENCODING)
df_total_under.to_csv(OUTPUT_TOTAL_UNDER, index=False, sep=DELIMITER, encoding=ENCODING)

print(f"\n✅ Done! {len(df_total_under)} rows → '{OUTPUT_TOTAL_UNDER}', {len(df_under)} rows → '{OUTPUT_UNDER}', {len(df_over)} rows → '{OUTPUT_OVER}'")
