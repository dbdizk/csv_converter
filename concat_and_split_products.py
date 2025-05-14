import pandas as pd
import os
from glob import glob

# Config
INPUT_FOLDER = "input_csvs"
OUTPUT_OVER = "over_29.csv"
OUTPUT_UNDER = "under_29.csv"
OUTPUT_TOTAL_UNDER = "total_under_29.csv"
MAX_LEN = 29
ENCODING = "utf-8-sig"
DELIMITER = ";"

# Fields kept in the output
USE_COLUMNS = [
    'Product_code', 'Product_description', 'Product_description2',
    'Product_group_code', 'Supplier_origin', 'Supplier_note',
    'Product_unit', 'Product_unit_amount', 'Product_unit_default_amount',
    'Product_unit_use_price_bit', 'Product_unit_use_stock_bit', 'Product_unit_use_sales_bit', 'Product_unit_use_purchase_bit',
    'Sales_account', 'Purchase_account',
    'Translate_language_code', 'Translate_product_description', 'Translate_product_description2',
    'Supplier_unit'
]

# Padding to 29 characters
def pad_to_29(text):
    return str(text).ljust(MAX_LEN)[:MAX_LEN]

# Safe reader that reads only needed columns
def try_read_csv(file):
    try:
        df = pd.read_csv(file, encoding='utf-8', sep=DELIMITER)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(file, encoding='latin1', sep=DELIMITER)
        except Exception as e:
            print(f"[ERROR] Could not read {file} with latin1: {e}")
            return None
    except pd.errors.ParserError as e:
        print(f"[ERROR] Parsing issue in {file}: {e}")
        return None

    df.columns = df.columns.str.strip()  # Remove any spaces around headers

    # Keep only needed columns, add missing ones as empty
    missing_cols = [col for col in USE_COLUMNS if col not in df.columns]
    for col in missing_cols:
        df[col] = pd.NA

    if all(col in df.columns for col in USE_COLUMNS):
        return df[USE_COLUMNS]
    else:
        print(f"[WARNING] Skipping {file}: unable to guarantee required columns.")
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

# Remove completely empty DataFrames
frames = [df for df in frames if not df.empty and not df.isna().all(axis=1).all()]

if not frames:
    raise RuntimeError("No valid CSV files found.")

df_all = pd.concat(frames, ignore_index=True)

def handle_duplicates(df):
    # Check for duplicates by Product_code
    dupes = df[df.duplicated(subset=['Product_code'], keep=False)]

    if dupes.empty:
        return df  # Nothing to do

    grouped = dupes.groupby('Product_code')
    rows_to_keep = []

    for code, group in grouped:
        print(f"\nFound duplicate Product_code: {code}")
        for idx, (_, row) in enumerate(group.iterrows(), start=1):
            print(f"{idx}) Product_description: {row['Product_description']}, Product_description2: {row['Product_description2']}")

        # Ask user which rows to keep until valid input
        valid = False
        while not valid:
            choice = input("Which one(s) to keep? (enter number(s) separated by commas, or 'a' for all): ").strip()
            
            if choice.lower() == 'a':
                rows_to_keep.extend(group.index.tolist())
                valid = True
            else:
                try:
                    selected = [int(x.strip()) for x in choice.split(',')]
                    if all(1 <= x <= len(group) for x in selected):
                        selected_indices = group.iloc[[i-1 for i in selected]].index.tolist()
                        rows_to_keep.extend(selected_indices)
                        valid = True
                    else:
                        print(f"[INPUT ERROR] Please choose numbers between 1 and {len(group)}.")
                except ValueError:
                    print("[INPUT ERROR] Please enter valid numbers separated by commas, or 'a'.")

    # Build new DataFrame with only selected rows
    kept_df = df.loc[rows_to_keep].copy()
    return kept_df


# Add this after combining all frames
df_all = handle_duplicates(df_all)


# Filter and formatting WIP
mask_over = (df_all['Product_description'].astype(str).str.len() > MAX_LEN) | \
            (df_all['Product_description2'].astype(str).str.len() > MAX_LEN)

mask_total_under = (
    df_all['Product_description'].astype(str).str.len() +
    df_all['Product_description2'].astype(str).str.len()
) < MAX_LEN

mask_under = ~mask_over & ~mask_total_under

df_over = df_all[mask_over].copy()
df_under = df_all[mask_under].copy()
df_total_under = df_all[mask_total_under].copy()

# Format the under-29 DataFrame
df_under['Product_description'] = df_under.apply(
    lambda row: pad_to_29(row['Product_description']) + pad_to_29(row['Product_description2']),
    axis=1
)

df_total_under['Product_description'] = df_total_under.apply(
    lambda row: (str(row['Product_description']) + ' ' + str(row['Product_description2'])).ljust(MAX_LEN),
    axis=1
)

# Keep only the required columns
df_under = df_under[USE_COLUMNS]
df_over = df_over[USE_COLUMNS]
df_total_under = df_total_under[USE_COLUMNS]

# Save results
df_over.to_csv(OUTPUT_OVER, index=False, sep=DELIMITER, encoding=ENCODING)
df_under.to_csv(OUTPUT_UNDER, index=False, sep=DELIMITER, encoding=ENCODING)
df_total_under.to_csv(OUTPUT_TOTAL_UNDER, index=False, sep=DELIMITER, encoding=ENCODING)

print(f"Done! {len(df_total_under)} rows → '{OUTPUT_TOTAL_UNDER}', {len(df_under)} rows → '{OUTPUT_UNDER}', {len(df_over)} rows → '{OUTPUT_OVER}'")
