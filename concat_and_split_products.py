import pandas as pd
import os
import re
import time
from glob import glob
from rapidfuzz import fuzz
from sentence_transformers import SentenceTransformer, util
from deep_translator import GoogleTranslator

# Config
INPUT_FOLDER = "input_csvs"
OUTPUT_OVER = "over_29.csv"
OUTPUT_UNDER = "under_29.csv"
OUTPUT_TOTAL_UNDER = "total_under_29.csv"
ENCODING = "utf-8-sig"
DELIMITER = ";"
MAX_LEN = 29

USE_COLUMNS = [
    'Product_code', 'Product_description', 'Product_description2',
    'Product_group_code', 'Supplier_origin', 'Supplier_note',
    'Product_unit', 'Product_unit_amount', 'Product_unit_default_amount',
    'Product_unit_use_price_bit', 'Product_unit_use_stock_bit',
    'Product_unit_use_sales_bit', 'Product_unit_use_purchase_bit',
    'Sales_account', 'Purchase_account',
    'Translate_language_code', 'Translate_product_description', 'Translate_product_description2',
    'Supplier_unit'
]

model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

ACCEPTED_PATTERNS = [
    "AISI316", "ASTM 420", "ASTM A-536", "ASTM A106", "ASTM A213",
    "ASTM A312", "ASTM A333", "ASTM A335", "ASTM A403", "ASTM A420",
    "ASTM A790", "ASTM SA516", "DIN 28011", "DIN 28013", "DIN 317",
    "DIN 875", "DIN 934", "DIN 976B", "DIN10", "DIN11", "DIN12",
    "DIN125", "DIN345N", "DIN6916-10", "DIN831", "DIN875", "DIN975",
    "EN 10025", "EN 10028-2", "EN 10217-7", "EN 1092-1",
    "EN 1514-1", "EN-F", "EN10025", "EN10025-2", "EN10028-2",
    "EN10028-3", "EN10028-7", "EN10088-10", "EN10088-11", "EN10088-12",
    "EN10088-13", "EN10088-14", "EN10088-15", "EN10088-16", "EN10088-17",
    "EN10088-18", "EN10088-19", "EN10088-2", "EN10088-20", "EN10088-21",
    "EN10088-22", "EN10088-23", "EN10088-24", "EN10088-25", "EN10088-26",
    "EN10088-27", "EN10088-28", "EN10088-29", "EN10088-3", "EN10088-30",
    "EN10088-31", "EN10088-32", "EN10088-33", "EN10088-34", "EN10088-4",
    "EN10088-5", "EN10088-6", "EN10088-7", "EN10088-8", "EN10088-9",
    "EN10204", "EN10216-2", "EN10216-5", "EN10217-7", "EN10253-2",
    "EN10253-2A", "EN10253-2B", "EN10253-4", "EN10272", "EN10277-2",
    "EN1092-1", "EN13445", "EN1514-2", "ISO 7089", "SS 482", "SS 6",
    "SS482", "SS483", "TP304", "TP304L", "TP316", "TP316L", "TP321",
    "TP3501", "1.0254", "1.0305", "1.0405", "1.045", "1.0484", "1.0486",
    "1.0566", "1.0582", "1.4301", "1.4306", "1.4307", "1.4401", "1.4404",
    "1.4410", "1.4435", "1.4436", "1.4438", "1.4439", "1.4462", "1.4529",
    "1.4539", "1.4541", "1.4547", "1.4550", "1.4571", "1.4903", "1.5145",
    "1.5637", "1.6905", "1.7335", "1.7362", "1.7380", "1.8902", "1.8905",
    "1.8972", "API 5L", "ASTM A 105", "ASTM A 106", "ASTM A 182", "ASTM A 234",
    "ASTM A 312", "ASTM A 333", "ASTM A 350", "ASTM A 403", "ASTM A 53",
    "ASTM A 694", "ASTM A 815", "DIN 2605", "DIN 2615", "DIN 2616", "DIN 2617",
    "DIN 28011", "DIN 28013", "EN 10028-2", "EN 10028-7", "EN 10213-2",
    "EN 10216-2", "EN 10216-5", "EN 10222-2", "EN 10222-4", "EN 10222-5",
    "EN 10253-2", "EN 10253-4", "UNS S31803", "UNS S31803 (Duplex)",
    "UNS S32205", "UNS S32750", "UNS S32750 (Superduplex)", "UNS S32760",
    "P235TR1", "P235GH", "A53 Grade A", "S235JR", "S355J2", "A106 Grade A",
    "13CrMo4-4", "13CrMo4-5", "A182 Grade F11", "10CrMo9-10", "11CrMo9-10",
    "A182 Grade F22", "X10CrMoVNb9-1", "A182 Grade F91", "10Ni14", "12Ni14",
    "A333 Grade 6", "15NiCuMoNb5-6-4", "P355NL1", "P265NL", "StE 285", "P355N",
    "A694 Grade F52", "StE 420", "P420N", "A694 Grade F60", "StE 460", "P460N",
    "A694 Grade F70", "L245NB", "L290NB", "L360NB", "L415NB", "X2CrNi18-9",
    "TP304L", "X2CrNi19-11", "TP304", "X5CrNi18-10", "X6CrNiTi18-10", "TP321",
    "X6CrNiNb18-10", "TP347", "X2CrNiMo17-12-2", "TP316L", "X5CrNiMo17-12-2",
    "TP316", "X6CrNiMoTi17-12-2", "TP316Ti", "X6CrNiMoNb17-12-2", "TP316LN",
    "X1NiCrMoCu25-20-5", "904L", "X1NiCrMoCuN25-20-7", "X2CrNiMoN22-5-3",
    "X2CrNiMoN25-7-4", "P265GH", "P355GH", "P355NL2", "16Mo3", "F11", "F22",
    "F91", "F52", "F60", "F70", 
    r"[A-Z]{2,}\s?[A-Z0-9\-]+",     # generic
    r"TP\d+[A-Z]*",                 # TP316L
    r"\d+[\.,]\d+\s?[xX]\s?\d+[\.,]\d+", # 10.3 x 1.24
    r"\([\dA-Za-z]+\)"            # (5S), etc.
]

def pad_to_29(text):
    return str(text).ljust(MAX_LEN)[:MAX_LEN]

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
    for c in USE_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[USE_COLUMNS]

def are_fuzzy_similar(a, b, threshold=80):
    if not a or not b:
        return False
    return fuzz.partial_ratio(str(a).lower(), str(b).lower()) >= threshold

def handle_duplicates(df):
    dupes = df[df.duplicated(['Product_code'], keep=False)]
    if dupes.empty:
        return df
    keep = []
    for code, group in dupes.groupby('Product_code'):
        if len(group) == 2:
            r1, r2 = group.iloc[0], group.iloc[1]
            if any(are_fuzzy_similar(x, y) for x, y in [
                (r1['Product_description'], r2['Translate_product_description']),
                (r2['Product_description'], r1['Translate_product_description']),
                (r1['Product_description'], r2['Product_description'])
            ]):
                keep.append(r1.name)
                continue
        print(f"\n[MANUAL] Duplicate for Product_code: {code}")
        for i, (_, row) in enumerate(group.iterrows(), 1):
            print(f"{i}) {row['Product_description']} / {row['Product_description2']}")
        while True:
            choice = input("Which row to keep? (number): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(group):
                keep.append(group.iloc[int(choice)-1].name)
                break
    kept = df.loc[keep]
    return pd.concat([df.drop(dupes.index), kept], ignore_index=True)

def translate_missing(row):
    if pd.isna(row['Translate_product_description']) or not str(row['Translate_product_description']).strip():
        txt = str(row['Product_description']).strip()
        if len(txt) > 2:
            try:
                start = time.time()
                tr = GoogleTranslator(source='fi', target='en').translate(txt)
                if time.time() - start <= 5:
                    row['Translate_product_description'] = tr
            except:
                pass
    return row

def split_over29_rows(df_over):
    def split_to_two_parts(text):
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) <= MAX_LEN:
            return (text, "")

        matches = []
        for patt in ACCEPTED_PATTERNS:
            for m in re.finditer(patt, text):
                grp = m.group().strip()
                if patt == r"[A-Z]{2,}\s?[A-Z0-9\-]+" and not any(ch.isdigit() for ch in grp):
                    continue
                matches.append((m.start(), m.end(), grp))

        split_idx = MAX_LEN
        if matches:
            _, end, _ = min(matches, key=lambda x: x[0])
            split_idx = end

        # Move forward to next space
        while split_idx < len(text) and text[split_idx] != ' ':
            split_idx += 1
        if split_idx >= len(text) - 1:
            print(f"[fallback split] No space found after pattern in: '{text}'")
            return (text, "")

        split_idx += 1
        part1 = text[:split_idx].strip()
        part2 = text[split_idx:].strip()

        if len(part1) <= MAX_LEN and len(part2) <= MAX_LEN:
            return (part1, part2)

        print(f"[fallback split] Post-pattern split failed length check in: '{text}'")
        return (text, "")

    split_parts = df_over.apply(
        lambda r: split_to_two_parts(f"{r['Product_description']} {r['Product_description2']}"),
        axis=1, result_type='expand'
    )
    df_over['part1'] = split_parts[0]
    df_over['part2'] = split_parts[1]

    mask_second_under = (
        df_over['part1'].str.len() <= MAX_LEN
    ) & (
        df_over['part2'].str.len() <= MAX_LEN
    ) & (
        df_over['part2'].str.len() > 0
    )

    df_second_under = df_over[mask_second_under].copy()
    df_second_over = df_over[~mask_second_under].copy()

    df_second_under['Product_description'] = (
        df_second_under['part1'].apply(pad_to_29) +
        df_second_under['part2'].apply(pad_to_29)
    )
    df_second_under['Product_description2'] = pd.NA

    df_second_over['Product_description'] = (
        (df_second_over['part1'] + ' ' + df_second_over['part2'])
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
        .str.ljust(MAX_LEN)
    )
    df_second_over['Product_description2'] = pd.NA

    df_second_under = df_second_under[USE_COLUMNS]
    df_second_over = df_second_over[USE_COLUMNS]

    df_second_under.to_csv("second_part_under_29.csv", index=False, sep=DELIMITER, encoding=ENCODING)
    df_second_over.to_csv(OUTPUT_OVER, index=False, sep=DELIMITER, encoding=ENCODING)

    print(f"\n✅ Split 'over 29' rows: {len(df_second_under)} to 'second_part_under_29.csv', "
          f"{len(df_second_over)} remaining in '{OUTPUT_OVER}'")

# === Execution ===
all_files = glob(os.path.join(INPUT_FOLDER, "*.csv"))
frames = [df for f in all_files if (df := try_read_csv(f)) is not None]
if not frames:
    raise RuntimeError("No valid CSV files found.")

df_all = pd.concat(frames, ignore_index=True)

df_all = handle_duplicates(df_all)
df_all = df_all.apply(translate_missing, axis=1)

mask_over = (
    df_all['Product_description'].astype(str).str.len() > MAX_LEN
) | (
    df_all['Product_description2'].astype(str).str.len() > MAX_LEN
)
mask_total_under = (
    df_all['Product_description'].astype(str).str.len()
    + df_all['Product_description2'].astype(str).str.len()
) < MAX_LEN
mask_under = ~mask_over & ~mask_total_under

df_over = df_all[mask_over].copy()
split_over29_rows(df_over)

df_under = df_all[mask_under].copy()
df_under['Product_description'] = df_under.apply(
    lambda r: pad_to_29(r['Product_description']) + pad_to_29(r['Product_description2']),
    axis=1
)
df_under['Product_description2'] = pd.NA

df_total_under = df_all[mask_total_under].copy()
df_total_under['Product_description'] = df_total_under.apply(
    lambda r: f"{r['Product_description']} {r['Product_description2']}".strip().ljust(MAX_LEN),
    axis=1
)
df_total_under['Product_description2'] = pd.NA

for df in (df_under, df_total_under):
    df = df[USE_COLUMNS]

df_under.to_csv(OUTPUT_UNDER, index=False, sep=DELIMITER, encoding=ENCODING)
df_total_under.to_csv(OUTPUT_TOTAL_UNDER, index=False, sep=DELIMITER, encoding=ENCODING)

print(f"\n✅ Done! {len(df_total_under)} rows → '{OUTPUT_TOTAL_UNDER}', "
      f"{len(df_under)} rows → '{OUTPUT_UNDER}'")
