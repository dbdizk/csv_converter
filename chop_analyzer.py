import pandas as pd
import re
import os
from glob import glob

# Configuration
INPUT_FOLDER = "input_csvs"
DELIMITER = ";"
OUTPUT_FILE = "accepted_patterns.txt"
COLUMNS_TO_SCAN = ["Product_description", "Product_description2"]

# Patterns to detect choppable things
patterns = [
    r"\b(?:ASTM|EN|DIN|ISO|SS)\s?[A-Z0-9\-]+",  # Only real standards
    r"\bTP\d+[A-Z]*\b",                         # TP316L, TP304
    r"\([\dA-Za-z]+\)",                         # (5S), (STD)
    r"\bAISI\s?\d+\b",                           # AISI 316 etc
]


# Smart CSV reader

def try_read_csv(file):
    try:
        return pd.read_csv(file, encoding="utf-8", sep=DELIMITER)
    except UnicodeDecodeError:
        try:
            return pd.read_csv(file, encoding="latin1", sep=DELIMITER)
        except Exception as e:
            print(f"[ERROR] Could not read {file}: {e}")
            return None

# Load all CSVs
all_files = glob(os.path.join(INPUT_FOLDER, "*.csv"))
frames = []
for file in all_files:
    df = try_read_csv(file)
    if df is not None:
        frames.append(df)

if not frames:
    raise RuntimeError("No valid CSVs found.")

# Merge
full_df = pd.concat(frames, ignore_index=True)

def normalize_match(m):
    if isinstance(m, tuple):
        m = ''.join(m)
    return m.strip()


# Prepare unique found patterns
found_patterns = set()
approved_patterns = set()

# Analyze descriptions
for col in COLUMNS_TO_SCAN:
    if col not in full_df.columns:
        continue
    for desc in full_df[col].dropna().unique():
        desc = str(desc)
        for pat in patterns:
            matches = re.findall(pat, desc)
            for match in matches:
                normalized = normalize_match(match)
                if normalized not in found_patterns:
                    found_patterns.add(normalized)

                    # Show full description + candidate
                    print("\nFull Description:")
                    print(desc)
                    print("Candidate chop:")
                    print(f"  -> '{normalized}'")

                    while True:
                        choice = input("Keep this chop? (y/n): ").strip().lower()
                        if choice in ('y', 'n'):
                            break
                        else:
                            print("Please type 'y' or 'n' only.")

                    if choice == 'y':
                        approved_patterns.add(normalized)


# Save accepted
if approved_patterns:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("patterns = [\n")
        for pattern in sorted(approved_patterns):
            f.write(f'    "{pattern}",\n')
        f.write("]\n")

    print(f"\n Saved {len(approved_patterns)} confirmed patterns to '{OUTPUT_FILE}'.")
else:
    print("\n No patterns were approved.")
