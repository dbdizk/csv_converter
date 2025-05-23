# 📚 CSV Tools for Product Data Processing

This repository contains Python scripts for advanced handling of product data in CSV format.
It includes tools for:

* Combining multiple CSV files
* Filtering and formatting product descriptions
* Smartly splitting descriptions based on patterns
* Handling duplicates cleanly
* Auto-translating missing English fields
* Ensuring UTF-8 compatibility with Finnish characters

---

## 📜 Scripts

### `csv_combiner.py`

Combines all `.csv` files from the `csvs_to_combine/` folder into one file: `combined_output.csv`.

**Features:**

* Merges all CSVs with `;` delimiter
* Keeps only rows with valid `Product_code`
* Removes trailing `.0` from codes (e.g., `1234.0` → `1234`)
* Removes rows where `Product_code` is blank, missing, or `"nan"`
* Preserves Finnish characters
* Outputs a clean, ready-to-use combined CSV

---

### `concat_and_split_products.py`

Processes product rows based on description lengths:

* `Product_description`
* `Product_description2`

**Outputs:**

* `under_29.csv`:
  Rows where **both** descriptions are individually under 29 characters.
  Fields are **padded** and **concatenated** for clean 29+29 formatting.

* `total_under_29.csv`:
  Rows where the **combined** description length is under 29 characters.
  Combined with a space and padded to 29.

* `over_29.csv`:
  Rows where **either** field exceeds 29 characters.
  **Smart chopping** applied based on custom patterns and spaces for clean line breaks.

**Advanced Features:**

* **Duplicate Handling:**

  * Auto-detects duplicates based on `Product_code`
  * If possible, resolves automatically by comparing Finnish and English descriptions
  * Otherwise, asks user to manually select

* **Automatic Translation:**

  * If `Translate_product_description` is empty, auto-translates Finnish `Product_description` to English using Google Translate API
  * Skips failed translations gracefully

* **Smart Pattern-Based Slicing:**

  * Known patterns (e.g., standards, material names, dimensions) are protected
  * Prefers natural breaks at spaces or patterns rather than hard-cutting words

---

### `chop_analyzer.py`

Helps generate your **custom chopping rules** for smart splitting!

**Features:**

* Scans your product descriptions for common patterns (e.g., dimensions like `114.3 x 2.11`)
* Shows you full context + proposed "choppable" candidates
* Asks you interactively whether to accept or reject each candidate
* Saves the accepted results in a `accepted_patterns.txt` for automatic usage by the main scripts

---

## 👢 Folder Structure

```
project_root/
👉
├── csv_combiner.py
├── concat_and_split_products.py
├── chop_analyzer.py
├── csvs_to_combine/          # CSVs for combiner
├── input_csvs/               # CSVs for concat & split
├── accepted_patterns.txt     # Chopping rules generated by chop_analyzer.py
├── combined_output.csv
├── under_29.csv
├── over_29.csv
├── total_under_29.csv
└── README.md
```

---

## ⚙️ Requirements

* Python 3.8+
* pandas
* rapidfuzz
* sentence-transformers
* deep-translator
* chardet

Install everything with:

```bash
pip install pandas rapidfuzz sentence-transformers deep-translator chardet
```

---

## 🇫🇮 Notes for Finnish Compatibility

* All CSVs are read and saved with `UTF-8 with BOM` (`utf-8-sig`) encoding
* `;` is used as CSV delimiter (standard in Finnish locales)
* Finnish characters (`ä`, `ö`, `å`, etc.) are fully preserved

---

## ✅ How to Run

**Combine CSVs:**

```bash
python csv_combiner.py
```

**Analyze chopping rules:**

```bash
python chop_analyzer.py
```

**Process, split and output final CSVs:**

```bash
python concat_and_split_products.py
```

---

# 🌟 Happy slicing, merging, and refining!

Built for elegance, clarity, and perfect product registers ✨
