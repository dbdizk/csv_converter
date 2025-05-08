# csv_converter# CSV Tools for Product Data

This repository contains Python scripts for processing and cleaning product data in CSV format. It includes tools for:

- Combining multiple CSVs into one  
- Filtering and formatting based on product description lengths  
- Cleaning inconsistent product codes  
- Ensuring UTF-8 compatibility with Finnish characters

---

## 📜 Scripts

### `csv_combiner.py`

Combines all `.csv` files from the `csvs_to_combine/` folder into one file: `combined_output.csv`.

**Features:**

- Automatically merges all CSVs using `;` as the delimiter  
- Keeps only rows with valid `Product_code`  
- Removes trailing `.0` from codes (e.g. `1234.0` → `1234`)  
- Filters out rows with:  
  - Blank codes  
  - Missing codes  
  - Literal string `"nan"` (case-insensitive)  
- Ensures UTF-8 compatibility for Finnish characters  

---

### `concat_and_split_products.py`

Processes product rows based on the length of two description fields:  
- `Product_description`  
- `Product_description2`  

**Outputs:**

- `under_29.csv`: Rows where both fields are under 29 characters; each field padded to 29 and concatenated.  
- `total_under_29.csv`: Rows where the **combined** length of both fields is less than 29 characters; combined with a space and padded to 29.  
- `over_29.csv`: Rows where **either** field exceeds 29 characters; untouched for review.  

---

## 🗂 Folder Structure

```
project_root/
│
├── csv_combiner.py
├── concat_and_split_products.py
├── csvs_to_combine/          # Place CSVs to merge here
├── input_csvs/               # Place CSVs for processing by concat_and_split_products.py
├── combined_output.csv
├── under_29.csv
├── over_29.csv
├── total_under_29.csv
└── README.md
```

---

## ⚙️ Requirements

- Python 3.7+
- pandas

Install with:

```
pip install pandas
```

---

## 🇫🇮 Notes for Finnish Compatibility

- All CSVs are read and saved with `UTF-8 with BOM` (`utf-8-sig`) encoding  
- `;` is used as the CSV delimiter (common in Finnish locales)  
- Special characters like `ä`, `ö`, `å` are preserved  

---

## ✅ How to Run

**Combine CSVs:**

```
python csv_combiner.py
```

**Split and process by description length:**

```
python concat_and_split_products.py
```

---

## 📬 Questions or Extensions?

Want to sort the final output, export to Excel, or customize formatting rules? Open an issue or suggest a feature!
