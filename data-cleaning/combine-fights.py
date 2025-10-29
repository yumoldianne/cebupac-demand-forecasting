#!/usr/bin/env python3
"""
combine_flights.py

Combine all sheets in files under ./data into a single table safely.
Outputs: ./output/combined.xlsx (or combined.csv if too large) and ./output/source_checksums.xlsx
"""

from pathlib import Path
import pandas as pd
import hashlib
import tempfile
import os
import sys
from datetime import datetime

# Configuration
DATA_DIR = Path("data")
OUTPUT_DIR = Path("data-cleaning/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EXPECTED_COLS = [
    "DATE", "FROM", "TO", "AIRCRAFT", "FLIGHT TIME", "STD",
    "ATD", "STA", "STATUS", "FLIGHT NUMBER", "AIRLINE"
]
EXCEL_ROW_LIMIT = 1_048_576  # Excel limit for rows per sheet

def file_md5(path: Path, chunk_size=8192):
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_read_excel(path: Path, sheet_name):
    # Read everything as string first to avoid dtype surprises
    try:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str, engine="openpyxl")
    except Exception:
        # fallback to default engine for older xls
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str)

def normalize_df(df: pd.DataFrame):
    # Normalize column names: strip, uppercase
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    # Replace em-dash and NBSP with NaN, strip whitespace
    df = df.replace({"—": pd.NA, "\u00A0": " "}, regex=True)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Ensure expected columns exist (add missing as NA)
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    # Keep only expected columns and in order
    df = df[EXPECTED_COLS]
    # Convert DATE to datetime if possible (coerce errors)
    try:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    except Exception:
        # leave as is if conversion fails
        pass
    # Ensure FLIGHT NUMBER is string, strip whitespace
    df["FLIGHT NUMBER"] = df["FLIGHT NUMBER"].astype("string")
    return df

def gather_files(data_dir: Path):
    exts = ("*.xlsx", "*.xls", "*.csv")
    files = []
    for ext in exts:
        files.extend(data_dir.rglob(ext))
    return sorted(set(files))

def main():
    files = gather_files(DATA_DIR)
    if not files:
        print(f"No files found under {DATA_DIR.resolve()}. Put your files in that folder and run again.")
        sys.exit(1)

    combined_dfs = []
    checks = []

    for f in files:
        print(f"Processing: {f}")
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        md5 = None
        try:
            md5 = file_md5(f)
        except Exception as e:
            print(f"  Warning: couldn't compute MD5 for {f}: {e}")

        if f.suffix.lower() in (".xlsx", ".xls"):
            try:
                xls = pd.ExcelFile(f, engine="openpyxl")
            except Exception:
                xls = pd.ExcelFile(f)  # fallback
            for sheet in xls.sheet_names:
                try:
                    df = safe_read_excel(f, sheet)
                except Exception as e:
                    print(f"  Error reading sheet '{sheet}' in {f}: {e}")
                    continue
                df = normalize_df(df)
                nrows = len(df)
                combined_dfs.append(df)
                checks.append({
                    "source_file": str(f),
                    "sheet": sheet,
                    "rows": nrows,
                    "md5": md5,
                    "last_modified": mtime
                })
                print(f"  -> sheet '{sheet}': {nrows} rows")
        else:  # CSV
            try:
                df = pd.read_csv(f, dtype=str)
            except Exception as e:
                print(f"  Error reading CSV {f}: {e}")
                continue
            df = normalize_df(df)
            nrows = len(df)
            combined_dfs.append(df)
            checks.append({
                "source_file": str(f),
                "sheet": f.name,
                "rows": nrows,
                "md5": md5,
                "last_modified": mtime
            })
            print(f"  -> csv '{f.name}': {nrows} rows")

    if not combined_dfs:
        print("No sheets read successfully. Exiting.")
        sys.exit(1)

    combined = pd.concat(combined_dfs, ignore_index=True)
    print(f"Total combined rows: {len(combined)}")

    # If too many rows for Excel, write CSV for combined
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_xlsx = OUTPUT_DIR / f"combined_{timestamp}.xlsx"
    combined_csv = OUTPUT_DIR / f"combined_{timestamp}.csv"
    checks_df = pd.DataFrame(checks)
    # Format last_modified column as ISO strings
    if "last_modified" in checks_df.columns:
        checks_df["last_modified"] = checks_df["last_modified"].apply(lambda x: x.isoformat() if pd.notna(x) else "")

    # Replace pandas NA with empty strings in checks_df for clearer output
    checks_df = checks_df.fillna("")

    # Atomic write: write to temp then replace
    if len(combined) >= EXCEL_ROW_LIMIT:
        # write CSV for combined, and write checks to an xlsx
        print("Combined row count exceeds Excel sheet limit; writing combined CSV and checks Excel.")
        temp_csv = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name)
        try:
            combined.to_csv(temp_csv, index=False)
            os.replace(temp_csv, combined_csv)
            # write checks as xlsx safely
            tmp_xlsx = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name)
            with pd.ExcelWriter(tmp_xlsx, engine="openpyxl") as w:
                checks_df.to_excel(w, index=False, sheet_name="source_checksums")
            os.replace(tmp_xlsx, combined_xlsx)
            print(f"Wrote: {combined_csv}")
            print(f"Wrote: {combined_xlsx} (checksums)")
        finally:
            # cleanup if temp still exists
            if temp_csv.exists() and temp_csv.name.startswith(tempfile.gettempdir()):
                pass
    else:
        # Write combined and checks as two sheets in one workbook
        tmp_xlsx = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name)
        try:
            with pd.ExcelWriter(tmp_xlsx, engine="openpyxl") as writer:
                combined.to_excel(writer, index=False, sheet_name="combined")
                checks_df.to_excel(writer, index=False, sheet_name="source_checksums")
            os.replace(tmp_xlsx, combined_xlsx)
            print(f"Wrote: {combined_xlsx}")
        finally:
            # nothing to do; os.replace should have moved it
            pass

    print("Done. Originals left untouched in the 'data/' folder.")
    print(f"Output files are in: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
