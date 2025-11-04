import pandas as pd
import numpy as np
import re
import os
import csv
import unicodedata
from typing import List, Union

# -------------------------
# Robust reader + keep-only helpers
# -------------------------
def try_read(path):
    """Robust CSV/XLSX reader: tries encodings and delimiters and returns DataFrame."""
    if path.lower().endswith(('.xls', '.xlsx')):
        return pd.read_excel(path)

    # read a sample to let csv.Sniffer try detect delimiter
    sample = None
    try:
        with open(path, 'rb') as f:
            sample = f.read(8192)
    except Exception:
        sample = None

    sniffed_sep = None
    if sample is not None:
        try:
            sample_text = sample.decode('utf-8', errors='ignore')
            sniffed = csv.Sniffer().sniff(sample_text)
            sniffed_sep = sniffed.delimiter
        except Exception:
            sniffed_sep = None

    encs = ['utf-8', 'latin-1', 'cp1252']
    seps = [sniffed_sep] if sniffed_sep else [None, ',', '\t', ';', '|']

    for enc in encs:
        for sep in seps:
            try:
                if sep is None:
                    df = pd.read_csv(path, encoding=enc, engine='python')
                else:
                    df = pd.read_csv(path, encoding=enc, sep=sep, engine='python')
                print(f"[READ] Loaded {path!r} with encoding={enc!r} sep={sep!r}")
                return df
            except Exception:
                continue

    # last-resort fallback
    print(f"[READ] Fallback read for {path!r} (engine=python, default encoding).")
    return pd.read_csv(path, engine='python')


def normalise_text_cols(df, cols=None):
    """Unicode-normalise object columns, remove NBSP and common mojibake for em-dash."""
    if cols is None:
        cols = [c for c, dtype in df.dtypes.items() if dtype == 'object']
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).apply(
                lambda s: unicodedata.normalize('NFKC', s).replace('\xa0', ' ').strip()
            )
            df[c] = df[c].str.replace('â€”', '', regex=False).str.replace('—', '', regex=False)
    return df


# Canonical columns the pipeline expects (user-specified important columns)
WANTED_CANONICAL = [
    'DATE', 'FROM', 'TO', 'AIRCRAFT', 'FLIGHT NUMBER', 'AIRLINE',
    '# OF SEATS', 'LOAD FACTOR', 'MONTH'
]

# aliases to match noisy file headers
ALIASES = {
    'DATE': ['DATE', 'FLIGHT DATE', 'DEP DATE'],
    'FROM': ['FROM', 'ORIGIN', 'DEP'],
    'TO': ['TO', 'DEST', 'ARR'],
    'AIRCRAFT': ['AIRCRAFT', 'TYPE', 'ACFT'],
    'FLIGHT NUMBER': ['FLIGHT NUMBER', 'FLIGHT', 'FLIGHT NO', 'FLIGHT_NUMBER', 'FLIGHTNUMBER'],
    'AIRLINE': ['AIRLINE', 'CARRIER'],
    '# OF SEATS': ['# OF SEATS', 'NO. OF SEATS', 'SEATS', 'NUMBER OF SEATS'],
    'LOAD FACTOR': ['LOAD FACTOR', 'LOAD_FACTOR', 'LOADFACTOR', 'LOAD'],
    'MONTH': ['MONTH']
}


def keep_only_columns(df, wanted_canonicals=WANTED_CANONICAL, aliases=ALIASES, strict=False):
    """
    Return df with only the columns that map to wanted_canonicals.
    strict=False: don't raise if some canonical not found (we handle required checks downstream).
    """
    df_col_map = {c.strip().upper(): c for c in df.columns}
    selected = {}
    for canonical in wanted_canonicals:
        found = None
        for alt in aliases.get(canonical, [canonical]):
            key = alt.strip().upper()
            if key in df_col_map:
                found = df_col_map[key]
                break
        # substring fallback
        if found is None:
            for uc, orig in df_col_map.items():
                if canonical.replace(' ', '') in uc.replace(' ', ''):
                    found = orig
                    break
        if found:
            selected[found] = canonical
        else:
            if strict:
                raise KeyError(f"Required column for '{canonical}' not found. Available columns: {list(df.columns)}")
            # else: skip if absent

    if not selected:
        # nothing matched; return original df to let downstream error handling show more context
        return df

    kept_df = df[list(selected.keys())].copy()
    rename_map = {orig: canon for orig, canon in selected.items()}
    kept_df.rename(columns=rename_map, inplace=True)
    return kept_df


def read_and_keep(path):
    """Read file, normalise text cols, and keep only canonical columns (tolerant)."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = try_read(path)
    df.columns = [c.strip() for c in df.columns]
    df = normalise_text_cols(df)
    df = keep_only_columns(df, strict=False)
    return df


# -------------------------
# Helpers (IATA, HHI, checks)
# -------------------------
import unicodedata
import re

# --- Replace your existing extract_iata with this robust version ---
def extract_iata(cell):
    """
    Robust IATA extraction:
      - Unicode-normalises input
      - converts many parentheses types to ASCII ()
      - first tries to find 3-letter code inside parentheses
      - falls back to: trailing 3-letter token or any standalone 3-letter token
      - returns uppercase 3-letter code or None
    """
    if pd.isna(cell):
        return None
    s = str(cell)
    # Normalize unicode, remove NBSP and common mojibake
    s = unicodedata.normalize('NFKC', s).replace('\xa0', ' ').replace('\u3000', ' ').strip()
    s = s.replace('â€”', '').replace('\u2014','-').replace('\u2013','-')
    # Normalize a few bracket-like characters to ASCII parentheses
    s = s.translate(str.maketrans({
        '（':'(', '）':')', '【':'(', '】':')', '〔':'(', '〕':')',
        '[':'(', ']':')', '{':'(', '}':')', '＜':'(', '＞':')', '《':'(', '》':')'
    }))
    # Uppercase for consistent matching
    su = s.upper()

    # 1) Primary: explicit parentheses: (ABC) or ( ABC )
    m = re.search(r'\(\s*([A-Z]{3})\s*\)', su)
    if m:
        return m.group(1)

    # 2) Some data uses no parentheses but has e.g. "City - BCD" or "City BCD"
    # Try trailing token: last 3-letter token at end (word boundary)
    m2 = re.search(r'([A-Z]{3})\s*$', su)
    if m2:
        code = m2.group(1)
        # sanity: ensure code was not part of a longer token like "A320" (digits)
        if not re.search(r'\d', code):
            return code

    # 3) Fallback: any standalone 3-letter token (surrounded by non-letters or boundaries)
    m3 = re.search(r'\b([A-Z]{3})\b', su)
    if m3:
        return m3.group(1)

    return None

# --- Small helper to normalise FROM/TO columns then force-extract IATA for the whole DF ---
def normalise_cell_for_iata(x):
    """Apply small normalisations so extract_iata works consistently."""
    if pd.isna(x):
        return x
    s = str(x)
    s = unicodedata.normalize('NFKC', s).replace('\xa0',' ').strip()
    # replace weird comma separators inside cells that could confuse parsing (optional)
    # s = s.replace(' ,', ',').replace(', ', ', ')
    return s

def force_iata_extraction(df, from_col='FROM', to_col='TO', diag=True, max_display=8):
    """
    Normalises FROM/TO cells and (re)computes FROM_IATA, TO_IATA using the robust extractor.
    Call this after you have canonical 'FROM' and 'TO' columns (i.e. after rename).
    Prints small diagnostic of rows still missing IATA (so you can inspect the raw values).
    """
    # normalise text columns first
    if from_col in df.columns:
        df[from_col] = df[from_col].astype(str).apply(normalise_cell_for_iata)
    if to_col in df.columns:
        df[to_col] = df[to_col].astype(str).apply(normalise_cell_for_iata)

    df['FROM_IATA'] = df[from_col].apply(extract_iata) if from_col in df.columns else None
    df['TO_IATA']   = df[to_col].apply(extract_iata)   if to_col in df.columns else None

    if diag:
        # rows where either side is missing but FROM/TO contains parentheses-like text
        missing_from = df[df['FROM_IATA'].isna() & df[from_col].notna()]
        missing_to   = df[df['TO_IATA'].isna()   & df[to_col].notna()]
        # but focus on rows where FROM or TO contains '(' since IATA is expected in ()
        missing_from_paren = missing_from[missing_from[from_col].str.contains(r'\(|（', na=False)]
        missing_to_paren   = missing_to[missing_to[to_col].str.contains(r'\(|（', na=False)]

        if len(missing_from_paren):
            print(f"[IATA_DIAG] Rows where FROM had parentheses but FROM_IATA is still missing: {len(missing_from_paren)}")
            print(missing_from_paren[[from_col,'FROM_IATA']].head(max_display).to_string(index=False))
        if len(missing_to_paren):
            print(f"[IATA_DIAG] Rows where TO had parentheses but TO_IATA is still missing: {len(missing_to_paren)}")
            print(missing_to_paren[[to_col,'TO_IATA']].head(max_display).to_string(index=False))

        # also show a small random sample of any rows where both IATAs are missing for manual inspection
        both_missing = df[df['FROM_IATA'].isna() & df['TO_IATA'].isna()]
        if len(both_missing):
            print(f"[IATA_DIAG] Rows with both FROM_IATA and TO_IATA missing: {len(both_missing)} (showing up to {max_display})")
            cols_to_show = [from_col,'FROM_IATA', to_col,'TO_IATA','AIRLINE']
            existing = [c for c in cols_to_show if c in df.columns]
            print(both_missing[existing].head(max_display).to_string(index=False))

    return df

def extract_city_before_paren(cell):
    if pd.isna(cell):
        return None
    s = str(cell)
    if "(" in s:
        return s.split("(")[0].strip().upper()
    return s.strip().upper()


def calculate_hhi(values):
    """HHI where values are numeric shares base (we compute shares from values). Returns HHI on 0-10000 scale."""
    arr = np.array(values, dtype=float)
    if arr.size == 0:
        return 0.0
    total = arr.sum()
    if total == 0:
        return 0.0
    shares_pct = (arr / total) * 100.0
    return float(np.sum(shares_pct ** 2))


def _is_incomplete_route_val(val):
    """Return True if route is incomplete: missing, just '-', 'AAA-' or '-AAA' or empty side around dash."""
    if pd.isna(val):
        return True
    s = str(val).strip()
    if s == '' or s == '-':
        return True
    s_nospace = s.replace(' ', '')
    s_up = s_nospace.upper()
    # AAA- or -AAA (IATA-like)
    if re.match(r'^[A-Z]{3}-$', s_up) or re.match(r'^-[A-Z]{3}$', s_up):
        return True
    # more general: split by dash and check if either side empty
    parts = s.split('-')
    if len(parts) == 2:
        if parts[0].strip() == '' or parts[1].strip() == '':
            return True
    return False


# -------------------------
# Main pipeline (integrated + limited diagnostics)
# -------------------------
def compute_passenger_and_hhi(files_or_dfs: Union[List[str], pd.DataFrame, str],
                              outpath: str = 'final_routes_monthly_with_hhi.csv',
                              use_seats_times_load: bool = False,
                              diag: bool = True,
                              keep_incomplete_routes: bool = False,
                              focus_patterns: Union[List[str], None] = None,
                              max_display: int = 5):
    """
    files_or_dfs: single path, list of paths, or DataFrame(s).
    diag: if True print limited diagnostics at multiple points.
    keep_incomplete_routes: if True, do NOT drop rows flagged as incomplete routes after construction.
    focus_patterns: list of strings (text) to identify rows of interest to show in full detail.
                    defaults to ['5J125','BCD-CEB','Cebu Pacific'].
    max_display: maximum number of sample rows to print for each diagnostic.
    """
    if focus_patterns is None:
        focus_patterns = ['5J125', 'BCD-CEB', 'Cebu Pacific']

    def _match_focus(df):
        # returns boolean Series matching any focus pattern anywhere in the row (case-insensitive)
        pattern = '|'.join(map(re.escape, focus_patterns))
        return df.astype(str).apply(lambda col: col.str.contains(pattern, case=False, na=False)).any(axis=1)

    if not isinstance(files_or_dfs, (list, tuple)):
        files_or_dfs = [files_or_dfs]

    dfs = []
    ingested_info = []

    # --- read each input and record ingestion info ---
    for idx, item in enumerate(files_or_dfs):
        if isinstance(item, pd.DataFrame):
            df_copy = item.copy()
            dfs.append(df_copy)
            info = {'source': f'DataFrame_input_{idx}', 'rows': len(df_copy), 'cols': list(df_copy.columns)[:10]}
            ingested_info.append(info)
            if diag:
                print(f"[INGEST] {info['source']}: rows={info['rows']} cols={len(df_copy.columns)}")
                mask = _match_focus(df_copy)
                print(f"  -> matching focus rows in this input: {mask.sum()}")
                if mask.sum():
                    print(df_copy.loc[mask].head(max_display).to_string(index=False))
            continue

        # treat item as path: try variants
        tried = [item, item + '.csv', item + '.xlsx', item + '.xls']
        loaded = None
        used_path = None
        for p in tried:
            if os.path.exists(p):
                used_path = p
                try:
                    loaded = read_and_keep(p)
                except Exception as e:
                    raise RuntimeError(f"Failed to read and keep columns from {p}: {e}")
                break
        if loaded is None:
            raise FileNotFoundError(f"Could not find any of: {tried}")

        dfs.append(loaded)
        info = {'source': used_path, 'rows': len(loaded), 'cols': list(loaded.columns)[:10]}
        ingested_info.append(info)
        if diag:
            print(f"[INGEST] {used_path}: rows={info['rows']} cols={len(loaded.columns)}")
            mask = _match_focus(loaded)
            print(f"  -> matching focus rows in file: {mask.sum()}")
            if mask.sum():
                print(loaded.loc[mask].head(max_display).to_string(index=False))

    # brief ingestion summary
    if diag:
        print("\n[INGEST_SUMMARY]")
        total_rows_before_concat = sum(info['rows'] for info in ingested_info)
        for info in ingested_info:
            print(f" - {info['source']}: rows={info['rows']} cols_preview={info['cols']}")
        print(f" Total rows (sum of per-file counts): {total_rows_before_concat}")
        print(f" Concatenating {len(dfs)} dataframes...\n")

    # concatenate all inputs
    df = pd.concat(dfs, ignore_index=True)
    df.columns = [c.strip() for c in df.columns]
    if diag:
        print(f"[AFTER_CONCAT] total rows={len(df)} total cols={len(df.columns)}")
        mask_raw = _match_focus(df)
        print(f"[DIAG] focus matches right after concat: {mask_raw.sum()}")
        if mask_raw.sum():
            print(df.loc[mask_raw].head(max_display).to_string(index=False))
        else:
            print("  (no focus matches found right after concat)")

    # identify columns (case-insensitive)
    colmap = {c.upper(): c for c in df.columns}

    def pick(*choices_upper):
        for ch in choices_upper:
            if ch in colmap:
                return colmap[ch]
        return None

    AIRLINE_COL = pick('AIRLINE')
    DATE_COL = pick('DATE')
    FROM_COL = pick('FROM')
    TO_COL = pick('TO')
    SEATS_COL = pick('# OF SEATS', 'NO. OF SEATS', 'SEATS', 'NUMBER OF SEATS')
    LOAD_COL = pick('LOAD FACTOR', 'LOAD_FACTOR', 'LOADFACTOR', 'LOAD')
    MONTH_COL = pick('MONTH')

    if diag:
        print("[DIAG] Detected important columns (pre-rename):",
              {k: pick(k) for k in ('DATE', 'FROM', 'TO', 'AIRLINE', 'LOAD FACTOR', 'MONTH')})

    # required checks (same as original)
    if DATE_COL is None:
        raise KeyError("DATE column not found.")
    if FROM_COL is None or TO_COL is None:
        raise KeyError("FROM and/or TO column not found.")
    if AIRLINE_COL is None:
        raise KeyError("AIRLINE column not found.")
    if LOAD_COL is None:
        raise KeyError("LOAD FACTOR column not found.")

    # rename to canonical names
    df = df.rename(columns={
        DATE_COL: 'DATE',
        FROM_COL: 'FROM',
        TO_COL: 'TO',
        AIRLINE_COL: 'AIRLINE',
        LOAD_COL: 'LOAD_FACTOR',
    })
    if SEATS_COL:
        df = df.rename(columns={SEATS_COL: 'SEATS'})
    if MONTH_COL:
        df = df.rename(columns={MONTH_COL: 'MONTH'})

    if diag:
        print("[DIAG] Columns after rename:", df.columns.tolist())
        mask_after_rename = _match_focus(df)
        print(f"[DIAG] focus matches after rename: {mask_after_rename.sum()}")
        if mask_after_rename.sum():
            print(df.loc[mask_after_rename, ['DATE', 'FROM', 'TO', 'AIRLINE']].head(max_display).to_string(index=False))

    # parse date and build period columns
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
    df['Year'] = df['DATE'].dt.year
    if 'MONTH' in df.columns and pd.api.types.is_numeric_dtype(df['MONTH']):
        df['Month'] = df['MONTH'].astype(int)
    else:
        df['Month'] = df['DATE'].dt.month

    # build route and iata extraction (diagnostic on focus rows)
    df['FROM_IATA'] = df['FROM'].apply(extract_iata)
    df['TO_CITY'] = df['TO'].apply(extract_city_before_paren)
    df['TO_IATA'] = df['TO'].apply(extract_iata)
    df = force_iata_extraction(df, from_col='FROM', to_col='TO', diag=True, max_display=8)
    df['ROUTE'] = df['FROM_IATA'].fillna('') + "-" + df['TO_IATA'].fillna('')

    if diag:
        mask_target = _match_focus(df)
        print(f"[DIAG] IATA extraction matches (focus): {mask_target.sum()}")
        if mask_target.sum():
            print(df.loc[mask_target, ['DATE', 'FROM', 'FROM_IATA', 'TO', 'TO_IATA', 'ROUTE', 'AIRLINE']].head(max_display).to_string(index=False))
        else:
            print("  (no focus matches to show IATA extraction)")

    # inspect mask of constructed-incomplete routes
    mask_incomplete_constructed = df['ROUTE'].apply(_is_incomplete_route_val)
    if diag:
        print(f"[DIAG] Constructed-incomplete ROUTE count: {mask_incomplete_constructed.sum()} / {len(df)}")
        if mask_incomplete_constructed.sum():
            print(df.loc[mask_incomplete_constructed, ['DATE', 'FROM', 'TO', 'FROM_IATA', 'TO_IATA', 'ROUTE', 'AIRLINE']].head(max_display).to_string(index=False))

    # drop incomplete constructed routes unless user asked to keep them
    if mask_incomplete_constructed.any():
        if keep_incomplete_routes:
            if diag:
                print("[DIAG] keep_incomplete_routes=True -> NOT dropping constructed-incomplete route rows.")
        else:
            if diag:
                print(f"[ACTION] Dropping {mask_incomplete_constructed.sum()} constructed-incomplete route rows.")
            df = df.loc[~mask_incomplete_constructed].reset_index(drop=True)

    # drop unwanted airlines (defensive)
    unwanted_airlines = {'unknown', 'sunlight air', 'seair international', 'jeju air', 'air seoul'}
    if 'AIRLINE' in df.columns:
        airline_series = df['AIRLINE'].astype(str).str.strip().str.lower()
        mask_unwanted = airline_series.isin(unwanted_airlines)
        if diag:
            print(f"[DIAG] Unwanted-airline rows to drop: {mask_unwanted.sum()}")
        if mask_unwanted.any():
            df = df.loc[~mask_unwanted].reset_index(drop=True)

    # clean numeric fields
    df['LOAD_FACTOR'] = pd.to_numeric(df['LOAD_FACTOR'], errors='coerce').fillna(0.0)
    if 'SEATS' in df.columns:
        df['SEATS'] = pd.to_numeric(df['SEATS'], errors='coerce').fillna(0.0)

    # compute the metric to aggregate (Passenger)
    if use_seats_times_load and 'SEATS' in df.columns:
        df['PASSENGER_METRIC'] = df['SEATS'] * df['LOAD_FACTOR']
    else:
        df['PASSENGER_METRIC'] = df['LOAD_FACTOR']

    # aggregate Passenger by Airline-Route-Year-Month
    grouped = df.groupby(['AIRLINE', 'ROUTE', 'Year', 'Month'], as_index=False).agg(
        Passenger=('PASSENGER_METRIC', 'sum')
    )

    # airline totals per period & total period
    airline_period = grouped.groupby(['Year', 'Month', 'AIRLINE'])['Passenger'].sum().reset_index(name='Airline_Load_Period')
    total_period = grouped.groupby(['Year', 'Month'])['Passenger'].sum().reset_index(name='Total_Load_Period')

    grouped = grouped.merge(airline_period, on=['Year', 'Month', 'AIRLINE'], how='left')
    grouped = grouped.merge(total_period, on=['Year', 'Month'], how='left')

    grouped['OwnShfli'] = grouped.apply(
        lambda r: round((r['Airline_Load_Period'] / r['Total_Load_Period'] * 100.0) if (r['Total_Load_Period'] and r['Airline_Load_Period']) else 0.0, 2),
        axis=1
    )

    # Route HHI (per Year,Month,Route)
    route_air_vals = grouped.groupby(['Year', 'Month', 'ROUTE', 'AIRLINE'])['Passenger'].sum().reset_index()
    route_hhi = {}
    for (y, m, route), grp in route_air_vals.groupby(['Year', 'Month', 'ROUTE']):
        route_hhi[(y, m, route)] = calculate_hhi(grp['Passenger'].values)
    grouped['RouteHHI'] = grouped.apply(lambda r: round(route_hhi.get((r['Year'], r['Month'], r['ROUTE']), 0.0), 2), axis=1)

    # Airport HHIs and Flight Counts:
    from_ends = df[['Year', 'Month', 'FROM_IATA', 'AIRLINE', 'PASSENGER_METRIC']].rename(columns={'FROM_IATA': 'AIRPORT', 'PASSENGER_METRIC': 'VAL'}).dropna(subset=['AIRPORT'])
    to_ends = df[['Year', 'Month', 'TO_IATA', 'AIRLINE', 'PASSENGER_METRIC']].rename(columns={'TO_IATA': 'AIRPORT', 'PASSENGER_METRIC': 'VAL'}).dropna(subset=['AIRPORT'])
    ends = pd.concat([from_ends, to_ends], ignore_index=True)

    seats_by_airport = ends.groupby(['Year', 'Month', 'AIRPORT', 'AIRLINE'])['VAL'].sum().reset_index()
    airport_hhi = {}
    for (y, m, airport), grp in seats_by_airport.groupby(['Year', 'Month', 'AIRPORT']):
        airport_hhi[(y, m, airport)] = calculate_hhi(grp['VAL'].values)

    flights_by_airport = ends.groupby(['Year', 'Month', 'AIRPORT']).size().reset_index(name='Flights_Count')
    flights_map = {(int(r.Year), int(r.Month), r.AIRPORT): int(r.Flights_Count) for r in flights_by_airport.itertuples()}

    # mapping route -> endpoint IATA codes (first occurrence)
    route_map = df[['ROUTE', 'FROM_IATA', 'TO_IATA']].drop_duplicates(subset=['ROUTE']).set_index('ROUTE')

    def _get_airhhi(y, m, route, endpoint):
        try:
            row = route_map.loc[route]
            airport = row['FROM_IATA'] if endpoint == 'FROM' else row['TO_IATA']
            if pd.isna(airport):
                return 0.0
            return round(airport_hhi.get((y, m, airport), 0.0), 2)
        except Exception:
            return 0.0

    def _get_airfli(y, m, route, endpoint):
        try:
            row = route_map.loc[route]
            airport = row['FROM_IATA'] if endpoint == 'FROM' else row['TO_IATA']
            if pd.isna(airport):
                return 0
            return flights_map.get((int(y), int(m), airport), 0)
        except Exception:
            return 0

    grouped['AirHHI_From'] = grouped.apply(lambda r: _get_airhhi(r['Year'], r['Month'], r['ROUTE'], 'FROM'), axis=1)
    grouped['AirHHI_To'] = grouped.apply(lambda r: _get_airhhi(r['Year'], r['Month'], r['ROUTE'], 'TO'), axis=1)

    grouped['AirFli_From'] = grouped.apply(lambda r: _get_airfli(r['Year'], r['Month'], r['ROUTE'], 'FROM'), axis=1)
    grouped['AirFli_To'] = grouped.apply(lambda r: _get_airfli(r['Year'], r['Month'], r['ROUTE'], 'TO'), axis=1)

    final = grouped[['AIRLINE', 'ROUTE', 'Year', 'Month', 'Passenger', 'OwnShfli', 'RouteHHI',
                     'AirHHI_From', 'AirHHI_To', 'AirFli_From', 'AirFli_To']].copy()
    final = final.rename(columns={'AIRLINE': 'Airline', 'Year': 'Year', 'Month': 'Month'})

    # Final diagnostic: show only focused matches (limited)
    if diag:
        pattern = '|'.join(map(re.escape, focus_patterns))
        mask_final = final.astype(str).apply(lambda col: col.str.contains(pattern, case=False, na=False)).any(axis=1)
        print(f"[FINAL_DIAG] Rows in final matching focus patterns: {mask_final.sum()}")
        if mask_final.sum():
            print(final.loc[mask_final].head(max_display).to_string(index=False))
        else:
            print("[FINAL_DIAG] No matching aggregated rows found in final result.")

    # sort and save
    final = final.sort_values(['Year', 'Month', 'ROUTE', 'Airline']).reset_index(drop=True)
    final.to_csv(outpath, index=False)
    if diag:
        print(f"[SAVE] Saved final file to: {outpath}")
    return final


# -------------------------
# Example usage (if run as script)
# -------------------------
if __name__ == "__main__":
    files = ['data/data_0.csv', 'data/data_1.csv', 'data/data_2.csv', 'data/data_3.csv']
    # diag=True prints limited diagnostics; set keep_incomplete_routes=True to avoid dropping constructed-incomplete routes
    final_df = compute_passenger_and_hhi(files,
                                         outpath='final_probit_with_diag.csv',
                                         diag=True,
                                         keep_incomplete_routes=False,
                                         focus_patterns=['5J125', 'BCD-CEB', 'Cebu Pacific'],
                                         max_display=5)