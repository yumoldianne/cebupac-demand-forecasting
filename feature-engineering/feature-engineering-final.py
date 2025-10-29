import pandas as pd
import numpy as np
import re
import os
from typing import List, Union

# -------------------------
# Helpers
# -------------------------
def try_read(path):
    """Try reading CSV/XLSX with latin-1 fallback to cp1252 then default."""
    if path.lower().endswith(('.xls', '.xlsx')):
        return pd.read_excel(path)
    try:
        return pd.read_csv(path, encoding='latin-1')
    except Exception:
        try:
            return pd.read_csv(path, encoding='cp1252')
        except Exception:
            return pd.read_csv(path)

def extract_iata(cell):
    if pd.isna(cell):
        return None
    m = re.search(r'\(([A-Z]{3})\)', str(cell).upper())
    return m.group(1) if m else None

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
# Main pipeline
# -------------------------
def compute_passenger_and_hhi(files_or_dfs: Union[List[str], pd.DataFrame, str],
                              outpath: str = 'final_routes_monthly_with_hhi.csv',
                              use_seats_times_load: bool = False):
    """
    files_or_dfs: single path, list of paths, or DataFrame(s).
    use_seats_times_load: if True, Passenger = sum(# of seats * load_factor)
                         if False (default), Passenger = sum(LOAD FACTOR)
    """
    # normalize input to list
    if not isinstance(files_or_dfs, (list, tuple)):
        files_or_dfs = [files_or_dfs]

    dfs = []
    for item in files_or_dfs:
        if isinstance(item, pd.DataFrame):
            dfs.append(item.copy())
            continue
        # try path variants
        tried = [item, item + '.csv', item + '.xlsx', item + '.xls']
        loaded = None
        for p in tried:
            if os.path.exists(p):
                loaded = try_read(p)
                break
        if loaded is None:
            raise FileNotFoundError(f"Could not find any of: {tried}")
        dfs.append(loaded)

    df = pd.concat(dfs, ignore_index=True)
    df.columns = [c.strip() for c in df.columns]

    # normalize column names to known names
    colmap = {c.upper(): c for c in df.columns}
    def pick(*choices_upper):
        for ch in choices_upper:
            if ch in colmap:
                return colmap[ch]
        return None

    # ---------- EARLY DROPS ----------
    # drop rows with unwanted airlines (do this as early as possible)
    unwanted_airlines = {
        'unknown', 'sunlight air', 'seair international', 'jeju air', 'air seoul'
    }

    AIRLINE_COL = pick('AIRLINE')
    if AIRLINE_COL is not None:
        # Make safe series (string, stripped, lower) and drop matches
        airline_series = df[AIRLINE_COL].astype(str).str.strip().str.lower()
        mask_unwanted = airline_series.isin(unwanted_airlines)
        if mask_unwanted.any():
            df = df.loc[~mask_unwanted].reset_index(drop=True)

    # If input already has a ROUTE column, drop incomplete ones immediately
    ROUTE_COL_INPUT = pick('ROUTE')
    if ROUTE_COL_INPUT is not None:
        mask_incomplete_route = df[ROUTE_COL_INPUT].apply(_is_incomplete_route_val)
        if mask_incomplete_route.any():
            df = df.loc[~mask_incomplete_route].reset_index(drop=True)

    # -------------------------
    # Continue with normal column detection/renaming
    # -------------------------
    DATE_COL = pick('DATE')
    FROM_COL = pick('FROM')
    TO_COL = pick('TO')
    # AIRLINE_COL already set above (may be None)
    SEATS_COL = pick('# OF SEATS','NO. OF SEATS','SEATS','NUMBER OF SEATS')
    LOAD_COL = pick('LOAD FACTOR','LOAD_FACTOR','LOADFACTOR')
    MONTH_COL = pick('MONTH')

    if DATE_COL is None:
        raise KeyError("DATE column not found.")
    if FROM_COL is None or TO_COL is None:
        raise KeyError("FROM and/or TO column not found.")
    if AIRLINE_COL is None:
        raise KeyError("AIRLINE column not found.")
    if LOAD_COL is None:
        raise KeyError("LOAD FACTOR column not found.")

    # standardize names
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

    # parse date and period
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
    df['Year'] = df['DATE'].dt.year
    # if a MONTH column exists and looks numeric, use it; otherwise use the date
    if 'MONTH' in df.columns and pd.api.types.is_numeric_dtype(df['MONTH']):
        df['Month'] = df['MONTH'].astype(int)
    else:
        df['Month'] = df['DATE'].dt.month

    # build route: IATA-FULLCITY uppercase for destination (MNL-CEBU)
    df['FROM_IATA'] = df['FROM'].apply(extract_iata)
    df['TO_CITY'] = df['TO'].apply(extract_city_before_paren)
    df['TO_IATA'] = df['TO'].apply(extract_iata)
    df['ROUTE'] = df['FROM_IATA'].fillna('') + "-" + df['TO_IATA'].fillna('')

    # After constructing ROUTE, drop rows where constructed ROUTE is incomplete
    mask_incomplete_constructed = df['ROUTE'].apply(_is_incomplete_route_val)
    if mask_incomplete_constructed.any():
        df = df.loc[~mask_incomplete_constructed].reset_index(drop=True)

    # Also re-ensure we don't have unwanted airlines after rename (defensive)
    if 'AIRLINE' in df.columns:
        airline_series = df['AIRLINE'].astype(str).str.strip().str.lower()
        mask_unwanted = airline_series.isin(unwanted_airlines)
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
    grouped = df.groupby(['AIRLINE','ROUTE','Year','Month'], as_index=False).agg(
        Passenger=('PASSENGER_METRIC','sum')
    )

    # airline totals per period & total period
    airline_period = grouped.groupby(['Year','Month','AIRLINE'])['Passenger'].sum().reset_index(name='Airline_Load_Period')
    total_period = grouped.groupby(['Year','Month'])['Passenger'].sum().reset_index(name='Total_Load_Period')

    grouped = grouped.merge(airline_period, on=['Year','Month','AIRLINE'], how='left')
    grouped = grouped.merge(total_period, on=['Year','Month'], how='left')

    grouped['OwnShfli'] = grouped.apply(
        lambda r: round((r['Airline_Load_Period'] / r['Total_Load_Period'] * 100.0) if (r['Total_Load_Period'] and r['Airline_Load_Period']) else 0.0, 2),
        axis=1
    )

    # Route HHI (per Year,Month,Route)
    route_air_vals = grouped.groupby(['Year','Month','ROUTE','AIRLINE'])['Passenger'].sum().reset_index()
    route_hhi = {}
    for (y,m,route), grp in route_air_vals.groupby(['Year','Month','ROUTE']):
        route_hhi[(y,m,route)] = calculate_hhi(grp['Passenger'].values)
    grouped['RouteHHI'] = grouped.apply(lambda r: round(route_hhi.get((r['Year'], r['Month'], r['ROUTE']), 0.0), 2), axis=1)

    # Airport HHIs and Flight Counts:
    # We'll count flights touching each airport during the Year-Month (arrivals + departures).
    from_ends = df[['Year','Month','FROM_IATA','AIRLINE','PASSENGER_METRIC']].rename(columns={'FROM_IATA':'AIRPORT','PASSENGER_METRIC':'VAL'}).dropna(subset=['AIRPORT'])
    to_ends   = df[['Year','Month','TO_IATA','AIRLINE','PASSENGER_METRIC']].rename(columns={'TO_IATA':'AIRPORT','PASSENGER_METRIC':'VAL'}).dropna(subset=['AIRPORT'])
    ends = pd.concat([from_ends, to_ends], ignore_index=True)

    # HHI map (same as before)
    seats_by_airport = ends.groupby(['Year','Month','AIRPORT','AIRLINE'])['VAL'].sum().reset_index()
    airport_hhi = {}
    for (y,m,airport), grp in seats_by_airport.groupby(['Year','Month','AIRPORT']):
        airport_hhi[(y,m,airport)] = calculate_hhi(grp['VAL'].values)

    # Flights count map: number of flights touching that airport in the period (arrivals+departures)
    flights_by_airport = ends.groupby(['Year','Month','AIRPORT']).size().reset_index(name='Flights_Count')
    flights_map = {(int(r.Year), int(r.Month), r.AIRPORT): int(r.Flights_Count) for r in flights_by_airport.itertuples()}

    # mapping route -> endpoint IATA codes (first occurrence)
    route_map = df[['ROUTE','FROM_IATA','TO_IATA']].drop_duplicates(subset=['ROUTE']).set_index('ROUTE')

    def _get_airhhi(y, m, route, endpoint):
        try:
            row = route_map.loc[route]
            airport = row['FROM_IATA'] if endpoint=='FROM' else row['TO_IATA']
            if pd.isna(airport):
                return 0.0
            return round(airport_hhi.get((y,m,airport), 0.0), 2)
        except Exception:
            return 0.0

    def _get_airfli(y, m, route, endpoint):
        try:
            row = route_map.loc[route]
            airport = row['FROM_IATA'] if endpoint=='FROM' else row['TO_IATA']
            if pd.isna(airport):
                return 0
            return flights_map.get((int(y), int(m), airport), 0)
        except Exception:
            return 0

    grouped['AirHHI_From'] = grouped.apply(lambda r: _get_airhhi(r['Year'], r['Month'], r['ROUTE'], 'FROM'), axis=1)
    grouped['AirHHI_To']   = grouped.apply(lambda r: _get_airhhi(r['Year'], r['Month'], r['ROUTE'], 'TO'), axis=1)

    # NEW: number of flights served at route endpoints in the period
    grouped['AirFli_From'] = grouped.apply(lambda r: _get_airfli(r['Year'], r['Month'], r['ROUTE'], 'FROM'), axis=1)
    grouped['AirFli_To']   = grouped.apply(lambda r: _get_airfli(r['Year'], r['Month'], r['ROUTE'], 'TO'), axis=1)

    # final selection & rename
    final = grouped[['AIRLINE','ROUTE','Year','Month','Passenger','OwnShfli','RouteHHI',
                     'AirHHI_From','AirHHI_To','AirFli_From','AirFli_To']].copy()
    final = final.rename(columns={'AIRLINE':'Airline','Year':'Year','Month':'Month'})

    # sort and save
    final = final.sort_values(['Year','Month','ROUTE','Airline']).reset_index(drop=True)
    final.to_csv(outpath, index=False)
    print(f"Saved final file to: {outpath}")
    return final

# -------------------------
# Example usage (if run as script)
# -------------------------
if __name__ == "__main__":
    # If you have a directory of CSVs you could do:
    # files = ['data/file1.csv', 'data/file2.csv']
    # Or pass a single CSV path: files = 'data/myfile.csv'
    # Or pass a DataFrame directly.

    # Example: run on sample CSV (uncomment below to test inline)
    #sample_df = pd.read_csv('path_to_sample.csv')  # or load from your files
    final_df = compute_passenger_and_hhi('final.csv', outpath='final_out.csv')

    # For demonstration only, do nothing here.
    pass