import pandas as pd
import numpy as np
from pathlib import Path
import glob

def process_flight_data(input_pattern='*.csv', output_file='consolidated_flight_data.csv', input_folders=None):
    """
    Process multiple CSV files containing flight data and compute HHI metrics.
    
    Parameters:
    -----------
    input_pattern : str or list
        Glob pattern(s) to match CSV files (e.g., '*.csv' or 'flights_*.csv')
        Can be a single string or list of patterns
    output_file : str
        Name of the output consolidated file
    input_folders : list, optional
        List of folders to search for CSV files
    """
    
    # Handle multiple input patterns or folders
    csv_files = []
    
    if input_folders:
        # Search in multiple folders
        for folder in input_folders:
            pattern = f"{folder}/*.csv" if not folder.endswith('.csv') else folder
            csv_files.extend(glob.glob(pattern))
    elif isinstance(input_pattern, list):
        # Multiple patterns provided
        for pattern in input_pattern:
            csv_files.extend(glob.glob(pattern))
    else:
        # Single pattern
        csv_files = glob.glob(input_pattern)
    
    if not csv_files:
        print(f"No CSV files found matching pattern: {input_pattern}")
        return None
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Read and combine all CSV files
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            print(f"  ✓ Loaded: {file}")
        except Exception as e:
            print(f"  ✗ Error loading {file}: {e}")
    
    if not dfs:
        print("No data loaded successfully")
        return None
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal records loaded: {len(combined_df)}")
    
    # Clean column names (remove extra spaces)
    combined_df.columns = combined_df.columns.str.strip()
    
    # Display available columns for debugging
    print(f"\nAvailable columns in data:")
    for i, col in enumerate(combined_df.columns, 1):
        print(f"  {i}. '{col}'")
    
    # Remove completely empty rows
    combined_df = combined_df.dropna(how='all')
    print(f"After removing empty rows: {len(combined_df)} records")
    
    # Map common column name variations (expanded to handle more cases)
    column_mapping = {
        'LOAD FAC': ['LOAD FAC', 'LOAD_FAC', 'LOADFAC', 'LOAD FACTOR', 'LOAD_FACTOR', 'LOADFACTOR'],
        '# OF SEAT': ['# OF SEAT', '# OF SEATS', 'NUM_SEATS', 'SEATS', 'NUMBER OF SEATS', 'NO OF SEATS', 'SEAT COUNT'],
        'FROM': ['FROM', 'ORIGIN', 'DEPARTURE', 'DEP', 'DEPARTURE AIRPORT'],
        'TO': ['TO', 'DESTINATION', 'ARRIVAL', 'ARR', 'ARRIVAL AIRPORT'],
        'AIRLINE': ['AIRLINE', 'CARRIER', 'AIRLINE_NAME', 'AIRLINE NAME'],
        'DATE': ['DATE', 'FLIGHT_DATE', 'DEPARTURE_DATE', 'FLIGHT DATE'],
        'MONTH': ['MONTH', 'MTH', 'MONTH_NUM']
    }
    
    # Find actual column names
    actual_columns = {}
    for standard_name, variations in column_mapping.items():
        for variation in variations:
            if variation in combined_df.columns:
                actual_columns[standard_name] = variation
                break
    
    # Check if required columns exist
    required_cols = ['LOAD FAC', '# OF SEAT', 'FROM', 'TO', 'AIRLINE']
    missing_cols = [col for col in required_cols if col not in actual_columns]
    
    if missing_cols:
        print(f"\n❌ ERROR: Could not find columns for: {missing_cols}")
        print(f"Please check your CSV file column names.")
        print(f"\nExpected columns include:")
        print(f"  - Load Factor: LOAD FAC, LOAD_FAC, or LOADFAC")
        print(f"  - Seats: # OF SEAT, # OF SEATS, or SEATS")
        print(f"  - Origin: FROM or ORIGIN")
        print(f"  - Destination: TO or DESTINATION")
        print(f"  - Airline: AIRLINE or CARRIER")
        return None
    
    # Rename columns to standard names
    rename_dict = {v: k for k, v in actual_columns.items()}
    combined_df.rename(columns=rename_dict, inplace=True)
    print(f"\n✓ Column mapping successful")
    
    # Parse dates - handle multiple formats
    if 'DATE' in combined_df.columns:
        # Try multiple date formats
        combined_df['DATE'] = pd.to_datetime(combined_df['DATE'], errors='coerce', dayfirst=True)
        combined_df['Year'] = combined_df['DATE'].dt.year
        
        # If MONTH column exists, use it; otherwise extract from DATE
        if 'MONTH' not in combined_df.columns:
            combined_df['Month'] = combined_df['DATE'].dt.month
        else:
            # Use MONTH column but fill missing values from DATE
            combined_df['Month'] = combined_df['MONTH'].fillna(combined_df['DATE'].dt.month)
    elif 'MONTH' in combined_df.columns:
        combined_df['Month'] = combined_df['MONTH']
        # Default year if no date column
        combined_df['Year'] = 2025  # Adjust based on your data
    
    # Remove rows where essential data is missing
    before_clean = len(combined_df)
    combined_df = combined_df.dropna(subset=['FROM', 'TO', 'AIRLINE', 'LOAD FAC', '# OF SEAT'])
    after_clean = len(combined_df)
    if before_clean > after_clean:
        print(f"Removed {before_clean - after_clean} rows with missing essential data")
    
    # Extract airport codes from "City (CODE)" format if present
    def extract_airport_code(location):
        if pd.isna(location):
            return location
        # Try to extract code from format like "Manila (MNL)"
        if '(' in str(location) and ')' in str(location):
            return str(location).split('(')[1].split(')')[0].strip()
        return str(location).strip()
    
    combined_df['FROM'] = combined_df['FROM'].apply(extract_airport_code)
    combined_df['TO'] = combined_df['TO'].apply(extract_airport_code)
    
    # Create route identifier (FROM-TO)
    combined_df['ROUTE'] = combined_df['FROM'].str.strip() + '-' + combined_df['TO'].str.strip()
    
    # Handle Load Factor - check if it needs to be divided by 100
    # If load factor > 100, it's likely in format like 150.288 meaning 150.288 passengers, not 150%
    # In this case, passengers = LOAD FAC directly, not (LOAD FAC * SEATS / 100)
    max_load_factor = combined_df['LOAD FAC'].max()
    
    if max_load_factor > 100:
        # Load factor appears to be passenger count, not percentage
        print(f"⚠ Load factor values > 100 detected (max: {max_load_factor:.2f})")
        print(f"  Assuming 'LOAD FACTOR' column contains passenger counts, not percentages")
        combined_df['Passengers'] = combined_df['LOAD FAC']
    else:
        # Normal case: load factor is a percentage
        combined_df['Passengers'] = (combined_df['LOAD FAC'] * combined_df['# OF SEAT']) / 100
    
    print(f"✓ Calculated passengers for {len(combined_df)} records")
    
    # Group by Airline, Route, Year, Month
    grouped = combined_df.groupby(['AIRLINE', 'ROUTE', 'Year', 'Month']).agg({
        'Passengers': 'sum',
        '# OF SEAT': 'sum'
    }).reset_index()
    
    grouped.columns = ['Airline', 'ROUTE', 'Year', 'Month', 'Passenger', 'TotalSeats']
    
    # Calculate OwnShfli (Own Share of Flight on Route)
    # This is the percentage of seats this airline has on this specific route
    route_totals = grouped.groupby(['ROUTE', 'Year', 'Month'])['TotalSeats'].transform('sum')
    grouped['OwnShfli'] = (grouped['TotalSeats'] / route_totals * 100).round(2)
    
    # Calculate RouteHHI (Herfindahl-Hirschman Index for each route)
    # HHI = sum of squared market shares
    grouped['RouteHHI'] = grouped.groupby(['ROUTE', 'Year', 'Month'])['OwnShfli'].transform(
        lambda x: (x ** 2).sum()
    ).round(2)
    
    # Extract FROM and TO airports from ROUTE
    grouped[['FROM_AIRPORT', 'TO_AIRPORT']] = grouped['ROUTE'].str.split('-', expand=True)
    
    # Calculate Airport HHI for FROM airport
    # Sum all seats by airline from each origin airport across all routes
    from_airport_airline = combined_df.groupby(['FROM', 'AIRLINE', 'Year', 'Month'])['# OF SEAT'].sum().reset_index()
    from_airport_airline.columns = ['FROM', 'Airline', 'Year', 'Month', 'FromSeats']
    
    # Calculate market share at FROM airport
    from_total = from_airport_airline.groupby(['FROM', 'Year', 'Month'])['FromSeats'].transform('sum')
    from_airport_airline['FromShare'] = (from_airport_airline['FromSeats'] / from_total * 100)
    
    # Calculate FROM airport HHI
    from_hhi = from_airport_airline.groupby(['FROM', 'Year', 'Month'])['FromShare'].apply(
        lambda x: (x ** 2).sum()
    ).reset_index()
    from_hhi.columns = ['FROM', 'Year', 'Month', 'AirHHI_From']
    
    # Calculate total flights from FROM airport
    from_flights = combined_df.groupby(['FROM', 'Year', 'Month']).size().reset_index(name='AirFli_From')
    
    # Calculate Airport HHI for TO airport
    to_airport_airline = combined_df.groupby(['TO', 'AIRLINE', 'Year', 'Month'])['# OF SEAT'].sum().reset_index()
    to_airport_airline.columns = ['TO', 'Airline', 'Year', 'Month', 'ToSeats']
    
    # Calculate market share at TO airport
    to_total = to_airport_airline.groupby(['TO', 'Year', 'Month'])['ToSeats'].transform('sum')
    to_airport_airline['ToShare'] = (to_airport_airline['ToSeats'] / to_total * 100)
    
    # Calculate TO airport HHI
    to_hhi = to_airport_airline.groupby(['TO', 'Year', 'Month'])['ToShare'].apply(
        lambda x: (x ** 2).sum()
    ).reset_index()
    to_hhi.columns = ['TO', 'Year', 'Month', 'AirHHI_To']
    
    # Calculate total flights to TO airport
    to_flights = combined_df.groupby(['TO', 'Year', 'Month']).size().reset_index(name='AirFli_To')
    
    # Merge all metrics
    result = grouped.copy()
    
    # Merge FROM airport metrics
    result = result.merge(
        from_hhi, 
        left_on=['FROM_AIRPORT', 'Year', 'Month'], 
        right_on=['FROM', 'Year', 'Month'], 
        how='left'
    )
    result = result.merge(
        from_flights,
        left_on=['FROM_AIRPORT', 'Year', 'Month'],
        right_on=['FROM', 'Year', 'Month'],
        how='left'
    )
    
    # Merge TO airport metrics
    result = result.merge(
        to_hhi,
        left_on=['TO_AIRPORT', 'Year', 'Month'],
        right_on=['TO', 'Year', 'Month'],
        how='left'
    )
    result = result.merge(
        to_flights,
        left_on=['TO_AIRPORT', 'Year', 'Month'],
        right_on=['TO', 'Year', 'Month'],
        how='left'
    )
    
    # Select and reorder final columns
    final_columns = [
        'Airline', 'ROUTE', 'Year', 'Month', 'Passenger', 'OwnShfli', 
        'RouteHHI', 'AirHHI_From', 'AirHHI_To', 'AirFli_From', 'AirFli_To'
    ]
    
    result = result[final_columns]
    
    # Round numerical columns
    result['Passenger'] = result['Passenger'].round(3)
    result['AirHHI_From'] = result['AirHHI_From'].round(2)
    result['AirHHI_To'] = result['AirHHI_To'].round(2)
    
    # Sort by Year, Month, Airline, Route
    result = result.sort_values(['Year', 'Month', 'Airline', 'ROUTE']).reset_index(drop=True)
    
    # Save to CSV
    result.to_csv(output_file, index=False)
    print(f"\n✓ Consolidated data saved to: {output_file}")
    print(f"  Total rows: {len(result)}")
    print(f"  Columns: {', '.join(result.columns)}")
    
    # Display summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"Unique Airlines: {result['Airline'].nunique()}")
    print(f"Unique Routes: {result['ROUTE'].nunique()}")
    print(f"Date Range: {result['Year'].min()}/{result['Month'].min()} to {result['Year'].max()}/{result['Month'].max()}")
    print(f"Total Passengers: {result['Passenger'].sum():,.0f}")
    print("\nSample output:")
    print(result.head(10).to_string(index=False))
    
    return result


def search_data(df=None, csv_file='consolidated_flight_data.csv', airline=None, route=None, 
                year=None, month=None, export=False, export_file='search_results.csv'):
    """
    Search and filter the consolidated flight data.
    
    Parameters:
    -----------
    df : DataFrame, optional
        DataFrame to search. If None, will load from csv_file
    csv_file : str
        Path to consolidated CSV file (used if df is None)
    airline : str or list, optional
        Airline name(s) to filter (partial match, case-insensitive)
        Examples: 'Cebu', 'Philippine', ['Cebu', 'AirAsia']
    route : str or list, optional
        Route(s) to filter (exact match, case-insensitive)
        Examples: 'MNL-CEB', 'BCD-MNL', ['MNL-CEB', 'CEB-MNL']
    year : int or list, optional
        Year(s) to filter. Examples: 2024, [2023, 2024]
    month : int or list, optional
        Month(s) to filter. Examples: 9, [9, 10, 11]
    export : bool
        If True, export results to CSV
    export_file : str
        Name of export file if export=True
    
    Returns:
    --------
    DataFrame with filtered results
    """
    
    # Load data if DataFrame not provided
    if df is None:
        try:
            df = pd.read_csv(csv_file)
            print(f"✓ Loaded data from: {csv_file}")
        except FileNotFoundError:
            print(f"❌ File not found: {csv_file}")
            print("Please run process_flight_data() first to create the consolidated file.")
            return None
    
    # Make a copy to avoid modifying original
    filtered_df = df.copy()
    
    print(f"\nStarting with {len(filtered_df)} records")
    print("="*60)
    
    # Filter by airline
    if airline:
        if isinstance(airline, str):
            airline = [airline]
        mask = filtered_df['Airline'].str.contains('|'.join(airline), case=False, na=False)
        filtered_df = filtered_df[mask]
        print(f"After airline filter ({', '.join(airline)}): {len(filtered_df)} records")
    
    # Filter by route
    if route:
        if isinstance(route, str):
            route = [route]
        # Convert to uppercase for matching
        route_upper = [r.upper() for r in route]
        mask = filtered_df['ROUTE'].str.upper().isin(route_upper)
        filtered_df = filtered_df[mask]
        print(f"After route filter ({', '.join(route)}): {len(filtered_df)} records")
    
    # Filter by year
    if year:
        if isinstance(year, int):
            year = [year]
        filtered_df = filtered_df[filtered_df['Year'].isin(year)]
        print(f"After year filter ({', '.join(map(str, year))}): {len(filtered_df)} records")
    
    # Filter by month
    if month:
        if isinstance(month, int):
            month = [month]
        filtered_df = filtered_df[filtered_df['Month'].isin(month)]
        print(f"After month filter ({', '.join(map(str, month))}): {len(filtered_df)} records")
    
    print("="*60)
    print(f"\n✓ Final result: {len(filtered_df)} records\n")
    
    if len(filtered_df) == 0:
        print("⚠ No records found matching the criteria")
        return filtered_df
    
    # Display summary
    print("SEARCH RESULTS SUMMARY:")
    print("-"*60)
    print(f"Airlines: {', '.join(filtered_df['Airline'].unique())}")
    print(f"Routes: {', '.join(filtered_df['ROUTE'].unique())}")
    print(f"Date range: {filtered_df['Year'].min()}/{filtered_df['Month'].min()} to {filtered_df['Year'].max()}/{filtered_df['Month'].max()}")
    print(f"Total Passengers: {filtered_df['Passenger'].sum():,.2f}")
    print(f"Average RouteHHI: {filtered_df['RouteHHI'].mean():.2f}")
    
    # Display results
    print("\n" + "="*60)
    print("DETAILED RESULTS:")
    print("="*60)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 50)
    print(filtered_df.to_string(index=False))
    
    # Export if requested
    if export:
        filtered_df.to_csv(export_file, index=False)
        print(f"\n✓ Results exported to: {export_file}")
    
    return filtered_df


def get_route_summary(df=None, csv_file='consolidated_flight_data.csv', route=None):
    """
    Get a detailed summary of competition on a specific route.
    
    Parameters:
    -----------
    df : DataFrame, optional
        DataFrame to analyze
    csv_file : str
        Path to consolidated CSV file (used if df is None)
    route : str
        Route to analyze (e.g., 'MNL-CEB')
    """
    
    # Load data if needed
    if df is None:
        try:
            df = pd.read_csv(csv_file)
        except FileNotFoundError:
            print(f"❌ File not found: {csv_file}")
            return None
    
    if route is None:
        print("❌ Please specify a route (e.g., route='MNL-CEB')")
        return None
    
    # Filter for the route
    route_data = df[df['ROUTE'].str.upper() == route.upper()].copy()
    
    if len(route_data) == 0:
        print(f"❌ No data found for route: {route}")
        print(f"\nAvailable routes: {', '.join(sorted(df['ROUTE'].unique()))}")
        return None
    
    print(f"\n{'='*60}")
    print(f"ROUTE ANALYSIS: {route.upper()}")
    print(f"{'='*60}\n")
    
    # Overall statistics
    print("OVERALL STATISTICS:")
    print(f"  Total records: {len(route_data)}")
    print(f"  Date range: {route_data['Year'].min()}/{route_data['Month'].min()} to {route_data['Year'].max()}/{route_data['Month'].max()}")
    print(f"  Total passengers: {route_data['Passenger'].sum():,.2f}")
    print(f"  Average RouteHHI: {route_data['RouteHHI'].mean():.2f}")
    
    # Market share by airline
    print(f"\n{'='*60}")
    print("MARKET SHARE BY AIRLINE:")
    print(f"{'='*60}")
    airline_summary = route_data.groupby('Airline').agg({
        'Passenger': 'sum',
        'OwnShfli': 'mean',
        'ROUTE': 'count'
    }).reset_index()
    airline_summary.columns = ['Airline', 'Total_Passengers', 'Avg_Market_Share_%', 'Months_Operated']
    airline_summary = airline_summary.sort_values('Total_Passengers', ascending=False)
    print(airline_summary.to_string(index=False))
    
    # Monthly trend
    print(f"\n{'='*60}")
    print("MONTHLY TREND:")
    print(f"{'='*60}")
    monthly = route_data.groupby(['Year', 'Month', 'Airline']).agg({
        'Passenger': 'sum',
        'OwnShfli': 'first',
        'RouteHHI': 'first'
    }).reset_index()
    monthly['YearMonth'] = monthly['Year'].astype(str) + '-' + monthly['Month'].astype(str).str.zfill(2)
    
    # Pivot for easier reading
    pivot = monthly.pivot_table(
        index='YearMonth',
        columns='Airline',
        values='Passenger',
        fill_value=0
    )
    print(pivot.to_string())
    
    return route_data


if __name__ == "__main__":
    # STEP 1: Process all CSV files (run this first)
    result_df = process_flight_data(
        input_pattern='data/*.csv',
        output_file='consolidated_flight_data.csv'
    )
    
    # ============================================================
    # STEP 2: SEARCH EXAMPLES (uncomment to use)
    # ============================================================
    
    # Example 1: Search for specific airline
    # cebu_data = search_data(airline='Cebu Pacific')
    
    # Example 2: Search for specific route
    # mnl_ceb = search_data(route='MNL-CEB')
    
    # Example 3: Search for airline on specific route
    # cebu_mnl_ceb = search_data(airline='Cebu', route='MNL-CEB')
    
    # Example 4: Search multiple airlines
    # major_airlines = search_data(airline=['Cebu', 'Philippine', 'AirAsia'])
    
    # Example 5: Search multiple routes
    # manila_routes = search_data(route=['MNL-CEB', 'MNL-DVO', 'MNL-ILO'])
    
    # Example 6: Search by date range
    # q3_2024 = search_data(year=2024, month=[7, 8, 9])
    
    # Example 7: Complex search with export
    # cebu_q3 = search_data(
    #     airline='Cebu Pacific',
    #     route=['MNL-CEB', 'CEB-MNL'],
    #     year=2024,
    #     month=[7, 8, 9],
    #     export=True,
    #     export_file='cebu_q3_2024.csv'
    # )
    
    # Example 8: Get detailed route analysis
    # route_analysis = get_route_summary(route='MNL-CEB')
    
    # Example 9: Search from existing DataFrame
    # filtered = search_data(df=result_df, airline='Philippine Airlines')
    
    # ============================================================
    # ADDITIONAL UTILITY FUNCTIONS
    # ============================================================
    
    # List all airlines
    # if result_df is not None:
    #     print("\nAvailable Airlines:")
    #     print(sorted(result_df['Airline'].unique()))
    
    # List all routes
    # if result_df is not None:
    #     print("\nAvailable Routes:")
    #     print(sorted(result_df['ROUTE'].unique()))
    
    # List all routes for a specific airline
    # if result_df is not None:
    #     airline_name = 'Cebu Pacific'
    #     routes = result_df[result_df['Airline'].str.contains(airline_name, case=False)]['ROUTE'].unique()
    #     print(f"\nRoutes operated by {airline_name}:")
    #     print(sorted(routes))