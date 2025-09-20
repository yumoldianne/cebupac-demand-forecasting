import pandas as pd
import numpy as np
from datetime import datetime
import re
from collections import defaultdict
import math

# Aircraft passenger capacity mapping
AIRCRAFT_CAPACITY = {
    '32A': 186, '32B': 230, '32N': 186, '32Q': 230, '33Y': 440, '73F': 130,
    '73H': 189, '74Y': 416, '75F': 200, '77L': 313, '77W': 396, '7M1': 123,
    '07M': 82, 'A20N': 186, 'A21N': 230, 'A320': 186, 'A321': 230, 'A332': 406,
    'A333': 440, 'A339': 465, 'A359': 440, 'AT7': 72, 'AT4': 650, 'AT7': 566,
    'AT7': 672, 'ATR': 42, 'B38M': 200, 'B733': 140, 'B738': 189, 'B744': 416,
    'B752': 200, 'B772': 313, 'B773': 396, 'B77W': 396, 'B788': 359, 'B789': 406,
    'B78X': 440, 'DH8': 68, 'DH8D': 68, '320': 186, '321': 230, 'DH8': 68
}

def clean_aircraft_type(aircraft):
    """Clean and standardize aircraft type"""
    if pd.isna(aircraft) or aircraft == '—':
        return None
    
    # Remove registration numbers in parentheses
    aircraft = re.sub(r'\s*\([^)]*\)', '', str(aircraft))
    aircraft = aircraft.strip()
    
    # Handle special cases
    if aircraft == '320':
        return 'A320'
    elif aircraft == '321':
        return 'A321'
    elif aircraft.startswith('DH8'):
        return 'DH8'
    
    return aircraft

def get_passenger_capacity(aircraft_type):
    """Get passenger capacity for aircraft type"""
    if not aircraft_type:
        return 0
    
    clean_type = clean_aircraft_type(aircraft_type)
    return AIRCRAFT_CAPACITY.get(clean_type, 186)  # Default to A320 capacity

def parse_date(date_str):
    """Parse date string to datetime"""
    try:
        # Handle different date formats
        if isinstance(date_str, str):
            # Try different formats
            for fmt in ['%m/%d/%Y', '%d-%b-%y', '%Y-%m-%d']:
                try:
                    return pd.to_datetime(date_str, format=fmt)
                except:
                    continue
        return pd.to_datetime(date_str)
    except:
        return None

def calculate_route_distance(from_airport, to_airport):
    """
    Calculate approximate distance between airports
    This is a simplified version - in practice, you'd use actual airport coordinates
    """
    # Simplified distance mapping (in km) - you should replace with actual coordinates
    distance_map = {
        ('MNL', 'CEB'): 630,
        ('CRK', 'BSO'): 580,
        ('MNL', 'PAG'): 950,
        # Add more routes as needed
    }
    
    route = (from_airport, to_airport)
    reverse_route = (to_airport, from_airport)
    
    return distance_map.get(route, distance_map.get(reverse_route, 800))  # Default distance

def process_flight_data(df, start_date='2024-07-01', end_date='2025-06-30'):
    """Process flight data and calculate variables"""
    
    # Convert dates
    df['DATE'] = df['DATE'].apply(parse_date)
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Filter data for the specified period
    df_filtered = df[(df['DATE'] >= start_dt) & (df['DATE'] <= end_dt)].copy()
    
    # Clean FROM and TO airport codes
    df_filtered['FROM_CLEAN'] = df_filtered['FROM'].str.extract(r'\(([A-Z]{3})\)')[0]
    df_filtered['TO_CLEAN'] = df_filtered['TO'].str.extract(r'\(([A-Z]{3})\)')[0]
    
    # Create route identifier
    df_filtered['ROUTE'] = df_filtered['FROM_CLEAN'] + '-' + df_filtered['TO_CLEAN']
    
    # Clean aircraft type and get capacity
    df_filtered['AIRCRAFT_CLEAN'] = df_filtered['AIRCRAFT'].apply(clean_aircraft_type)
    df_filtered['PASSENGER_CAPACITY'] = df_filtered['AIRCRAFT_CLEAN'].apply(get_passenger_capacity)
    
    # Only include landed/completed flights for passenger calculations
    completed_flights = df_filtered[df_filtered['STATUS'].str.contains('Landed', na=False)]
    
    return df_filtered, completed_flights

def calculate_route_characteristics(completed_flights):
    """Calculate route-level characteristics"""
    route_stats = {}
    
    for route, group in completed_flights.groupby('ROUTE'):
        from_airport = group['FROM_CLEAN'].iloc[0]
        to_airport = group['TO_CLEAN'].iloc[0]
        
        # Distance
        dist = calculate_route_distance(from_airport, to_airport)
        
        # Total passengers (sum of all flight capacities on this route)
        passengers = group['PASSENGER_CAPACITY'].sum()
        
        route_stats[route] = {
            'Dist': dist,
            'Passenger': passengers,
            'FROM': from_airport,
            'TO': to_airport
        }
    
    return route_stats

def calculate_airport_characteristics(df_all, completed_flights):
    """Calculate airport-level characteristics"""
    airport_stats = {}
    
    # Count all flights (scheduled and completed) for each airport
    all_airports = set()
    all_airports.update(df_all['FROM_CLEAN'].dropna())
    all_airports.update(df_all['TO_CLEAN'].dropna())
    
    for airport in all_airports:
        # Count flights where airport is either origin or destination
        flights_from = df_all[df_all['FROM_CLEAN'] == airport].shape[0]
        flights_to = df_all[df_all['TO_CLEAN'] == airport].shape[0]
        total_flights = flights_from + flights_to
        
        airport_stats[airport] = {
            'AirFli': total_flights
        }
    
    return airport_stats

def calculate_airline_characteristics(df_all, completed_flights):
    """Calculate airline-level characteristics"""
    airline_stats = {}
    
    # Get all unique airlines
    airlines = df_all['AIRLINE'].unique()
    
    for airline in airlines:
        airline_flights = df_all[df_all['AIRLINE'] == airline]
        
        # Get airports served by this airline
        airports_from = set(airline_flights['FROM_CLEAN'].dropna())
        airports_to = set(airline_flights['TO_CLEAN'].dropna())
        all_airports_served = airports_from.union(airports_to)
        
        # Calculate routes served by this airline
        routes_served = set(airline_flights['ROUTE'].dropna())
        
        # Calculate total seats flown (for completed flights only)
        completed_airline_flights = completed_flights[completed_flights['AIRLINE'] == airline]
        total_seats = completed_airline_flights['PASSENGER_CAPACITY'].sum()
        
        airline_stats[airline] = {
            'airports_served': all_airports_served,
            'routes_served': routes_served,
            'total_seats': total_seats
        }
    
    return airline_stats

def calculate_hhi(market_shares):
    """Calculate Herfindahl-Hirschman Index"""
    # Handle empty or None inputs
    if market_shares is None:
        return 0
    
    # Convert to numpy array for consistent handling
    if hasattr(market_shares, 'values'):  # pandas Series
        shares_array = market_shares.values
    else:
        shares_array = np.array(market_shares)
    
    # Check if empty
    if len(shares_array) == 0:
        return 0
    
    # Convert to percentages if not already
    total = np.sum(shares_array)
    if total == 0:
        return 0
    
    normalized_shares = (shares_array / total) * 100
    hhi = np.sum(normalized_shares ** 2)
    return hhi

def calculate_competitor_characteristics(completed_flights, airline_stats):
    """Calculate competition-related characteristics"""
    route_hhi = {}
    airport_hhi = {}
    
    # Route HHI - market concentration per route
    for route, group in completed_flights.groupby('ROUTE'):
        airline_seats = group.groupby('AIRLINE')['PASSENGER_CAPACITY'].sum()
        route_hhi[route] = calculate_hhi(airline_seats.values)
    
    # Airport HHI - market concentration per airport
    airports = set()
    airports.update(completed_flights['FROM_CLEAN'].dropna())
    airports.update(completed_flights['TO_CLEAN'].dropna())
    
    for airport in airports:
        # Get all flights involving this airport
        airport_flights = completed_flights[
            (completed_flights['FROM_CLEAN'] == airport) | 
            (completed_flights['TO_CLEAN'] == airport)
        ]
        
        if not airport_flights.empty:
            airline_seats = airport_flights.groupby('AIRLINE')['PASSENGER_CAPACITY'].sum()
            airport_hhi[airport] = calculate_hhi(airline_seats.values)
        else:
            airport_hhi[airport] = 0
    
    return route_hhi, airport_hhi

def calculate_final_variables(df_all, route_stats, airport_stats, airline_stats, route_hhi, airport_hhi):
    """Calculate final variables for each route-airline combination"""
    results = []
    
    # Get total seats across all completed flights for OwnShfli calculation
    total_seats_all = sum(stats['total_seats'] for stats in airline_stats.values())
    
    for route in route_stats:
        route_info = route_stats[route]
        from_airport = route_info['FROM']
        to_airport = route_info['TO']
        
        # Get airlines that serve this route
        route_flights = df_all[df_all['ROUTE'] == route]
        route_airlines = route_flights['AIRLINE'].unique()
        
        for airline in route_airlines:
            # SerBothEnds - check if airline serves both endpoints
            airline_info = airline_stats.get(airline, {})
            airports_served = airline_info.get('airports_served', set())
            ser_both_ends = 1 if (from_airport in airports_served and to_airport in airports_served) else 0
            
            # OwnShfli - proportion of total seats
            airline_seats = airline_info.get('total_seats', 0)
            own_shfli = (airline_seats / total_seats_all * 100) if total_seats_all > 0 else 0
            
            result = {
                'Route': route,
                'Airline': airline,
                'Dist': route_info['Dist'],
                'Passenger': route_info['Passenger'],
                'AirFli_From': airport_stats.get(from_airport, {}).get('AirFli', 0),
                'AirFli_To': airport_stats.get(to_airport, {}).get('AirFli', 0),
                'SerBothEnds': ser_both_ends,
                'OwnShfli': round(own_shfli, 2),
                'RouteHHI': round(route_hhi.get(route, 0), 2),
                'AirHHI_From': round(airport_hhi.get(from_airport, 0), 2),
                'AirHHI_To': round(airport_hhi.get(to_airport, 0), 2)
            }
            
            results.append(result)
    
    return pd.DataFrame(results)

# Main processing function
def analyze_flight_data(dataframes, start_date='2024-07-01', end_date='2025-06-30'):
    """
    Main function to analyze flight data and calculate all variables
    
    Parameters:
    dataframes: list of pandas DataFrames or single DataFrame
    start_date: analysis start date
    end_date: analysis end date
    
    Returns:
    DataFrame with calculated variables
    """
    
    # Combine all dataframes if multiple provided
    if isinstance(dataframes, list):
        df_combined = pd.concat(dataframes, ignore_index=True)
    else:
        df_combined = dataframes.copy()
    
    print("Processing flight data...")
    df_all, completed_flights = process_flight_data(df_combined, start_date, end_date)
    
    print("Calculating route characteristics...")
    route_stats = calculate_route_characteristics(completed_flights)
    
    print("Calculating airport characteristics...")
    airport_stats = calculate_airport_characteristics(df_all, completed_flights)
    
    print("Calculating airline characteristics...")
    airline_stats = calculate_airline_characteristics(df_all, completed_flights)
    
    print("Calculating competitor characteristics...")
    route_hhi, airport_hhi = calculate_competitor_characteristics(completed_flights, airline_stats)
    
    print("Calculating final variables...")
    results_df = calculate_final_variables(
        completed_flights, route_stats, airport_stats, airline_stats, route_hhi, airport_hhi
    )
    
    return results_df

def load_csv_with_encoding(filepath):
    """
    Load CSV file with proper encoding handling
    """
    encodings_to_try = ['utf-8', 'latin-1', 'windows-1252', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings_to_try:
        try:
            print(f"Trying to read {filepath} with {encoding} encoding...")
            df = pd.read_csv(filepath, encoding=encoding)
            print(f"Successfully loaded {filepath} with {encoding} encoding")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error with {encoding}: {e}")
            continue
    
    # If all encodings fail, try with error handling
    try:
        print(f"Trying {filepath} with utf-8 and error handling...")
        df = pd.read_csv(filepath, encoding='utf-8', encoding_errors='replace')
        print(f"Loaded {filepath} with error replacement")
        return df
    except Exception as e:
        print(f"Failed to load {filepath}: {e}")
        return None

# Example usage:
"""
# Method 1: Use the encoding helper function
df_ceb = load_csv_with_encoding('fr24_CEB.csv')
df_bso = load_csv_with_encoding('fr24_BSO.csv') 
df_pag = load_csv_with_encoding('fr24_PAG.csv')
"""
# Method 2: Specify encoding directly (try these one by one)
df_ceb = pd.read_csv('small-scale/fr24_CEB.csv', encoding='latin-1')
df_bso = pd.read_csv('small-scale/fr24_BSO.csv', encoding='latin-1')
df_pag = pd.read_csv('small-scale/fr24_PAG.csv', encoding='latin-1')

# Method 3: If you know the specific encoding
# df_ceb = pd.read_csv('fr24_CEB.csv', encoding='windows-1252')

# Method 4: Use error handling to replace problematic characters
# df_ceb = pd.read_csv('fr24_CEB.csv', encoding='utf-8', encoding_errors='replace')

# Filter out None values if any files failed to load
dataframes = [df for df in [df_ceb, df_bso, df_pag] if df is not None]

if dataframes:
    # Analyze the data
    results = analyze_flight_data(dataframes, '2024-07-01', '2025-06-30')
    
    # Display results
    print(results.head())
    print(f"\nResults shape: {results.shape}")
    print("\nColumn descriptions:")
    print("Dist: Route distance in km")
    print("Passenger: Total passengers on route in period")
    print("AirFli_From/To: Number of flights at origin/destination airports")
    print("SerBothEnds: 1 if airline serves both endpoints, 0 otherwise")
    print("OwnShfli: Airline's percentage of total market seats")
    print("RouteHHI: Route market concentration index")
    print("AirHHI_From/To: Airport market concentration index")
    
    # Save results
    results.to_csv('flight_analysis_results.csv', index=False)
    print("\nResults saved to 'flight_analysis_results.csv'")
else:
    print("No files could be loaded successfully!")

"""
# Alternative: If you want to clean the data after loading
def clean_dataframe_encoding(df):
    '''Clean encoding issues in dataframe'''
    for column in df.select_dtypes(include=['object']).columns:
        df[column] = df[column].astype(str).str.encode('utf-8', errors='ignore').str.decode('utf-8')
    return df
"""