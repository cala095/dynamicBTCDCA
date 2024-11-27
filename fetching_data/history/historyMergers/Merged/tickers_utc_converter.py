import pandas as pd
import os
from pytz import timezone

# Define the tickers and their corresponding filenames and timezones
tickers = {
    'DXY': {'filename': 'Merged_DXY.csv', 'timezone': 'America/Chicago'},
    'GOLD': {'filename': 'Merged_GOLD.csv', 'timezone': 'America/Chicago'},
    'NDQ': {'filename': 'Merged_NDQ.csv', 'timezone': 'America/Chicago'},
    'SPX': {'filename': 'Merged_SPX.csv', 'timezone': 'America/Chicago'},
    'US02Y': {'filename': 'Merged_US02Y.csv', 'timezone': 'America/New_York'},
    'US10Y': {'filename': 'Merged_US10Y.csv', 'timezone': 'America/New_York'},
    'VIX': {'filename': 'Merged_VIX.csv', 'timezone': 'America/Chicago'}
}

# Directory containing the CSV files
input_dir = ''  # Replace with your input directory
output_dir = 'timezoneConverted_data'  # Replace with your output directory

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to convert datetime to UTC
def convert_to_utc(df, original_tz):
    # Convert 'datetime' column to datetime objects
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    
    # Check if the datetime objects are already timezone-aware
    if df['datetime'].dt.tz is not None:
        # If they are, convert directly to UTC
        df['datetime'] = df['datetime'].dt.tz_convert('UTC')
    else:
        # Localize the datetime to the original timezone, handling ambiguous and nonexistent times
        df['datetime'] = df['datetime'].dt.tz_localize(original_tz, ambiguous='NaT', nonexistent='shift_forward')
        df['datetime'] = df['datetime'].dt.tz_convert('UTC')
    
    # Remove rows with NaT in 'datetime' column
    df = df.dropna(subset=['datetime'])
    
    return df

# Process each ticker
for ticker, data in tickers.items():
    filename = data['filename']
    original_tz = timezone(data['timezone'])
    
    # Full path to the input file
    input_path = os.path.join(input_dir, filename)
    
    # Check if the file exists
    if not os.path.isfile(input_path):
        print(f"File not found: {input_path}")
        continue
    
    # Read the CSV file
    df = pd.read_csv(input_path)
    
    # Convert datetime to UTC
    df = convert_to_utc(df, original_tz)

     # Format datetime to remove timezone information
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Full path to the output file
    output_path = os.path.join(output_dir, f"{ticker}_UTC.csv")
    
    # Save the modified dataframe to a new CSV file
    df.to_csv(output_path, index=False)
    
    print(f"Processed {ticker} and saved to {output_path}")