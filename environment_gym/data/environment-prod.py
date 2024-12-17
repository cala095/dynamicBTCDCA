import pandas as pd
from datetime import datetime, timezone
import time
import os

def process_last_line(input_file, output_file):
    if not os.path.exists(input_file):
        print("Input file does not exist.")
        return None
    
    df = pd.read_csv(input_file)
    if df.empty:
        print("Input file is empty.")
        return None

    # Get the last line
    last_line = df.tail(1).copy()
    # Rename columns
    column_mapping = {
        'Timestamp': 'Formatted_Time',
        'Open': 'BTC1m_Open',
        'High': 'BTC1m_High',
        'Low': 'BTC1m_Low',
        'Close': 'BTC1m_Close',
        'Volume': 'BTC1m_Volume'
    }
    last_line.rename(columns=column_mapping, inplace=True)
    
    # Convert Unix timestamp to formatted time
    # Assuming 'Formatted_Time' is currently a Unix timestamp
    last_line['Formatted_Time'] = pd.to_datetime(last_line['Formatted_Time'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Save to output_file
    last_line.to_csv(output_file, index=False)
    print(f"Processed last line saved to {output_file}")
    print(last_line)

    # Return the timestamp we just processed (as a string)
    return last_line['Formatted_Time'].iloc[0]

input_file = '../../fetching_data/live/PriceData/BTCUSD_data.csv'
output_file = 'processed_btc_last_line_1m.csv'

last_processed_timestamp = None

while True:
    # Wait until the start of the next minute
    now = datetime.now(timezone.utc)
    sleep_seconds = 60 - now.second
    print(f"Waiting for the next minute... {sleep_seconds}s")
    time.sleep(sleep_seconds + 2)

    # Now attempt to process the last line
    current_timestamp = process_last_line(input_file, output_file)

    # If current_timestamp is None, something went wrong (e.g. no file or empty file).
    # Just wait again and continue.
    if current_timestamp is None:
        continue

    # If we processed a line but it's the same timestamp as last time,
    # we keep retrying every 1 second until new data is available.
    while last_processed_timestamp == current_timestamp:
        print("No new data yet. Retrying in 1 second...")
        time.sleep(1)
        current_timestamp = process_last_line(input_file, output_file)
        if current_timestamp is None:
            # If file got empty or something unexpected happened, break and retry next minute
            break

    # Update last_processed_timestamp
    last_processed_timestamp = current_timestamp
