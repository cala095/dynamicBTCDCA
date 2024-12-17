import pandas as pd
from datetime import datetime, timezone
import time

def process_last_line(input_file, output_file):
    # Read the last line of the CSV file
    df = pd.read_csv(input_file)
    df = df.tail(1)
    print(df)    
    # Rename the columns
    column_mapping = {
        'Timestamp': 'Formatted_Time',
        'Open': 'BTC1m_Open',
        'High': 'BTC1m_High',
        'Low': 'BTC1m_Low',
        'Close': 'BTC1m_Close',
        'Volume': 'BTC1m_Volume'
    }
    df = df.rename(columns=column_mapping)
    
    # Convert Unix timestamp to yyyy-mm-dd hh:mm:ss format
    df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Save the processed data to a new CSV file
    df.to_csv(output_file, index=False)
    
    print(f"Processed last line saved to {output_file}")
    print(df)


# Example usage
input_file = '../../fetching_data/live/PriceData/BTCUSD_data.csv'
output_file = 'processed_btc_last_line_1m.csv'
while True:
    process_last_line(input_file, output_file)
    now = datetime.now(timezone.utc)
    sleep_seconds = 60 - now.second
    print(f"Waiting for the next minute... {sleep_seconds}s")
    time.sleep(sleep_seconds + 2)
