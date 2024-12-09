import pandas as pd
import os
from datetime import datetime

def update_processed_data(ticker_name, processed_file, live_file):
    print(f"**Updating {ticker_name} Data**")

    # **1. Load Existing Processed Data**
    print("Loading existing processed data...")
    processed_df = pd.read_csv(processed_file, parse_dates=['datetime'])

    # Check if 'Volume' column exists in processed data
    volume_in_processed = 'Volume' in processed_df.columns

    # **2. Load New Live Data**
    print("Loading new live data...")
    live_df = pd.read_csv(live_file)

    # **3. Preprocess Live Data**
    print("Preprocessing live data...")
    live_df['datetime'] = pd.to_datetime(live_df['timestamp'], format='%Y-%m-%d %H:%M')

    # Check if 'volume' column exists and has any non-zero values
    if 'volume' in live_df.columns and (live_df['volume'] != 0).any():
        volume_in_live = True
        # Rename columns for consistency
        live_df.rename(columns={'price': 'Price', 'volume': 'Volume'}, inplace=True)
        live_df = live_df[['datetime', 'Price', 'Volume']]
    else:
        volume_in_live = False
        live_df.rename(columns={'price': 'Price'}, inplace=True)
        live_df = live_df[['datetime', 'Price']]

    # **4. Determine if 'Volume' Should Be Included**
    volume_present = volume_in_processed or volume_in_live

    # **5. Merge Data**
    print("Merging data...")
    # If 'Volume' is not present in both, ensure consistency
    if not volume_present and 'Volume' in processed_df.columns:
        processed_df.drop(columns=['Volume'], inplace=True)

    # Concatenate the dataframes
    merged_df = pd.concat([processed_df, live_df], ignore_index=True)

    # Remove duplicates based on 'datetime'
    merged_df.drop_duplicates(subset='datetime', keep='last', inplace=True)

    # Sort by 'datetime'
    merged_df.sort_values(by='datetime', inplace=True)

    # **6. Forward Fill Missing Data**
    print("Forward filling missing data...")
    merged_df.set_index('datetime', inplace=True)

    # Create a complete datetime index from start to end at 1-minute frequency
    all_minutes = pd.date_range(
        start=merged_df.index.min(),
        end=merged_df.index.max(),
        freq='T'
    )

    merged_df = merged_df.reindex(all_minutes)

    # Forward fill 'Price' values
    merged_df['Price'] = merged_df['Price'].ffill()

    # Forward fill 'Volume' if present
    if volume_present and 'Volume' in merged_df.columns:
        merged_df['Volume'] = merged_df['Volume'].ffill()
    else:
        # Drop 'Volume' column if not needed
        if 'Volume' in merged_df.columns:
            merged_df.drop(columns=['Volume'], inplace=True)

    # Reset index to make 'datetime' a column again
    merged_df.reset_index(inplace=True)
    merged_df.rename(columns={'index': 'datetime'}, inplace=True)

    # **7. Save the Updated Data**
    print("Saving updated data...")
    merged_df.to_csv(processed_file, index=False)

    print(f"Data for {ticker_name} updated successfully! The file '{processed_file}' has been overwritten with the latest data.\n")


def copy_btc_data():
    try:
        print("** Reading BTCUSD_data.csv **")
        df = pd.read_csv('../../live/PriceData/BTCUSD_data.csv')
        
        print("** Convert Unix time to formatted datetime **")
        # Convert 'Timestamp' to datetime assuming it's in seconds and in UTC
        df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='s', utc=True)
        
        # Remove duplicates based on 'Timestamp'
        print("** Removing duplicates based on Timestamp **")
        duplicates = df.duplicated(subset='Timestamp', keep=False)
        num_duplicates = duplicates.sum()
        if num_duplicates > 0:
            # Create a DataFrame of duplicated records
            duplicated_records = df[duplicates]
            # Save duplicated records to a CSV file
            duplicated_records.to_csv('duplicated_records.csv', index=False)
            print(f"Found {num_duplicates} duplicate entries")
            print("Duplicated records have been saved to duplicated_records.csv")

            # Group by Timestamp and compare volumes
            for timestamp, group in duplicated_records.groupby('Timestamp'):
                # print(f"\nDuplicates for {timestamp}:")
                # print(group)
                # Keep row with highest volume
                max_volume_row = group.loc[group['Volume'].idxmax()]
            
                # Remove all rows with this timestamp and add back the one with max volume
                df = df[df['Timestamp'] != timestamp]
                df = pd.concat([df, pd.DataFrame([max_volume_row])], ignore_index=True)
            print("Duplicated records have been removed.csv")

        else:
            print("No duplicates found.")
        
        # Ensure the data is sorted by timestamp
        df.sort_values(by='Datetime', inplace=True)
        
        # Check for missing minutes
        print("** Checking for missing minutes **")
        start = df['Datetime'].min()
        end = df['Datetime'].max()
        complete_range = pd.date_range(start=start, end=end, freq='T', tz='UTC')
        existing_timestamps = pd.to_datetime(df['Datetime'])
        missing_minutes = complete_range[~complete_range.isin(existing_timestamps)]
        
        if len(missing_minutes) > 0:
            print(f"Found {len(missing_minutes)} missing minutes.")
            # Convert missing_minutes to a DataFrame
            missing_df = pd.DataFrame(missing_minutes, columns=['Missing_Time'])
            # Save to a CSV file
            missing_df.to_csv('missing_minutes.csv', index=False)
            print("Missing minutes have been saved to missing_minutes.csv")
        else:
            print("No missing minutes found.")
        
        # Convert 'Datetime' to formatted string if needed
        df['Formatted_Time'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Reorder columns to put Formatted_Time at the beginning
        cols = ['Formatted_Time'] + [col for col in df.columns if col != 'Formatted_Time' and col != 'Datetime']
        df = df[cols]
        
        # Save the processed data to a new CSV file
        print("** trying to save BTCUSD to a new CSV file **")
        df.to_csv('Processed_BTC.csv', index=False)
        print("** Processed_BTC.csv Saved **")
    except Exception as e:
        print(f"Error transforming BTC data: {str(e)}\n")

if __name__ == "__main__":
    # BTC data
    copy_btc_data()

    # tickers
    tickers = ['DXY', 'GOLD', 'NDQ', 'US02Y', 'US10Y', 'VIX', 'SPX']
    for ticker in tickers:
        processed_file_path = f'Processed_{ticker}.csv'
        live_file_path = f'../../live/PriceData/{ticker}_data.csv'
        update_processed_data(ticker, processed_file_path, live_file_path)

