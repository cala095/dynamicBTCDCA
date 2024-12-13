import pandas as pd
import os
from datetime import datetime
import traceback

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
    # **2.1 Deleting Live except last 10 lines ** -> ITS BETTER IF THIS GETS DONE ONCE THE PROCESSER IS DONE -> OTHERWISE YOU LOSE THE NEW EMAIL
    print("Deleting useless live data...") #TODO kinda risky doing this here -> should be better doing it at the end (but i dont want to lose time to avoid losing email/btcrequests)
    last_1000_lines = live_df.tail(1000) #1000 is enough the processer will not start untill we reach the waiting state for the mail
    last_1000_lines.to_csv(live_file, index=False)

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
        live_df = pd.read_csv('../../live/PriceData/BTCUSD_data.csv')
        
        print("Cleaning up live data (keeping only last 10 lines)...")
        last_10_lines = live_df.tail(120)
        last_10_lines.to_csv('../../live/PriceData/BTCUSD_data.csv', index=False)
        
        # Convert 'Timestamp' to datetime (UTC)
        print("** Converting Unix time to datetime (UTC) **")
        live_df['Datetime'] = pd.to_datetime(live_df['Timestamp'], unit='s', utc=True)
        
        # Remove duplicates based on Timestamp by keeping the entry with the highest Volume
        print("** Removing duplicates based on Timestamp **")
        duplicates = live_df.duplicated(subset='Timestamp', keep=False)
        num_duplicates = duplicates.sum()
        if num_duplicates > 0:
            duplicated_records = live_df[duplicates]
            duplicated_records.to_csv('duplicated_records.csv', index=False)
            print(f"Found {num_duplicates} duplicate entries. Duplicates saved in duplicated_records.csv")

            # For each duplicate group, keep only the row with max volume
            new_live_rows = []
            for timestamp, group in duplicated_records.groupby('Timestamp'):
                max_volume_row = group.loc[group['Volume'].idxmax()]
                new_live_rows.append(max_volume_row)
            
            # Remove all duplicates from main df and add back only the max volume rows
            live_df = live_df[~live_df['Timestamp'].isin(duplicated_records['Timestamp'])]
            live_df = pd.concat([live_df, pd.DataFrame(new_live_rows)], ignore_index=True)
            print("Duplicates have been resolved.")
        else:
            print("No duplicates found.")

        # Sort live data by Datetime
        live_df.sort_values(by='Datetime', inplace=True)

        # Create Formatted_Time column
        live_df['Formatted_Time'] = live_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        print("Loading existing processed data...")
        processed_file = 'Processed_BTC.csv'
        processed_df = pd.read_csv(processed_file, parse_dates=['Formatted_Time'], 
                                   date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S', utc=True))

        # Sort processed data by time
        processed_df.sort_values(by='Formatted_Time', inplace=True)

        # Remove from live_df any rows that are already in processed_df (based on Timestamp)
        print("** Filtering out live data that are already in the processed file **")
        processed_timestamps = processed_df['Timestamp'].unique()
        live_df = live_df[~live_df['Timestamp'].isin(processed_timestamps)]

        # If no new data, just exit
        if live_df.empty:
            print("No new live data to append. Exiting.")
            return

        # Identify the range for missing minutes
        print("** Checking for missing minutes **")
        last_processed_time = processed_df['Formatted_Time'].max()
        last_processed_time_utc = pd.to_datetime(last_processed_time, utc=True)
        max_live_time_utc = pd.to_datetime(live_df['Datetime'].max(), utc=True)

        # Create a complete range from last processed to last live time
        complete_range = pd.date_range(start=last_processed_time_utc, end=max_live_time_utc, freq='T', tz='UTC')

        # Existing timestamps (from merged perspective)
        # Note: Live_df and processed_df might have overlapping intervals, but we already filtered duplicates.
        all_existing_times = pd.to_datetime(pd.concat([processed_df['Formatted_Time'], live_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')]).unique(), utc=True)
        
        # Find missing minutes
        missing_minutes = complete_range.difference(all_existing_times)

        if len(missing_minutes) > 0:
            print(f"Found {len(missing_minutes)} missing minutes. They will be appended as blank rows.")

            # Create a DataFrame for missing minutes
            # These rows will have only the Formatted_Time
            missing_df = pd.DataFrame(missing_minutes, columns=['Formatted_Time'])
            
            # Check if the file exists
            file_exists = os.path.isfile('missing_minutes.csv')
            # Append the DataFrame to the CSV file
            missing_df.to_csv('missing_minutes.csv', mode='a', index=False, header=not file_exists)
        else:
            print('no missing minute found')

        # Convert to naive datetime (drop UTC timezone) before formatting
        live_df['Formatted_Time'] = (
            live_df['Datetime']
            .dt.tz_localize(None)   # Remove timezone awareness, leaving a naive datetime
            .dt.strftime('%Y-%m-%d %H:%M:%S')
        )
        processed_df['Formatted_Time'] = processed_df['Formatted_Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Reorder live_df columns
        desired_cols = ['Formatted_Time','Timestamp','Open','High','Low','Close','Volume']
        live_df = live_df[desired_cols]

        # Merge processed_df, missing_df, and live_df
        print("Merging processed data and new live data...")
        merged_df = pd.concat([processed_df, live_df], ignore_index=True)
        
        # Remove duplicates by Formatted_Time just in case
        merged_df.drop_duplicates(subset='Formatted_Time', keep='last', inplace=True)

        # Sort by Timestamp
        merged_df.sort_values(by='Timestamp', inplace=True)
        

        # Save the final processed data
        print("Saving the merged and processed BTC data to Processed_BTC.csv")
        merged_df.to_csv('Processed_BTC.csv', index=False)
        print("** Processed_BTC.csv Saved **")

    except Exception as e:
        print(f"Error transforming BTC data: {str(e)}")
        print("\nComplete stack trace:")
        print(traceback.format_exc())
        return -1



if __name__ == "__main__":
    # BTC data
    copy_btc_data()

    # tickers
    tickers = ['DXY', 'GOLD', 'NDQ', 'US02Y', 'US10Y', 'VIX', 'SPX']
    for ticker in tickers:
        processed_file_path = f'Processed_{ticker}.csv'
        live_file_path = f'../../live/PriceData/{ticker}_data.csv'
        update_processed_data(ticker, processed_file_path, live_file_path)

