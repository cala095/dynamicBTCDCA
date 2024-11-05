import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

def get_last_timestamp(csv_file):
    """
    Reads the CSV file and returns the last valid timestamp in unix format (int).
    """
    df = pd.read_csv(csv_file)
    # Ensure that 'Timestamp' column exists and is numeric
    if 'Timestamp' in df.columns:
        # Drop rows where Timestamp is NaN
        df = df.dropna(subset=['Timestamp'])
        # Convert Timestamp to int
        df['Timestamp'] = df['Timestamp'].astype(float).astype(int)
        if not df.empty:
            last_timestamp = df['Timestamp'].max()
            return last_timestamp
    return None  # Return None if no valid timestamp found

def download_missing_data(api_key, last_timestamp):
    """
    Downloads missing BTC data from last_timestamp to now.
    """
    base_url = 'https://min-api.cryptocompare.com/data/v2/histominute'

    # Subtract 2 minutes from current time to ensure data is available
    end_timestamp = int((datetime.now() - timedelta(minutes=2)).timestamp())

    all_data = []

    toTs = end_timestamp
    last_timestamp += 60  # Start from the next minute after last_timestamp

    while toTs >= last_timestamp:
        # Calculate the number of data points to fetch
        minutes_to_fetch = int((toTs - last_timestamp) / 60) + 1
        if minutes_to_fetch <= 0:
            break

        # Since limit = number of data points - 1, we need to set limit accordingly
        limit = min(2000, minutes_to_fetch) - 1
        if limit < 1:
            limit = 1

        params = {
            'api_key': api_key,
            'fsym': 'BTC',
            'tsym': 'USD',
            'limit': limit,
            'toTs': toTs,
            'aggregate': 1
        }

        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'Data' in data and 'Data' in data['Data']:
                    df = pd.DataFrame(data['Data']['Data'])
                    all_data.append(df)
                    print(f"Downloaded data up to {datetime.fromtimestamp(toTs)}")
                else:
                    print("No data found in response.")
                    break

                # Move backward in time
                toTs -= (limit + 1) * 60  # (limit + 1) because limit + 1 data points were fetched
            else:
                print(f"Error: Status code {response.status_code}")
                print(f"Response: {response.text}")
                break  # Exit the loop on error

            # Respect API rate limits
            time.sleep(1.5)

        except Exception as e:
            print(f"Error downloading data: {e}")
            time.sleep(60)  # Wait longer if error occurs

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        # Remove duplicate timestamps
        combined_df.drop_duplicates(subset=['time'], inplace=True)
        # Filter out any data with time less than or equal to last_timestamp - 60
        combined_df = combined_df[combined_df['time'] >= last_timestamp]
        # Sort by time ascending
        combined_df.sort_values('time', inplace=True)
        return combined_df
    else:
        return None

def fetch_latest_data(api_key):
    """
    Fetches the latest 1-minute BTC data point.
    """
    base_url = 'https://min-api.cryptocompare.com/data/v2/histominute'
    params = {
        'api_key': api_key,
        'fsym': 'BTC',
        'tsym': 'USD',
        'limit': 1,
        'aggregate': 1
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'Data' in data and 'Data' in data['Data']:
                df = pd.DataFrame(data['Data']['Data'])
                return df
            else:
                print("No data found in response.")
                return None
        else:
            print(f"Error: Status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching latest data: {e}")
        return None

def main(csv_file, api_key):
    try:
        if not os.path.exists(csv_file):
            print(f"CSV file {csv_file} does not exist.")
            return

        # Read CSV file once to get last_timestamp
        last_timestamp = get_last_timestamp(csv_file)
        if last_timestamp is None:
            print("No valid timestamp found in CSV file.")
            return

        print(f"Starting from last timestamp: {datetime.fromtimestamp(last_timestamp)}")

        # Check if there is missing data between last_timestamp and now
        current_timestamp = int((datetime.now() - timedelta(minutes=2)).timestamp())
        if current_timestamp > last_timestamp + 60:
            print("Downloading missing data...")
            new_data = download_missing_data(api_key, last_timestamp)
            if new_data is not None and not new_data.empty:
                # Process new data
                new_data['Timestamp'] = new_data['time']
                new_data.rename(columns={
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volumefrom': 'Volume'
                }, inplace=True)
                new_data = new_data[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]

                # Append new data to CSV file
                new_data.to_csv(csv_file, mode='a', header=False, index=False)

                # Update last_timestamp
                last_timestamp = new_data['Timestamp'].max()
                print(f"Appended {len(new_data)} new records to {csv_file}")

            else:
                print("No new data downloaded.")

        else:
            print("No missing data between last timestamp and now.")

        while True:
            try:
                new_data = fetch_latest_data(api_key)
                if new_data is not None and not new_data.empty:
                    # Check if new_data timestamp is greater than last_timestamp
                    new_timestamp = new_data['time'].iloc[-1]
                    if new_timestamp > last_timestamp:
                        # Process new data
                        new_data['Timestamp'] = new_data['time']
                        new_data.rename(columns={
                            'open': 'Open',
                            'high': 'High',
                            'low': 'Low',
                            'close': 'Close',
                            'volumefrom': 'Volume'
                        }, inplace=True)
                        new_data = new_data[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]

                        # Append new data to CSV file
                        new_data.to_csv(csv_file, mode='a', header=False, index=False)

                        # Update last_timestamp
                        last_timestamp = new_timestamp
                        print(f"Appended new record to {csv_file} at {datetime.fromtimestamp(new_timestamp)}")
                    else:
                        print("No new data available yet.")
                else:
                    print("No new data downloaded.")

                # Wait until the start of the next minute
                print("Waiting for the next minute...")
                now = datetime.now()
                sleep_seconds = 60 - now.second
                time.sleep(sleep_seconds)

            except KeyboardInterrupt:
                print("Script terminated by user.")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                print("Waiting for 60 seconds before retrying...")
                time.sleep(60)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    api_key = "freezalando2 key"  # Replace with your actual API key
    csv_file = "F:\\backup\\BTCDCA_DATA\\bitcoin 1 minute 2012 raw\\btcusd_1-min_data.csv"  # Path to your CSV file
    print("Starting BTC data update script...")
    main(csv_file, api_key)
