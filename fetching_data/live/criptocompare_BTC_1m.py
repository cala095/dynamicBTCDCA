import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
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
    end_timestamp = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp())

    all_data = []

    toTs = end_timestamp
    fromTs = last_timestamp + 60  # Start from the next minute after last_timestamp

    while toTs >= fromTs:
        # Calculate the number of data points to fetch
        minutes_to_fetch = int((toTs - fromTs) / 60) + 1  # +1 to include both endpoints
        if minutes_to_fetch <= 0:
            print(f'minutes_to_fetch < 0: {minutes_to_fetch}... returning')
            break

        # Since limit = number of data points - 1
        limit = min(2000, minutes_to_fetch) - 1  # Fetch up to 2000 data points per request

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
                    print(f"Downloaded data up to {datetime.fromtimestamp(toTs, timezone.utc)}")
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
        # Exclude any data with time less than or equal to last_timestamp
        combined_df = combined_df[combined_df['time'] > last_timestamp]
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
    toTs = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp())
    params = {
        'api_key': api_key,
        'fsym': 'BTC',
        'tsym': 'USD',
        'limit': 1,  # Fetch the latest data point
        'aggregate': 1,
        'toTs': toTs
    }
    try:
        response = requests.get(base_url, params=params)
        # print(f"Request URL: {response.url}")
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'Success' and 'Data' in data and 'Data' in data['Data']:
                df = pd.DataFrame(data['Data']['Data'])
                return df
            else:
                print("No data found in response.")
                print(f"Response data: {data}")
                print(f"Message: {data.get('Message')}")
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

        print(f"Starting from last timestamp: {datetime.fromtimestamp(last_timestamp, timezone.utc)}")

        # Download missing data and clean it
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

            # Read existing CSV
            existing_df = pd.read_csv(csv_file)

            # Append new data
            combined_df = pd.concat([existing_df, new_data], ignore_index=True)

            # Drop duplicates
            combined_df.drop_duplicates(subset=['Timestamp'], inplace=True)

            # Sort by Timestamp
            combined_df.sort_values('Timestamp', inplace=True)

            # Save back to CSV
            combined_df.to_csv(csv_file, index=False)

            # Update last_timestamp
            last_timestamp = combined_df['Timestamp'].max()
            print(f"Appended {len(new_data)} new records to {csv_file}")

        else:
            print("No new data downloaded.")

        # After initial run, only fetch and append the latest minute data
        while True:
            try:
                new_data = fetch_latest_data(api_key)
                if new_data is not None and not new_data.empty:
                    # Check if new_data timestamp is greater than last_timestamp
                    new_timestamp = new_data['time'].iloc[0]
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
                        print(f"Appended new record to {csv_file} at {datetime.fromtimestamp(new_timestamp, timezone.utc)}")
                    else:
                        print("No new data available yet.")
                else:
                    print("No new data downloaded.")

                # Wait until the start of the next minute
                print("Waiting for the next minute...")
                now = datetime.now(timezone.utc)
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

def read_api_key(file_path='apikey.txt'):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        print("Error: apikey.txt file not found!")
        return None
    except Exception as e:
        print(f"Error reading API key: {e}")
        return None

if __name__ == "__main__":
    api_key = read_api_key()
    if api_key is None:
        print("Failed to load API key. Exiting...")
        exit(1)
        
    csv_file = "PriceData\\BTCUSD_data.csv"
    print("Starting BTC data update script...")
    main(csv_file, api_key)

