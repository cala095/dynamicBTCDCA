import os
import base64
import json
import time
import datetime
import html
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

# Define Gmail API scopes
SCOPES = ['https://mail.google.com/']

# Global variables
last_ticker_states = {}
data_added = {}  # Tracks whether data was added for each ticker during the interval
tickers = ['US10Y', 'US02Y', 'VIX', 'SPX', 'GOLD', 'NDQ', 'MOVE', 'DXY']  # List of tickers to monitor

# Authenticate and create a service object
def authenticate_gmail():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                print("Token has expired or been revoked. Deleting token.json and re-authenticating.")
                os.remove('token.json')
                return authenticate_gmail()
        else:
            flow = InstalledAppFlow.from_client_secrets_file('gmail_credential.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token_file:
            token_file.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def get_label_id(service, label_name):
    label_list = service.users().labels().list(userId='me').execute()
    labels = label_list.get('labels', [])
    for label in labels:
        if label['name'] == label_name:
            return label['id']
    # If the label doesn't exist, create it
    label = {'name': label_name}
    result = service.users().labels().create(userId='me', body=label).execute()
    return result['id']

def fetch_and_process_emails(service):
    query = 'subject:"Alert" from:noreply@tradingview.com -label:Processed'
    emails = []
    page_token = None
    maxResults = None  # Set to None to fetch all emails

    while True:
        try:
            if page_token:
                results = service.users().messages().list(
                    userId='me', q=query, maxResults=maxResults, pageToken=page_token
                ).execute()
            else:
                results = service.users().messages().list(
                    userId='me', q=query, maxResults=maxResults
                ).execute()
            messages = results.get('messages', [])
            if messages:
                emails.extend(messages)
            page_token = results.get('nextPageToken')
            if not page_token or maxResults:
                break
        except Exception as e:
            print(f"An error occurred during email listing: {e}")
            break

    if not emails:
        print("No new TradingView alerts found.")
        return

    print(f"Fetched {len(emails)} email(s).")

    # Get the label ID for "Processed"
    processed_label_id = get_label_id(service, 'Processed')

    processed_emails = []
    for msg in emails:
        msg_id = msg['id']
        try:
            message = service.users().messages().get(userId='me', id=msg_id).execute()
            internal_date = int(message['internalDate'])  # milliseconds since epoch

            # Get the email subject
            headers = message['payload'].get('headers', [])
            subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'No Subject')
            print(f"Processing email ID: {msg_id}, Subject: {subject}")

            processed_emails.append({'id': msg_id, 'message': message, 'internalDate': internal_date})
        except HttpError as error:
            if error.resp.status == 404:
                print(f"Message with ID {msg_id} not found. It may have been deleted or moved.")
                continue
            else:
                print(f"An error occurred while retrieving message ID {msg_id}: {error}")
                continue

    if not processed_emails:
        print("No emails to process after filtering.")
        return

    # Sort emails by internalDate
    processed_emails.sort(key=lambda x: x['internalDate'])

    # Process each email
    for email in processed_emails:
        process_email(email['message'])
        # Add the "Processed" label and remove from Inbox
        try:
            service.users().messages().modify(
                userId='me',
                id=email['id'],
                body={
                    'addLabelIds': [processed_label_id],
                    'removeLabelIds': ['INBOX']
                }
            ).execute()
        except HttpError as e:
            print(f"An error occurred while modifying the email ID {email['id']}: {e}")


def process_email(message):
    # Extract email body content
    payload = message['payload']
    email_data = None

    if 'parts' in payload:
        parts = payload['parts']
        for part in parts:
            if part['mimeType'] == 'text/plain':
                email_data = part['body']['data']
                break
            elif part['mimeType'] == 'text/html':
                email_data = part['body']['data']
                break
    else:
        email_data = payload['body']['data']

    if email_data:
        decoded_data = base64.urlsafe_b64decode(email_data).decode('utf-8')
        email_content = decoded_data.strip()

        # If content is HTML, parse it to extract JSON
        if '<html' in email_content.lower():
            soup = BeautifulSoup(email_content, 'html.parser')
            # Find all <p> tags
            p_tags = soup.find_all('p')
            for p in p_tags:
                text = p.get_text(strip=True)
                # Unescape HTML entities
                text_unescaped = html.unescape(text)
                # Try to parse JSON
                try:
                    data = json.loads(text_unescaped)
                    required_keys = {"ticker", "volume", "price", "time"}
                    if all(key in data for key in required_keys):
                        ticker = data['ticker']
                        process_ticker_data(ticker, data)
                        print(f"Processed alert for ticker {ticker}: {data}")
                        return
                except json.JSONDecodeError:
                    continue  # Try next <p> tag
            print("No valid JSON found in the email content.")
            print("DEBUG Email Content:", email_content)
        else:
            # If not HTML, try to parse as JSON directly
            try:
                data = json.loads(email_content)
                required_keys = {"ticker", "volume", "price", "time"}
                if all(key in data for key in required_keys):
                    ticker = data['ticker']
                    process_ticker_data(ticker, data)
                    print(f"Processed alert for ticker {ticker}: {data}")
                else:
                    print("Email content does not match the required format.")
            except json.JSONDecodeError:
                print("Email content is not valid JSON.")
    else:
        print("No email content found.")

def parse_time(data_time):
    if isinstance(data_time, (int, float)):
        # Assume it's a UNIX timestamp in seconds
        return datetime.datetime.utcfromtimestamp(data_time)
    elif isinstance(data_time, str):
        try:
            # Try to parse as ISO format
            dt = datetime.datetime.fromisoformat(data_time.replace('Z', '+00:00'))
            # Remove timezone info to make it tz-naive
            return dt.replace(tzinfo=None)
        except ValueError:
            pass
        try:
            # Try to parse as UNIX timestamp in string
            return datetime.datetime.utcfromtimestamp(float(data_time))
        except ValueError:
            pass
    # If none of the above, raise an error
    raise ValueError(f"Unable to parse time: {data_time}")

def load_last_ticker_state(ticker):
    state_file = f"PriceData\\{ticker}_state.json"
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
            last_timestamp = datetime.datetime.fromisoformat(state['last_timestamp'])
            # Ensure last_timestamp is tz-naive
            if last_timestamp.tzinfo is not None:
                last_timestamp = last_timestamp.replace(tzinfo=None)
            last_price = state['last_price']
            last_volume = state.get('last_volume', 0)
            return last_timestamp, last_price, last_volume
    else:
        print(f"No last state found for {ticker}")
        return None, None, None

def save_last_ticker_state(ticker, last_timestamp, last_price, last_volume):
    state_file = f"PriceData\\{ticker}_state.json"
    state = {
        'last_timestamp': last_timestamp.isoformat(),
        'last_price': last_price,
        'last_volume': last_volume
    }
    with open(state_file, 'w') as f:
        json.dump(state, f)

def get_last_timestamp_from_file(ticker):
    data_file = f"PriceData\\{ticker}_data.csv"
    if not os.path.exists(data_file):
        return None
    df = pd.read_csv(data_file)
    if df.empty:
        return None
    # Parse 'timestamp' column
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M')
    last_timestamp = df['timestamp'].iloc[-1]
    return last_timestamp

def process_ticker_data(ticker, data):
    global data_added
    last_timestamp, last_price, last_volume = load_last_ticker_state(ticker)
    try:
        current_timestamp = parse_time(data['time'])
    except ValueError as e:
        print(f"Error parsing time: {e}")
        return
    current_price = data['price']
    current_volume = data['volume']

    # Convert current_timestamp to tz-naive and round to the nearest minute
    current_timestamp = current_timestamp.replace(tzinfo=None, second=0, microsecond=0)

    # Load existing data into a DataFrame without date_parser
    data_file = f"PriceData\\{ticker}_data.csv"
    if os.path.exists(data_file):
        df_existing = pd.read_csv(data_file)
    else:
        df_existing = pd.DataFrame(columns=['timestamp', 'price', 'volume'])

    # Parse 'timestamp' column
    if not df_existing.empty:
        df_existing['timestamp'] = pd.to_datetime(df_existing['timestamp'], format='%Y-%m-%d %H:%M')

    # Ensure last_timestamp is tz-naive
    if last_timestamp and last_timestamp.tzinfo is not None:
        last_timestamp = last_timestamp.replace(tzinfo=None)

    # If last_timestamp is None, initialize it to current_timestamp - 1 minute
    if last_timestamp is None:
        last_timestamp = current_timestamp - datetime.timedelta(minutes=1)
        last_price = current_price  # Initialize last_price to current_price

    # Check for time gaps and fill missing intervals
    time_difference = current_timestamp - last_timestamp
    total_minutes = int(time_difference.total_seconds() / 60)

    if total_minutes > 1:
        missing_timestamps = [last_timestamp + datetime.timedelta(minutes=i) for i in range(1, total_minutes)]
        missing_prices = [last_price] * len(missing_timestamps)
        missing_volumes = [0] * len(missing_timestamps)
        df_missing = pd.DataFrame({
            'timestamp': missing_timestamps,
            'price': missing_prices,
            'volume': missing_volumes
        })
        # Ensure 'timestamp' column in df_missing is datetime
        df_missing['timestamp'] = pd.to_datetime(df_missing['timestamp'])
        # Concatenate missing data with existing data
        df_existing = pd.concat([df_existing, df_missing], ignore_index=True)

    # Create a DataFrame for the current data point
    df_new = pd.DataFrame({
        'timestamp': [current_timestamp],
        'price': [current_price],
        'volume': [current_volume]
    })

    # Ensure 'timestamp' column in df_new is datetime
    df_new['timestamp'] = pd.to_datetime(df_new['timestamp'])

    # Append the new data
    df = pd.concat([df_existing, df_new], ignore_index=True)

    # Ensure 'timestamp' in df is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Aggregate data by timestamp (minute)
    df = df.groupby('timestamp').agg({
        'price': 'mean',  # Average the prices
        'volume': 'sum'   # Sum the volumes
    }).reset_index()

    # Sort the DataFrame by timestamp
    df.sort_values(by='timestamp', inplace=True)
    
    # Debugging: Print DataFrames (optional)
    # print("df_existing:")
    # print(df_existing)
    # print("df_new:")
    # print(df_new)
    # print("Concatenated df:")
    # print(df)
    # print("Data types in df:")
    # print(df.dtypes)

    # Save the DataFrame back to the CSV file
    df.to_csv(data_file, index=False, date_format='%Y-%m-%d %H:%M')

    # Update last_timestamp and last_price
    last_timestamp = current_timestamp
    last_price = current_price

    # Save the last state
    save_last_ticker_state(ticker, last_timestamp, last_price, current_volume)

    # Mark that data was added for this ticker
    data_added[ticker] = True

def sync_missing_data():
    current_time = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    for ticker in tickers:
        print(f"Syncing data for ticker {ticker}...")
        last_timestamp, last_price, last_volume = load_last_ticker_state(ticker)
        if last_price is not None:
            last_file_timestamp = get_last_timestamp_from_file(ticker)
            # If last_file_timestamp is None, initialize it to last_timestamp
            if not last_file_timestamp:
                last_file_timestamp = last_timestamp
            if last_file_timestamp is None:
                last_file_timestamp = current_time - datetime.timedelta(minutes=1)
            time_difference = current_time - last_file_timestamp
            total_minutes = int(time_difference.total_seconds() / 60)
            if total_minutes >= 1:
                # Create a DataFrame with missing timestamps
                timestamps = [last_file_timestamp + datetime.timedelta(minutes=i) for i in range(1, total_minutes + 1)]
                prices = [last_price] * len(timestamps)
                volumes = [0] * len(timestamps)
                df_missing = pd.DataFrame({
                    'timestamp': timestamps,
                    'price': prices,
                    'volume': volumes
                })

                # Load existing data without date_parser
                data_file = f"PriceData\\{ticker}_data.csv"
                if os.path.exists(data_file):
                    df_existing = pd.read_csv(data_file)
                else:
                    df_existing = pd.DataFrame(columns=['timestamp', 'price', 'volume'])

                # Parse 'timestamp' columns
                if not df_existing.empty:
                    df_existing['timestamp'] = pd.to_datetime(df_existing['timestamp'], format='%Y-%m-%d %H:%M')
                df_missing['timestamp'] = pd.to_datetime(df_missing['timestamp'])

                # Concatenate and aggregate
                df = pd.concat([df_existing, df_missing], ignore_index=True)

                df['timestamp'] = pd.to_datetime(df['timestamp'])

                df = df.groupby('timestamp').agg({
                    'price': 'mean',
                    'volume': 'sum'
                }).reset_index()
                df.sort_values(by='timestamp', inplace=True)
                # Save back to CSV
                df.to_csv(data_file, index=False, date_format='%Y-%m-%d %H:%M')
                # Update last_timestamp
                last_timestamp = current_time
                save_last_ticker_state(ticker, last_timestamp, last_price, last_volume)
                print(f"Appended missing data for {ticker}.")
            else:
                print(f"Data for {ticker} is already up to date.")
        else:
            print(f"No previous data available for ticker {ticker} to sync.")
        # Reset data_added for the next interval
        data_added[ticker] = False

# Main loop to poll for new emails
def main():
    global data_added
    service = authenticate_gmail()

    # Initialize last ticker states
    for ticker in tickers:
        last_timestamp, last_price, last_volume = load_last_ticker_state(ticker)
        last_ticker_states[ticker] = {'timestamp': last_timestamp, 'price': last_price, 'volume': last_volume}
        data_added[ticker] = False  # Initialize data_added flag

    while True:
        fetch_and_process_emails(service)
        sync_missing_data()
        print("Sleeping for 60 seconds...")
        time.sleep(60)  # Wait before checking again

if __name__ == '__main__':
    main()
