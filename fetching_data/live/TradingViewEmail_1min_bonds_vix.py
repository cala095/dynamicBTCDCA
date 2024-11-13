import os
import base64
import json
import time
import datetime
import html
import threading
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup

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
            creds.refresh(Request())
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
    maxResults = None #for debug purposes -> else None

    while True:
        try:
            if page_token:
                results = service.users().messages().list(userId='me', q=query, maxResults=maxResults, pageToken=page_token).execute()
            else:
                results = service.users().messages().list(userId='me', q=query, maxResults=maxResults).execute()
            messages = results.get('messages', [])
            if messages:
                emails.extend(messages)
            page_token = results.get('nextPageToken')
            if not page_token or maxResults:
                break
        except Exception as e:
            print(f"An error occurred: {e}")
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
        message = service.users().messages().get(userId='me', id=msg_id).execute()
        internal_date = int(message['internalDate'])  # milliseconds since epoch

        # Get the email subject
        headers = message['payload'].get('headers', [])
        subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'No Subject')
        print(f"Processing email ID: {msg_id}, Subject: {subject}")

        processed_emails.append({'id': msg_id, 'message': message, 'internalDate': internal_date})

    # Sort emails by internalDate
    processed_emails.sort(key=lambda x: x['internalDate'])

    # Process each email
    for email in processed_emails:
        process_email(email['message'])
        # Add the "Processed" label and remove from Inbox
        service.users().messages().modify(
            userId='me',
            id=email['id'],
            body={
                'addLabelIds': [processed_label_id],
                'removeLabelIds': ['INBOX']
            }
        ).execute()
        # Delete the email after processing (uncomment if desired)
        # service.users().messages().delete(userId='me', id=email['id']).execute()

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
        return datetime.datetime.utcfromtimestamp(data_time).replace(tzinfo=datetime.timezone.utc)
    elif isinstance(data_time, str):
        try:
            # Try to parse as ISO format
            dt = datetime.datetime.fromisoformat(data_time.replace('Z', '+00:00'))
            return dt
        except ValueError:
            pass
        try:
            # Try to parse as UNIX timestamp in string
            return datetime.datetime.utcfromtimestamp(float(data_time)).replace(tzinfo=datetime.timezone.utc)
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
            last_price = state['last_price']
            last_volume = state['last_volume']
            return last_timestamp, last_price, last_volume
    else:
        print(f"no last state found for {ticker}")
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
    data_file = f"{ticker}_data.txt"
    if not os.path.exists(data_file):
        return None
    with open(data_file, 'r') as f:
        lines = f.readlines()
        if not lines:
            return None
        last_line = lines[-1].strip()
        if not last_line:
            return None
        try:
            timestamp_str, _ = last_line.split(', ')
            last_timestamp_naive = datetime.datetime.strptime(timestamp_str, '%d-%m-%y_%H-%M-%S')
            # Make last_timestamp timezone-aware, assuming UTC
            last_timestamp = last_timestamp_naive.replace(tzinfo=datetime.timezone.utc)
            return last_timestamp
        except ValueError:
            return None

def append_to_ticker_file(ticker, line):
    data_file = f"PriceData\\{ticker}_data.txt"
    with open(data_file, 'a') as f:
        f.write(line)

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

    # If last_timestamp is None, initialize it to current_timestamp - 1 minute
    if last_timestamp is None:
        last_timestamp = current_timestamp - datetime.timedelta(minutes=1)
        last_price = current_price  # Initialize last_price to current_price

    # Now, check for time gaps
    time_difference = current_timestamp - last_timestamp
    total_minutes = int(time_difference.total_seconds() / 60)

    # Get the last timestamp from the data file to check for overlaps
    last_file_timestamp = get_last_timestamp_from_file(ticker)

    # If the current timestamp is the same as the last timestamp in the file, skip appending
    if last_file_timestamp and current_timestamp <= last_file_timestamp:
        print(f"Data for timestamp {current_timestamp} already exists in the file for {ticker}. Skipping append.")
        # Update last_timestamp and last_price in the state
        last_timestamp = current_timestamp
        last_price = current_price
        save_last_ticker_state(ticker, last_timestamp, last_price, current_volume)
        return

    # Fill in missing intervals
    if total_minutes > 1:
        for i in range(1, total_minutes):
            timestamp_to_add = last_timestamp + datetime.timedelta(minutes=i)
            if last_file_timestamp and timestamp_to_add <= last_file_timestamp:
                continue  # Skip timestamps that already exist
            timestamp_str = timestamp_to_add.strftime('%d-%m-%y_%H-%M-%S')
            volume_is_zero = current_volume = 0
            line = f"{timestamp_str}, {last_price}, {volume_is_zero}\n"
            append_to_ticker_file(ticker, line)

    # Append the current data if it's newer than the last file timestamp
    if not last_file_timestamp or current_timestamp > last_file_timestamp:
        timestamp_str = current_timestamp.strftime('%d-%m-%y_%H-%M-%S')
        line = f"{timestamp_str}, {current_price}, {current_volume}\n"
        append_to_ticker_file(ticker, line)

    # Update last_timestamp and last_price
    last_timestamp = current_timestamp
    last_price = current_price

    # Save the last state
    save_last_ticker_state(ticker, last_timestamp, last_price, current_volume)

    # Mark that data was added for this ticker
    data_added[ticker] = True

def clean_ticker_data(ticker):
    data_file = f"PriceData\\{ticker}_data.txt"
    if not os.path.exists(data_file):
        print(f"No data file found for ticker {ticker}.")
        return

    print(f"Cleaning data file for ticker {ticker}...")

    cleaned_data = {}
    with open(data_file, 'r') as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue  # Skip empty lines
            try:
                timestamp_str, price_str, volume_str = line.split(', ')
                # Parse timestamp
                timestamp_naive = datetime.datetime.strptime(timestamp_str, '%d-%m-%y_%H-%M-%S')
                # Make timestamp timezone-aware in UTC
                timestamp = timestamp_naive.replace(tzinfo=datetime.timezone.utc)
                # Parse price
                price = float(price_str)
                # Parse volume
                volume = float(volume_str)
                # Use timestamp as the key to ensure uniqueness
                cleaned_data[timestamp] = price
            except ValueError as e:
                print(f"Error parsing line {line_number} in {data_file}: {e}")
                continue  # Skip malformed lines

    # Sort the data by timestamp
    sorted_data = sorted(cleaned_data.items())

    # Write the cleaned data back to the file
    with open(data_file, 'w') as f:
        for timestamp, price in sorted_data:
            # Convert timezone-aware datetime to naive datetime before formatting
            timestamp_naive = timestamp.replace(tzinfo=None)
            timestamp_str = timestamp_naive.strftime('%d-%m-%y_%H-%M-%S')
            line = f"{timestamp_str}, {price}, {volume}\n"
            f.write(line)

    print(f"Data file for ticker {ticker} cleaned.")

def sync_missing_data():
    current_time = datetime.datetime.utcnow().replace(second=0, microsecond=0, tzinfo=datetime.timezone.utc)
    for ticker in tickers:
        if not data_added.get(ticker, False):
            # No new data was added for this ticker during the interval
            print(f"No new data for ticker {ticker} during this interval. Syncing data...")
            last_timestamp, last_price, last_volume = load_last_ticker_state(ticker)
            if last_price is not None:
                last_file_timestamp = get_last_timestamp_from_file(ticker)
                # If last_file_timestamp is None, initialize it to last_timestamp
                if not last_file_timestamp:
                    last_file_timestamp = last_timestamp
                time_difference = current_time - last_file_timestamp
                total_minutes = int(time_difference.total_seconds() / 60)
                if total_minutes >= 1:
                    for i in range(1, total_minutes + 1):
                        timestamp_to_add = last_file_timestamp + datetime.timedelta(minutes=i)
                        if timestamp_to_add > current_time:
                            break
                        timestamp_str = timestamp_to_add.strftime('%d-%m-%y_%H-%M-%S')
                        line = f"{timestamp_str}, {last_price}\n"
                        append_to_ticker_file(ticker, line)
                        # Update last_timestamp and last_price
                        last_timestamp = timestamp_to_add
                        save_last_ticker_state(ticker, last_timestamp, last_price, 0)
                        print(f"Appended data for {ticker} at {timestamp_str} with price {last_price} and zero volume")
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

    for ticker in tickers:
        clean_ticker_data(ticker)

    while True:
        fetch_and_process_emails(service)
        sync_missing_data()
        print("sleep 60 sec")
        time.sleep(60)  # Wait before checking again

if __name__ == '__main__':
    main()
