import os
import base64
import json
import re
import time
import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Define Gmail API scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

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

def fetch_and_process_emails(service):
    query = 'subject:"Alert" from:noreply@tradingview.com'
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    if not messages:
        print("No new TradingView alerts found.")
        return

    # Fetch message details
    emails = []
    for msg in messages:
        msg_id = msg['id']
        message = service.users().messages().get(userId='me', id=msg_id).execute()
        internal_date = int(message['internalDate'])  # milliseconds since epoch
        emails.append({'id': msg_id, 'message': message, 'internalDate': internal_date})

    # Sort emails by internalDate
    emails.sort(key=lambda x: x['internalDate'])

    # Process each email
    for email in emails:
        process_email(email['message'])
        # Delete the email after processing
        service.users().messages().delete(userId='me', id=email['id']).execute()

def process_email(message):
    # Extract email body content
    payload = message['payload']
    parts = payload.get('parts')
    if parts:
        # The message has multiple parts
        for part in parts:
            if part['mimeType'] == 'text/plain':
                email_data = part['body']['data']
                break
        else:
            print("No text/plain part found in email.")
            return
    else:
        email_data = payload['body']['data']

    decoded_data = base64.urlsafe_b64decode(email_data).decode('utf-8')
    email_content = decoded_data.strip()

    # Add this line to DEBUG email content
    print("DEBUG Email Content:", email_content)

    # Extract JSON string from the email content using regex
    json_match = re.search(r'\{.*\}', email_content, re.DOTALL)
    if json_match:
        json_str = json_match.group(0)
        try:
            data = json.loads(json_str)
                # Parse email content as JSON
            required_keys = {"ticker", "volume", "price", "time", "exchange"}
            if all(key in data for key in required_keys):
                ticker = data['ticker']
                process_ticker_data(ticker, data)
                print(f"Processed alert for ticker {ticker}: {data}")
            else:
                print("Email content does not match the required format.")
        except json.JSONDecodeError:
            print("Email content is not valid JSON.")
        except json.JSONDecodeError as e:
            print("Failed to parse JSON:", e)
    else:
        print("No JSON found in the email content.")

    

def parse_time(data_time):
    if isinstance(data_time, (int, float)):
        # Assume it's a UNIX timestamp in seconds
        return datetime.datetime.utcfromtimestamp(data_time)
    elif isinstance(data_time, str):
        try:
            # Try to parse as ISO format
            return datetime.datetime.fromisoformat(data_time)
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
    state_file = f"{ticker}_state.json"
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
            last_timestamp = datetime.datetime.fromisoformat(state['last_timestamp'])
            last_price = state['last_price']
            return last_timestamp, last_price
    else:
        return None, None

def save_last_ticker_state(ticker, last_timestamp, last_price):
    state_file = f"{ticker}_state.json"
    state = {
        'last_timestamp': last_timestamp.isoformat(),
        'last_price': last_price
    }
    with open(state_file, 'w') as f:
        json.dump(state, f)

def append_to_ticker_file(ticker, line):
    data_file = f"{ticker}_data.txt"
    with open(data_file, 'a') as f:
        f.write(line)

def process_ticker_data(ticker, data):
    last_timestamp, last_price = load_last_ticker_state(ticker)
    try:
        current_timestamp = parse_time(data['time'])
    except ValueError as e:
        print(f"Error parsing time: {e}")
        return
    current_price = data['price']

    # If last_timestamp is None, initialize it to current_timestamp - 1 minute
    if last_timestamp is None:
        last_timestamp = current_timestamp - datetime.timedelta(minutes=1)
        last_price = current_price  # Initialize last_price to current_price

    # Now, check for time gaps
    time_difference = current_timestamp - last_timestamp
    total_minutes = int(time_difference.total_seconds() / 60)
    if total_minutes > 1:
        # Fill in missing intervals
        for i in range(1, total_minutes):
            timestamp_to_add = last_timestamp + datetime.timedelta(minutes=i)
            timestamp_str = timestamp_to_add.strftime('%d-%m-%y_%H-%M-%S')
            line = f"{timestamp_str}, {last_price}\n"
            append_to_ticker_file(ticker, line)

    # Now, append the current data
    timestamp_str = current_timestamp.strftime('%d-%m-%y_%H-%M-%S')
    line = f"{timestamp_str}, {current_price}\n"
    append_to_ticker_file(ticker, line)

    # Update last_timestamp and last_price
    last_timestamp = current_timestamp
    last_price = current_price

    # Save the last state
    save_last_ticker_state(ticker, last_timestamp, last_price)

# Main loop to poll for new emails
def main():
    service = authenticate_gmail()

    while True:
        fetch_and_process_emails(service)
        time.sleep(60)  # Wait before checking again

if __name__ == '__main__':
    main()
