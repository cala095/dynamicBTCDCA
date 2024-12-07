import pandas as pd
from datetime import datetime

# Read the CSV file with ; as separator
df = pd.read_csv('btcusd-1m.csv', sep=';', 
                 names=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume'])

# Combine Date and Time columns
df['Formatted_Time'] = df['Date'] + ' ' + df['Time']

# Convert to datetime
df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time'], format='%d/%m/%Y %H:%M:%S')

# Create timestamp column (Unix timestamp)
df['Timestamp'] = df['Formatted_Time'].astype('int64') // 10**9

# Reorder columns
df = df[['Formatted_Time', 'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]

# Save to new CSV file
df.to_csv('output.csv', index=False)