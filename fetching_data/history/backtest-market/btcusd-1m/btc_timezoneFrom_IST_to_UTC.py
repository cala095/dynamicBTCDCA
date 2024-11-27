import pandas as pd
from datetime import timedelta

# Read the CSV file
df = pd.read_csv('Processed_BTC_mod_for_merge.csv')

# Convert Formatted_Time to datetime
df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time'])

# Subtract 5 hours
df['Formatted_Time'] = df['Formatted_Time'] - timedelta(hours=5)

# Recalculate timestamp
df['Timestamp'] = df['Formatted_Time'].astype('int64') // 10**9

# Save to new file
df.to_csv('BTC_timezone_adjusted_from_ITC_to_UTC.csv', index=False)
