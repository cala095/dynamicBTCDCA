import pandas as pd
from datetime import datetime

# Read the CSV file with headers
df = pd.read_csv('PriceData\\btcusd_1-min_data.csv')

# Convert Unix time to formatted datetime
df['Formatted_Time'] = df['Timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime('%d-%m-%y_%H-%M-%S'))

# Reorder columns to put Formatted_Time at the beginning
cols = ['Formatted_Time'] + [col for col in df.columns if col != 'Formatted_Time']
df = df[cols]

# Save to a new CSV file
df.to_csv('transformed_data.csv', index=False)
