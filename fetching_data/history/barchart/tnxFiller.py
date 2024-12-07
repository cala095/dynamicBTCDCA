import pandas as pd

# Read the CSV file
df = pd.read_csv('tnx 1m\\tnx_intraday_merged.csv', parse_dates=['Time'])

# Set Time column as index
df.set_index('Time', inplace=True)

# Create a complete time range with minute frequency
full_range = pd.date_range(start=df.index.min(),
                          end=df.index.max(),
                          freq='1min')

# Reindex the dataframe with the complete range and forward fill values
df_filled = df.reindex(full_range).ffill()

# Reset index and rename the column
df_filled = df_filled.reset_index()
df_filled = df_filled.rename(columns={'index': 'Time'})

# Save the filled dataframe to a new CSV file
df_filled.to_csv('tnx_filled_data.csv', index=False)