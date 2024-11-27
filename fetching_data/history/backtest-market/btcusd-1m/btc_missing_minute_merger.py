import pandas as pd

# Read both CSV files
df1 = pd.read_csv('merged_output.csv')  # base file
df2 = pd.read_csv('BTC_complementary_adjusted_output.csv') # file to fill gaps

# Convert Formatted_Time to datetime in both dataframes
df1['Formatted_Time'] = pd.to_datetime(df1['Formatted_Time'])
df2['Formatted_Time'] = pd.to_datetime(df2['Formatted_Time'])

# Remove any duplicate timestamps before setting index
df1 = df1.drop_duplicates('Formatted_Time', keep='first')
df2 = df2.drop_duplicates('Formatted_Time', keep='first')

# Set Formatted_Time as index for both dataframes
df1.set_index('Formatted_Time', inplace=True)
df2.set_index('Formatted_Time', inplace=True)

# Create a complete time range with minute frequency
full_range = pd.date_range(start=df1.index.min(), 
                          end=df1.index.max(), 
                          freq='1min')

# Reindex df1 to show all missing minutes
df1_reindexed = df1.reindex(full_range)

# Get missing timestamps before filling
missing_times = df1_reindexed[df1_reindexed.isnull().any(axis=1)].index

# Fill NaN values with corresponding values from df2
merged_df = df1_reindexed.fillna(df2)

# Log merged minutes
with open('merged_minutes.log', 'w') as f:
    f.write("Minutes merged from second file:\n")
    for time in missing_times:
        if time in df2.index:
            f.write(f"{time}\n")

# Reset index and prepare final output
merged_df.reset_index(inplace=True)
merged_df.rename(columns={'index': 'Formatted_Time'}, inplace=True)
merged_df['Timestamp'] = merged_df['Formatted_Time'].astype('int64') // 10**9

# Save the merged result
merged_df.to_csv('merged_output.csv', index=False)
