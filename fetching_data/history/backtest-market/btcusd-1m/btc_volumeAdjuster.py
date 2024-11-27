import pandas as pd

# Read the CSV file
df = pd.read_csv('output.csv')

# Adjust the Volume based on OHLC comparison
for i in range(1, len(df)):
    if (df.loc[i, 'Open'] == df.loc[i-1, 'Open'] and 
        df.loc[i, 'High'] == df.loc[i-1, 'High'] and 
        df.loc[i, 'Low'] == df.loc[i-1, 'Low'] and 
        df.loc[i, 'Close'] == df.loc[i-1, 'Close']):
        df.loc[i, 'Volume'] = 0

# Save the modified DataFrame to a new CSV file
df.to_csv('adjusted_output.csv', index=False)
