import pandas as pd

print("**1. Load Historical Data**")
historical_DXY = pd.read_csv(
    'backtest-market\\dlarind-1m_bk\\dlarind-1m_bk.csv',
    delimiter=';',
    header=None,
    names=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
)

print("**2. Load Live Data**")
live_DXY = pd.read_csv('..\\live\\PriceData\\DXY_data.csv', delimiter=',', header=0)

print("**3. Preprocess Historical Data**")
# Combine Date and Time into a single datetime column
historical_DXY['datetime'] = pd.to_datetime(
    historical_DXY['Date'] + ' ' + historical_DXY['Time'],
    format='%d/%m/%Y %H:%M'
)

# Select relevant columns
historical_DXY = historical_DXY[['datetime', 'Open']]

# Rename 'Open' to 'Price' for consistency
historical_DXY.rename(columns={'Open': 'Price'}, inplace=True)

print("**4. Preprocess Live Data**")
# Convert timestamp to datetime
live_DXY['datetime'] = pd.to_datetime(
    live_DXY['timestamp'],
    format='%Y-%m-%d %H:%M'
)

# Select relevant columns
live_DXY = live_DXY[['datetime', 'price']]

# Rename 'price' to 'Price' for consistency
live_DXY.rename(columns={'price': 'Price'}, inplace=True)

print("**5. Merge Datasets**")
# Concatenate the dataframes
merged_data = pd.concat([historical_DXY, live_DXY], ignore_index=True)

# Sort by datetime
merged_data.sort_values(by='datetime', inplace=True)

print("**6. Handle Duplicate Datetime Entries**")
# Check for duplicates
duplicates = merged_data[merged_data.duplicated(subset='datetime', keep=False)]

print("Number of duplicate datetime entries:", duplicates.shape[0])

if not duplicates.empty:
    # Optionally, save duplicates to a CSV for review
    duplicates.to_csv('duplicate_datetimes.csv', index=False)
    print("Duplicate datetime entries saved to 'duplicate_datetimes.csv' for review.")
    
    # Option A: Remove duplicates by keeping the first occurrence
    merged_data = merged_data.drop_duplicates(subset='datetime', keep='first')
    
    # Option B: Alternatively, aggregate duplicates by taking the average
    # merged_data = merged_data.groupby('datetime').mean().reset_index()

print("**7. Forward Fill Missing Minutes**")
# Set 'datetime' as the index
merged_data.set_index('datetime', inplace=True)

# Create a complete datetime index from start to end at 1-minute frequency
all_minutes = pd.date_range(
    start=merged_data.index.min(),
    end=merged_data.index.max(),
    freq='T'
)

# Reindex the dataframe to include all minutes
merged_data = merged_data.reindex(all_minutes)

# Forward fill the missing 'Price' values
merged_data['Price'] = merged_data['Price'].ffill()

# Reset index to make 'datetime' a column again
merged_data.reset_index(inplace=True)
merged_data.rename(columns={'index': 'datetime'}, inplace=True)

print("**8. Export Merged Data**")
merged_data.to_csv('merged_data.csv', index=False)

print("Data merged and forward-filled successfully! The merged file is saved as 'merged_data.csv'.")
