import pandas as pd
import os

print("**1. Load Historical Data**")
historical = pd.read_csv(
    '..\\backtest-market\\vix-1m_bk-1\\vix-1m_bk.csv',
    delimiter=';',
    header=None,
    names=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
)

print("**2. Load Live Data**")
live = pd.read_csv('..\\..\\live\\PriceData\\VIX_data.csv', delimiter=',', header=0)

print("**3. Preprocess Live Data**")
# Convert timestamp to datetime
live['datetime'] = pd.to_datetime(
    live['timestamp'],
    format='%Y-%m-%d %H:%M'
)

# Check if 'volume' is in live data columns and has any non-zero values
if 'volume' in live.columns and (live['volume'] != 0).any():
    volume_present = True
    # Rename 'price' to 'Price' and 'volume' to 'Volume' for consistency
    live.rename(columns={'price': 'Price', 'volume': 'Volume'}, inplace=True)
    # Select relevant columns including 'Volume'
    live = live[['datetime', 'Price', 'Volume']]
else:
    volume_present = False
    # Rename 'price' to 'Price'
    live.rename(columns={'price': 'Price'}, inplace=True)
    # Select relevant columns, exclude 'Volume'
    live = live[['datetime', 'Price']]

print("**4. Preprocess Historical Data**")
# Combine Date and Time into a single datetime column
historical['datetime'] = pd.to_datetime(
    historical['Date'] + ' ' + historical['Time'],
    format='%d/%m/%Y %H:%M'
)

if volume_present:
    # Select relevant columns including 'Volume'
    historical = historical[['datetime', 'Open', 'Volume']]
    # Rename 'Open' to 'Price' for consistency
    historical.rename(columns={'Open': 'Price'}, inplace=True)
else:
    # Select relevant columns excluding 'Volume'
    historical = historical[['datetime', 'Open']]
    # Rename 'Open' to 'Price' for consistency
    historical.rename(columns={'Open': 'Price'}, inplace=True)

print("**5. Merge Datasets**")
# Concatenate the dataframes
merged_data = pd.concat([historical, live], ignore_index=True)

# Sort by datetime
merged_data.sort_values(by='datetime', inplace=True)

print("**6. Handle Duplicate Datetime Entries**")
# Check for duplicates
duplicates = merged_data[merged_data.duplicated(subset='datetime', keep=False)]

print("Number of duplicate datetime entries:", duplicates.shape[0])

if not duplicates.empty:
    # Optionally, save duplicates to a CSV for review
    duplicates.to_csv('duplicate_datetimes_VIX.csv', index=False)
    print("Duplicate datetime entries saved to 'duplicate_datetimes_VIX.csv' for review.")

    # Remove duplicates by keeping the first occurrence
    merged_data = merged_data.drop_duplicates(subset='datetime', keep='first')

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

# Forward fill 'Volume' values if 'Volume' column exists
if 'Volume' in merged_data.columns:
    merged_data['Volume'] = merged_data['Volume'].ffill()

# Reset index to make 'datetime' a column again
merged_data.reset_index(inplace=True)
merged_data.rename(columns={'index': 'datetime'}, inplace=True)

print("**8. Export Merged Data**")
if not os.path.exists('Merged'):
    os.makedirs('Merged')
outFile = "Merged_VIX.csv"
merged_data.to_csv(f'Merged/{outFile}', index=False)

print(f"Data merged and forward-filled successfully! The merged file is saved as {outFile}.")
