import pandas as pd
import os

print("**1. Load Historical Data**")
# Adjust the file path to your US02Y historical data file
historical = pd.read_csv(
    '..\\barchart\\merged + filled data\\estimated_2yr_yield_1min_duration_method_adjusted.csv',
    delimiter=',',  # Use comma as delimiter
    header=0,  # Assuming the file has a header row
    names=['Time', 'Estimated_Yield_%']
)

print("**2. Load Live Data**")
live = pd.read_csv('..\\..\\live\\PriceData\\US02Y_data.csv', delimiter=',', header=0)

print("**3. Preprocess Historical Data**")
# Convert 'Time' to datetime
historical['datetime'] = pd.to_datetime(
    historical['Time'],
    format='%Y-%m-%d %H:%M:%S'
)

# Since there's no 'Volume' column in the historical data
volume_present = False

# Select relevant columns
historical = historical[['datetime', 'Estimated_Yield_%']]

# Rename 'Estimated_Yield_%' to 'Price' for consistency
historical.rename(columns={'Estimated_Yield_%': 'Price'}, inplace=True)

print("**4. Preprocess Live Data**")
# Convert 'timestamp' to datetime
live['datetime'] = pd.to_datetime(
    live['timestamp'],
    format='%Y-%m-%d %H:%M'
)

# Check if 'volume' is in live data columns and has any non-zero values
if 'volume' in live.columns and (live['volume'] != 0).any():
    volume_present_live = True
    # Rename 'price' to 'Price' and 'volume' to 'Volume' for consistency
    live.rename(columns={'price': 'Price', 'volume': 'Volume'}, inplace=True)
    # Select relevant columns including 'Volume'
    live = live[['datetime', 'Price', 'Volume']]
else:
    volume_present_live = False
    # Rename 'price' to 'Price'
    live.rename(columns={'price': 'Price'}, inplace=True)
    # Select relevant columns, exclude 'Volume'
    live = live[['datetime', 'Price']]

# Determine if 'Volume' should be included based on both historical and live data
volume_present = volume_present or volume_present_live

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
    duplicates.to_csv('duplicate_datetimes_US02Y.csv', index=False)
    print("Duplicate datetime entries saved to 'duplicate_datetimes_US02Y.csv' for review.")

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
if 'Volume' in merged_data.columns and volume_present:
    merged_data['Volume'] = merged_data['Volume'].ffill()
else:
    # If 'Volume' is not present or not needed, drop the column if it exists
    if 'Volume' in merged_data.columns:
        merged_data.drop(columns=['Volume'], inplace=True)

# Reset index to make 'datetime' a column again
merged_data.reset_index(inplace=True)
merged_data.rename(columns={'index': 'datetime'}, inplace=True)

print("**8. Export Merged Data**")
if not os.path.exists('Merged'):
    os.makedirs('Merged')
outFile = "Merged_US02Y.csv"
merged_data.to_csv(f'Merged/{outFile}', index=False)

print(f"Data merged and forward-filled successfully! The merged file is saved as {outFile}.")
