import pandas as pd
import numpy as np

# --- Step 1: Load SHY Intraday Data ---

# Replace with the path to your merged SHY data file
shy_data = pd.read_csv('shy (1 - 3 years maturity U.S. T.NOTE) 1m/shy_intraday_merged.csv')

# Convert 'Time' column to datetime
shy_data['Time'] = pd.to_datetime(shy_data['Time'])

# Ensure data is sorted chronologically
shy_data.sort_values('Time', inplace=True)
shy_data.reset_index(drop=True, inplace=True)

# Extract date from 'Time'
shy_data['Date'] = shy_data['Time'].dt.date

# --- Step 2: Load 2-Year Treasury Yield Data ---

# Replace with the path to your 2-year yield data file
yield_data = pd.read_csv('U.S. 2y yield 1d/usty2rt_daily_historical-data-11-15-2024.csv', skipfooter=1, engine='python')

# Convert 'Time' column to datetime
yield_data['Time'] = pd.to_datetime(yield_data['Time'], format='%m/%d/%Y')

# Ensure data is sorted chronologically
yield_data.sort_values('Time', inplace=True)
yield_data.reset_index(drop=True, inplace=True)

# Keep necessary columns
yield_data = yield_data[['Time', 'Last']]

# Convert yield to percentage
yield_data['Yield_%'] = yield_data['Last']

# Extract date from 'Time'
yield_data['Date'] = yield_data['Time'].dt.date

# --- Step 3: Merge SHY Data with Initial Yield ---

# Merge SHY data with yield data to get the initial yield for each date
shy_data = pd.merge(shy_data, yield_data[['Date', 'Yield_%']], on='Date', how='left')

# Check for missing yields
missing_yield_dates = shy_data[shy_data['Yield_%'].isna()]['Date'].unique()
if len(missing_yield_dates) > 0:
    print("Warning: Missing yield data for the following dates in SHY data:")
    for date in missing_yield_dates:
        print(date)
    # Optionally, remove these dates or handle them appropriately
    shy_data.dropna(subset=['Yield_%'], inplace=True)
else:
    print("All dates in SHY data have corresponding yield data.")

# Rename columns for clarity
shy_data.rename(columns={'Yield_%': 'Initial_Yield_%'}, inplace=True)

# --- Step 4: Set Modified Duration ---

# Use the actual modified duration of SHY
D_mod = 1.9  # Adjust this value as per SHY's current modified duration

# --- Step 5: Calculate Price Returns and Yield Changes ---

# Calculate price returns
shy_data['Price_Return'] = shy_data['Last'].pct_change()

# Handle the first value (which will be NaN)
shy_data['Price_Return'].fillna(0, inplace=True)

# Calculate yield changes (in percentage points)
shy_data['Delta_Yield_%'] = -shy_data['Price_Return'] / D_mod

# --- Step 6: Estimate the 2-Year Yield Over Time ---

# Initialize the estimated yield list
estimated_yields = []

# Iterate over the DataFrame using positional indexing
for idx in range(len(shy_data)):
    row = shy_data.iloc[idx]
    
    if idx == 0 or row['Date'] != shy_data.iloc[idx - 1]['Date']:
        # Start of a new day; use the initial yield
        estimated_yield = row['Initial_Yield_%']
    else:
        # Calculate the new yield
        estimated_yield = estimated_yields[-1] + row['Delta_Yield_%']
    
    estimated_yields.append(estimated_yield)

# Add the estimated yields to the DataFrame
shy_data['Estimated_Yield_%'] = estimated_yields

# --- Optional: Remove Constraints or Adjust Them ---

# If you still want to apply constraints, define reasonable bounds
# For example, allow the yield to vary within ±5 basis points of the initial yield
max_change_bp = 5  # Adjust as needed

shy_data['Upper_Bound'] = shy_data['Initial_Yield_%'] + (max_change_bp / 100)
shy_data['Lower_Bound'] = shy_data['Initial_Yield_%'] - (max_change_bp / 100)

# Apply the bounds
shy_data['Estimated_Yield_%'] = shy_data['Estimated_Yield_%'].clip(
    lower=shy_data['Lower_Bound'], upper=shy_data['Upper_Bound']
)

# --- Step 7: Handle Days Without SHY Data ---

# Get a list of all dates from the yield data
all_dates = pd.to_datetime(yield_data['Date'].unique())

# Get a list of dates present in SHY data
shy_dates = pd.to_datetime(shy_data['Date'].unique())

# Identify dates missing in SHY data
missing_shy_dates = np.setdiff1d(all_dates, shy_dates)

# For missing SHY dates, create entries using the daily yield
missing_yield_data = yield_data[yield_data['Date'].isin(missing_shy_dates)][['Date', 'Yield_%']].copy()

# Assign 'Time' to midday for consistency
missing_yield_data['Time'] = pd.to_datetime(missing_yield_data['Date']) + pd.Timedelta(hours=12)

# Assign 'Estimated_Yield_%' from 'Yield_%'
missing_yield_data['Estimated_Yield_%'] = missing_yield_data['Yield_%']

# --- Step 8: Combine Data and Output ---

# Prepare the output data
output_data = pd.concat([
    shy_data[['Time', 'Estimated_Yield_%']],
    missing_yield_data[['Time', 'Estimated_Yield_%']]
], ignore_index=True)

# Sort the output data by Time
output_data.sort_values('Time', inplace=True)
output_data.reset_index(drop=True, inplace=True)

# Convert yield to percentage format
output_data['Estimated_Yield_%'] = output_data['Estimated_Yield_%'] * 100

# Save to CSV
output_file = 'estimated_2yr_yield_1min_duration_method_adjusted.csv'
output_data.to_csv(output_file, index=False)


print(f'Estimated yields saved to {output_file}')

# --- Step 9: Fill Missing Minutes with Last Recorded Value ---

# Ensure 'Time' is in datetime format
output_data['Time'] = pd.to_datetime(output_data['Time'])

# Set 'Time' as the index
output_data.set_index('Time', inplace=True)

# Group by date to prevent forward-filling across days
grouped = output_data.groupby(output_data.index.date)

# Create an empty list to hold resampled data
resampled_groups = []

for date, group in grouped:
    # Resample to 1-minute intervals within the group
    group_resampled = group.resample('1T').ffill()
    resampled_groups.append(group_resampled)

# Concatenate the resampled groups
output_data_resampled = pd.concat(resampled_groups)

# Reset index to have 'Time' as a column again
output_data_resampled.reset_index(inplace=True)

# --- Optional: Verify Sorting ---

# Ensure data is sorted by 'Time'
output_data_resampled.sort_values('Time', inplace=True)
output_data_resampled.reset_index(drop=True, inplace=True)

# Save to CSV
output_file = 'estimated_2yr_yield_1min_duration_method_adjusted.csv'
output_data_resampled.to_csv(output_file, index=False)

print(f'Estimated yields saved to {output_file}')

print("\nFirst 5 dates:")
print(output_data_resampled['Time'].head())
print("\nLast 5 dates:")
print(output_data_resampled['Time'].tail())
