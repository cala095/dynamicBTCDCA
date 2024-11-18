# shy_data = pd.read_csv('shy (1 - 3 years maturity U.S. T.NOTE) 1m\\shy_intraday_merged.csv')
# yield_data = pd.read_csv('U.S. 2y yield 1d\\usty2rt_daily_historical-data-11-15-2024.csv')
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt

# --- Step 1: Load SHY Intraday Data ---

# Replace with the path to your merged SHY data file
shy_data = pd.read_csv('shy (1 - 3 years maturity U.S. T.NOTE) 1m\\shy_intraday_merged.csv')

# Convert 'Time' column to datetime
shy_data['Time'] = pd.to_datetime(shy_data['Time'])

# Ensure data is sorted chronologically
shy_data.sort_values('Time', inplace=True)
shy_data.reset_index(drop=True, inplace=True)

# Extract date from 'Time'
shy_data['Date'] = shy_data['Time'].dt.date

# --- Step 2: Load 2-Year Treasury Yield Data ---

# Replace with the path to your 2-year yield data file
yield_data = pd.read_csv('U.S. 2y yield 1d\\usty2rt_daily_historical-data-11-15-2024.csv', skipfooter=1, engine='python')

# Remove any rows where 'Time' does not match the expected date format
# Define a function to check if a string matches the date format 'mm/dd/yyyy'
def is_valid_date(date_str):
    return bool(re.match(r'\d{2}/\d{2}/\d{4}', str(date_str)))

# Filter the DataFrame
yield_data = yield_data[yield_data['Time'].apply(is_valid_date)].copy()

# Convert 'Time' column to datetime
yield_data['Time'] = pd.to_datetime(yield_data['Time'], format='%m/%d/%Y')

# Ensure data is sorted chronologically
yield_data.sort_values('Time', inplace=True)
yield_data.reset_index(drop=True, inplace=True)

# Keep necessary columns
yield_data = yield_data[['Time', 'Last']]

# Convert yield to percentage
yield_data['Yield_%'] = yield_data['Last'] * 100

# Extract date and month from 'Time'
yield_data['Date'] = yield_data['Time'].dt.date
yield_data['Month'] = yield_data['Time'].dt.to_period('M')

# --- Step 3: Calculate Monthly High and Low Yields ---

monthly_high_low = yield_data.groupby('Month')['Yield_%'].agg(Monthly_High='max', Monthly_Low='min').reset_index()

# --- Step 4: Prepare Initial Yield Data ---

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

# Merge monthly high/low with SHY data
shy_data['Month'] = shy_data['Time'].dt.to_period('M')
shy_data = pd.merge(shy_data, monthly_high_low, on='Month', how='left')

# --- Step 5: Set Modified Duration ---

# Set the modified duration (adjust based on SHY's reported duration)
D_mod = 1.9  # Modify this value if you have a more accurate duration

# --- Step 6: Calculate Price Returns and Yield Changes ---

# Calculate price returns
shy_data['Price_Return'] = shy_data['Last'].pct_change()

# Handle the first value (which will be NaN)
shy_data['Price_Return'].fillna(0, inplace=True)

# Calculate yield changes
shy_data['Delta_Yield_%'] = -shy_data['Price_Return'] / D_mod

# --- Step 7: Estimate the 2-Year Yield Over Time with Constraints ---

# Initialize the estimated yield list
estimated_yields = []

for idx, row in shy_data.iterrows():
    if idx == 0 or row['Date'] != shy_data.loc[idx - 1, 'Date']:
        # Start of a new day; use the initial yield
        estimated_yield = row['Initial_Yield_%']
    else:
        # Calculate the new yield
        estimated_yield = estimated_yields[-1] + row['Delta_Yield_%']
    
    # Apply monthly high/low constraints
    if estimated_yield > row['Monthly_High']:
        estimated_yield = row['Monthly_High']
    elif estimated_yield < row['Monthly_Low']:
        estimated_yield = row['Monthly_Low']
    
    estimated_yields.append(estimated_yield)

# Add the estimated yields to the DataFrame
shy_data['Estimated_Yield_%'] = estimated_yields

# --- Step 8: Handle Days Without SHY Data ---

# Get a list of all dates from the yield data
all_dates = pd.to_datetime(yield_data['Date'].unique())

# Get a list of dates present in SHY data
shy_dates = pd.to_datetime(shy_data['Date'].unique())

# Identify dates missing in SHY data
missing_shy_dates = np.setdiff1d(all_dates, shy_dates)

# For missing SHY dates, create entries using the daily yield
missing_yield_data = yield_data[yield_data['Date'].isin(missing_shy_dates)][['Date', 'Yield_%', 'Month']].copy()

# Assign 'Time' to midday for consistency
missing_yield_data['Time'] = pd.to_datetime(missing_yield_data['Date']) + pd.Timedelta(hours=12)

# Assign 'Estimated_Yield_%' from 'Yield_%'
missing_yield_data['Estimated_Yield_%'] = missing_yield_data['Yield_%']

# Merge monthly high/low with missing yield data
missing_yield_data = pd.merge(missing_yield_data, monthly_high_low, on='Month', how='left')

# Ensure estimated yields are within monthly bounds
missing_yield_data['Estimated_Yield_%'] = missing_yield_data['Estimated_Yield_%'].clip(
    lower=missing_yield_data['Monthly_Low'], upper=missing_yield_data['Monthly_High']
)

# --- Step 9: Combine Data and Output ---

# Prepare the output data
output_data = pd.concat([
    shy_data[['Time', 'Estimated_Yield_%']],
    missing_yield_data[['Time', 'Estimated_Yield_%']]
], ignore_index=True)

# Convert 'Time' to desired string format
output_data['Time'] = output_data['Time'].dt.strftime('%m/%d/%Y %H:%M')

# Sort the output data by Time
output_data.sort_values('Time', inplace=True, ascending=True)
output_data.reset_index(drop=True, inplace=True)

# Save to CSV
output_file = 'estimated_2yr_yield_1min_duration_method_with_constraints.csv'
output_data.to_csv(output_file, index=False)

print(f'Estimated yields saved to {output_file}')

# ordering it since it has mixed time format
# Read the CSV file
df = pd.read_csv('estimated_2yr_yield_1min_duration_method_with_constraints.csv')
# Convert Time column with mixed format handling
df['Time'] = pd.to_datetime(df['Time'], format='mixed', dayfirst=False)
# Sort the values
df_sorted = df.sort_values('Time')
# Reset index
df_sorted = df_sorted.reset_index(drop=True)
# Save sorted file
df_sorted.to_csv('estimated_2yr_yield_1min_duration_method_with_constraints.csv', index=False)

# Verify sorting
print("\nFirst 5 dates:")
print(df_sorted['Time'].head())
print("\nLast 5 dates:")
print(df_sorted['Time'].tail())

# --- Optional: Plotting the Estimated Yields ---

# Uncomment the following lines if you wish to visualize the results

plt.figure(figsize=(12, 6))
plt.plot(pd.to_datetime(output_data['Time']), output_data['Estimated_Yield_%'], label='Estimated Yield')
plt.xlabel('Time')
plt.ylabel('2-Year Treasury Yield (%)')
plt.title('Estimated 2-Year Treasury Yield Over Time')
plt.legend()
plt.show()
