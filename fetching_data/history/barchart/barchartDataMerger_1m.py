import pandas as pd
import glob
import os
#IT DOES 1 ONE OF THE TWO I DIDN'T HAVE TIME TO AUTOMATE IT SIMPLY RUN IT TWO TIMES
# Folder containing your CSV files
folder_path = 'shy (1 - 3 years maturity U.S. T.NOTE) 1m'
# folder_path2 = 'tnx 1m'

# File pattern to match your CSV files
file_pattern = os.path.join(folder_path, 'shy_intraday-1min_historical-data*.csv')
# file_pattern2 = os.path.join(folder_path2, 'tnx_intraday-1min_historical-data*.csv')

# Get all file names matching the pattern
csv_files = glob.glob(file_pattern)

print(f"Found {len(csv_files)} files.")

# List to hold DataFrames
df_list = []

for file in csv_files:
    print(f"Processing file: {file}")
    # Read the CSV file, skipping the footer line
    df = pd.read_csv(file, skipfooter=1, engine='python')
    
    # Remove any rows that are completely empty (in case of malformed data)
    df.dropna(how='all', inplace=True)
    
    # Append to the list
    df_list.append(df)

# Concatenate all DataFrames
combined_df = pd.concat(df_list, ignore_index=True)

# Convert 'Time' column to datetime
combined_df['Time'] = pd.to_datetime(combined_df['Time'], format='%m/%d/%Y %H:%M')

# Remove duplicates based on 'Time' column
combined_df.drop_duplicates(subset='Time', inplace=True)

# Sort the data by 'Time'
combined_df.sort_values('Time', inplace=True)

# Reset index
combined_df.reset_index(drop=True, inplace=True)

# Output file path
output_file = os.path.join(folder_path, 'shy_intraday_merged.csv')

# Save to CSV
combined_df.to_csv(output_file, index=False)

print(f"Data successfully merged and saved to {output_file}")
