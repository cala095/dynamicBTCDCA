import pandas as pd
import os
import shutil

def resample_data(file_path, output_base_dir):
    # Save original data as '1 minute' data
    one_minute_dir = os.path.join(output_base_dir, '1 minute')
    os.makedirs(one_minute_dir, exist_ok=True)
    one_minute_file = os.path.join(one_minute_dir, os.path.basename(file_path))
    shutil.copy(file_path, one_minute_file.replace('.csv', '_1m.csv'))
    
    # Read the data
    df = pd.read_csv(file_path)
    
    # Identify the time column and set it as index
    if 'Formatted_Time' in df.columns:
        time_col = 'Formatted_Time'
    elif 'datetime' in df.columns:
        time_col = 'datetime'
    else:
        print(f"Time column not found in {file_path}")
        return

    df[time_col] = pd.to_datetime(df[time_col])
    df.set_index(time_col, inplace=True)
    
    # Check which price columns are present
    if {'Open', 'High', 'Low', 'Close'}.issubset(df.columns):
        # Data is in OHLC format
        agg_dict = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last'
        }
    elif 'Price' in df.columns:
        # Data is in Price format
        # For resampling, use 'Price' to compute 'Open', 'High', 'Low', 'Close'
        agg_dict = {
            'Price': ['first', 'max', 'min', 'last']
        }
    else:
        print(f"No recognizable price columns found in {file_path}")
        return
    
    # Handle Volume if present
    if 'Volume' in df.columns:
        agg_dict['Volume'] = 'sum'
    
    # Define the time frames and their corresponding resampling rules
    time_frames = [
        ('1 hour', 'H', 'left', 'right'),
        ('1 day', 'D', 'left', 'right'),
        ('1 week', 'W', 'right', 'right'),
        ('1 month', 'M', 'right', 'right'),
        ('1 year', 'Y', 'right', 'right')
    ]
    
    # Process each time frame independently
    for dir_name, resample_rule, label, closed in time_frames:
        # Resample the data
        resampled = df.resample(resample_rule, label=label, closed=closed).agg(agg_dict)
        
        # If 'Price' was used, flatten MultiIndex columns and rename
        if 'Price' in df.columns:
            # Flatten the MultiIndex columns
            resampled.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in resampled.columns.values]
            # Rename columns to standard OHLC names
            column_mapping = {
                'Price_first': 'Open',
                'Price_max': 'High',
                'Price_min': 'Low',
                'Price_last': 'Close'
            }
            if 'Volume' or 'Volume_sum' in df.columns:
                column_mapping['Volume'] = 'Volume'
                column_mapping['Volume_sum'] = 'Volume'
            resampled.rename(columns=column_mapping, inplace=True)
        else:
            # For OHLC data, columns are already named correctly
            pass
        
        # Drop periods with NaN values in OHLC columns
        resampled.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
        
        # Reset index to get the time column back as a column
        resampled.reset_index(inplace=True)
        
        # Create the output directory
        output_dir = os.path.join(output_base_dir, dir_name)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the resampled data to CSV
        output_file = os.path.join(output_dir, os.path.basename(file_path))
        resampled.to_csv(output_file.replace('.csv','_1' + resample_rule + '.csv'), index=False)

if __name__ == "__main__":
    # Define the input and output directories
    input_dir = '../../fetching_data/history/LIVE PROCESSED'
    output_base_dir = 'resampled_data'
    
    # Ensure the input directory exists
    if not os.path.exists(input_dir):
        print(f"Input directory {input_dir} does not exist.")
        exit(1)
    
    # Get the list of CSV files in the input directory
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv') and "duplicated_records" not in f]
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}.")
        exit(1)
    
    # Process each file
    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        print(f"Processing {file_path}...")
        resample_data(file_path, output_base_dir)
        print(f"Finished processing {file_path}.")
