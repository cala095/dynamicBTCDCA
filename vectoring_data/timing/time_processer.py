import pandas as pd
import os

def resample_data(file_path, output_base_dir):
    # Read the data with assigned headers
    df = pd.read_csv(file_path)
    
    # Ensure 'Formatted_Time' is datetime
    df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time'])
    
    # Set 'Formatted_Time' as index
    df.set_index('Formatted_Time', inplace=True)
    
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
        # Resample the original data
        resampled = df.resample(resample_rule, label=label, closed=closed).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
        })
        
        # Drop periods with NaN values
        resampled.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
        
        # Reset index to get 'Formatted_Time' back as a column
        resampled.reset_index(inplace=True)
        
        # Create the output directory
        output_dir = os.path.join(output_base_dir, dir_name)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the resampled data
        output_file = os.path.join(output_dir, os.path.basename(file_path))
        resampled.to_csv(output_file, index=False)

if __name__ == "__main__":
    # Define the input and output directories
    input_dir = '../../fetching_data/history/LIVE PROCESSED'
    output_base_dir = 'resampled_data'  # Set your desired output base directory
    
    # Ensure the input directory exists
    if not os.path.exists(input_dir):
        print(f"Input directory {input_dir} does not exist.")
        exit(1)
    
    # Get the list of .csv files in the input directory
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('BTC.csv')]
    
    if not csv_files:
        print(f"No .csv files found in {input_dir}.")
        exit(1)
    
    # Process each file
    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        print(f"Processing {file_path}...")
        resample_data(file_path, output_base_dir)
        print(f"Finished processing {file_path}.")