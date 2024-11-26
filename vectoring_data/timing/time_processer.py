import pandas as pd
import os

def resample_data(file_path, output_base_dir):
    # Read the data
    df = pd.read_csv(file_path)

    # Ensure 'Formatted_Time' is datetime
    df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time'])

    # Set 'Formatted_Time' as index
    df.set_index('Formatted_Time', inplace=True)

    # Create the '1 minute' directory and save the original data
    minute_dir = os.path.join(output_base_dir, '1 minute')
    os.makedirs(minute_dir, exist_ok=True)
    minute_file = os.path.join(minute_dir, os.path.basename(file_path))
    df.to_csv(minute_file)

    # Initialize the data to be resampled as the 1-minute data
    current_data = df.copy()

    # Get the last timestamp in the data
    last_timestamp = current_data.index.max()

    # Define the time frames and their corresponding resampling rules
    time_frames = [
        ('1 hour', 'H'),
        ('1 day', 'D'),
        ('1 week', 'W'),
        ('1 month', 'M'),
        ('1 year', 'Y')
    ]

    # Process each time frame sequentially
    for dir_name, resample_rule in time_frames:
        # Set label and closed parameters based on resample frequency
        if resample_rule in ['W', 'M', 'Y']:
            label = 'right'
            closed = 'right'
        else:
            label = 'left'
            closed = 'right'

        # Resample the current data
        resampled = current_data.resample(resample_rule, label=label, closed=closed).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
        })

        # Drop periods with NaN values (e.g., periods with no data)
        # resampled.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)

        # Adjust filtering for 'M' and 'Y' resampling
        if resample_rule == 'W':
            # Calculate the end of the current week for last_timestamp
            last_period = (last_timestamp + pd.offsets.Week(weekday=6)).normalize()
            resampled = resampled[resampled.index <= last_period]
        elif resample_rule == 'M':
            # Calculate the end of the current month for last_timestamp
            last_period = (last_timestamp + pd.offsets.MonthEnd(0)).normalize()
            resampled = resampled[resampled.index <= last_period]
        elif resample_rule == 'Y':
            # Calculate the end of the current year for last_timestamp
            last_period = (last_timestamp + pd.offsets.YearEnd(0)).normalize()
            resampled = resampled[resampled.index <= last_period]
        else:
            # For other frequencies, use the original last_timestamp
            resampled = resampled[resampled.index <= last_timestamp]

        # Reset index to get 'Formatted_Time' back as a column
        resampled.reset_index(inplace=True)

        # Create the output directory
        output_dir = os.path.join(output_base_dir, dir_name)
        os.makedirs(output_dir, exist_ok=True)

        # Save the resampled data
        output_file = os.path.join(output_dir, os.path.basename(file_path))
        resampled.to_csv(output_file, index=False)

        # Prepare for the next iteration
        # Set 'Formatted_Time' as index again
        resampled.set_index('Formatted_Time', inplace=True)
        current_data = resampled.copy()

if __name__ == "__main__":
    # Define the input and output directories
    input_dir = '../../fetching_data/history/LIVE PROCESSED'
    output_base_dir = ''

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
