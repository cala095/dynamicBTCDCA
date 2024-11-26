import pandas as pd
import os


def check_duplicates(file_path, report_file_path):
    # Read the data with headers
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return
    
    # Identify the time column
    time_col = 'Formatted_Time' if 'Formatted_Time' in df.columns else 'datetime'
    if time_col not in df.columns:
        print(f"No time column found in {file_path}")
        return
    
    # Convert time column to datetime
    try:
        df[time_col] = pd.to_datetime(df[time_col])
    except Exception as e:
        print(f"Error converting time column in {file_path}: {e}")
        return
    
    # Check for duplicates
    duplicates = df[df[time_col].duplicated(keep=False)]
    
    if not duplicates.empty:
        # Ensure the directory for the report exists
        report_file_dir = os.path.dirname(report_file_path)
        os.makedirs(report_file_dir, exist_ok=True)
        
        # Write duplicates to report file
        duplicates.to_csv(report_file_path, index=False)
        print(f"Found {len(duplicates)} duplicates in {file_path}. Report saved to {report_file_path}")
    else:
        print(f"No duplicates found in {file_path}")

if __name__ == "__main__":
    # Define the input directory and report directory
    input_dir = 'resampled_data'
    report_dir = 'duplicate_reports'
    
    # Ensure the input directory exists
    if not os.path.exists(input_dir):
        print(f"Input directory {input_dir} does not exist.")
        exit(1)
    
    # Walk through all directories and files in input_dir
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                # Get the relative path from input_dir
                relative_path = os.path.relpath(file_path, input_dir)
                # Construct report_file_path
                report_file_path = os.path.join(report_dir, relative_path)
                # Replace the original .csv with _duplicates.csv
                report_file_path = os.path.splitext(report_file_path)[0] + '_duplicates.csv'
                # Call check_duplicates
                check_duplicates(file_path, report_file_path)