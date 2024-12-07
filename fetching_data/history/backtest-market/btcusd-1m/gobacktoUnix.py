import pandas as pd

def convert_to_unix(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Convert Formatted_Time to Unix timestamp
    df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time']).astype('int64') // 10**9
    df = df.drop('Timestamp', axis=1)
    
    # Save to new file
    output_file = 'unix_' + csv_file
    df.to_csv(output_file, index=False)
    
# Usage example
convert_to_unix('merged_output.csv')

