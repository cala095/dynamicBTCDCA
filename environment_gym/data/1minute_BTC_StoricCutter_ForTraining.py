import pandas as pd

def load_filter_and_save_dataframe(input_file, output_file):
    # Load the dataframe from a CSV file
    df = pd.read_csv(input_file)
    
    # Specify the columns to keep
    columns_to_keep = ['Formatted_Time', 'BTC1m_Open', 'BTC1m_High', 'BTC1m_Low', 'BTC1m_Close', 'BTC1m_Volume']
    
    # Filter the dataframe to keep only the specified columns
    filtered_df = df[columns_to_keep]
    
    # Save the filtered dataframe to a new CSV file
    filtered_df.to_csv(output_file, index=False)
    
    print(f"Filtered dataframe saved to {output_file}")



input_file = 'merged_data_1m.csv'
output_file = 'filtered_1m_btc_training.csv'
load_filter_and_save_dataframe(input_file, output_file)
