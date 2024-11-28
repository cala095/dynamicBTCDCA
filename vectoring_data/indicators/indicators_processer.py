import pandas as pd
import os
import pandas_ta as ta

def calculate_indicators(file_path, output_file):
    # Read the data
    df = pd.read_csv(file_path)
    
    # Ensure that 'Formatted_Time' is datetime
    if 'Formatted_Time' in df.columns:
        df['Formatted_Time'] = pd.to_datetime(df['Formatted_Time'])
    elif 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.rename(columns={'datetime': 'Formatted_Time'}, inplace=True)
    else:
        print(f"Time column not found in {file_path}")
        return
    
    df.set_index('Formatted_Time', inplace=True)
    
    # Check if required columns are present
    required_columns = {'Open', 'High', 'Low', 'Close'}
    if not required_columns.issubset(df.columns):
        print(f"Required columns {required_columns} not found in {file_path}")
        return
    
    # Calculate Simple Moving Averages (SMAs)
    sma_periods = [5, 10, 20, 50, 100, 200, 500]
    for period in sma_periods:
        if len(df) >= period:
            df[f'SMA_{period}'] = ta.sma(df['Close'], length=period)
        else:
            print(f"Not enough data to compute SMA_{period} for {file_path}")
    
    # Calculate RSI
    rsi_period = 14  # Default RSI period
    if len(df) >= rsi_period:
        df['RSI'] = ta.rsi(df['Close'], length=rsi_period)
    else:
        print(f"Not enough data to compute RSI for {file_path}")
    
    # Calculate MACD
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9
    if len(df) >= macd_slow:
        macd = ta.macd(df['Close'], fast=macd_fast, slow=macd_slow, signal=macd_signal)
        df = df.join(macd)
    else:
        print(f"Not enough data to compute MACD for {file_path}")
    
    # Calculate VWAP (Volume Weighted Average Price)
    if 'Volume' in df.columns and not df['Volume'].isnull().all():
        df['VWAP'] = ta.vwap(high=df['High'], low=df['Low'], close=df['Close'], volume=df['Volume'])
    else:
        print(f"Volume data not available or insufficient for VWAP in {file_path}")
    
    # Reset index to save 'Formatted_Time' as a column
    df.reset_index(inplace=True)
    
    # Save the DataFrame with indicators
    df.to_csv(output_file, index=False)
    print(f"Indicators calculated and saved to {output_file}")

if __name__ == "__main__":
    # Define the input directories for different time frames
    base_input_dir = '../timing/resampled_data'
    base_output_dir = 'indicators_data'
    
    time_frames = [
        ('1 minute', 'm'),
        ('1 hour', 'H'),
        ('1 day', 'D'),
        ('1 week', 'W'),
        ('1 month', 'M'),
        ('1 year', 'Y')
    ]
    
    # Ensure the output base directory exists
    os.makedirs(base_output_dir, exist_ok=True)
    
    for time_frame, time_name in time_frames:
        input_dir = os.path.join(base_input_dir, time_frame)
        output_dir = os.path.join(base_output_dir, time_frame)
        os.makedirs(output_dir, exist_ok=True)
        
        input_file = os.path.join(input_dir, 'Processed_BTC_1' + time_name + '.csv')
        output_file = os.path.join(output_dir, 'Processed_BTC_with_indicators_1' + time_name + '.csv')
        
        if os.path.exists(input_file):
            print(f"Processing {input_file}...")
            calculate_indicators(input_file, output_file)
        else:
            print(f"File {input_file} not found.")
