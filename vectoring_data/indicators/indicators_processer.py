import pandas as pd
import os
import pandas_ta as ta
import numpy as np
from numba import njit
from datetime import datetime

@njit
def compute_avwap(high, low, volume, k, d, close, useHiLow):
    n = len(high)
    hiAVWAP_arr = np.full(n, np.nan)
    loAVWAP_arr = np.full(n, np.nan)
    hiAVWAP_next_arr = np.full(n, np.nan)
    loAVWAP_next_arr = np.full(n, np.nan)
    
    hi = high[0]
    lo = low[0]
    phi = hi
    plo = lo
    state = 0
    hiAVWAP_s = 0.0
    loAVWAP_s = 0.0
    hiAVWAP_v = 0.0
    loAVWAP_v = 0.0
    hiAVWAP_s_next = 0.0
    loAVWAP_s_next = 0.0
    hiAVWAP_v_next = 0.0
    loAVWAP_v_next = 0.0
    lowerBand = 20
    upperBand = 80
    lowerReversal = 20
    upperReversal = 80
    
    for idx in range(n):
        h = high[idx]
        l = low[idx]
        vol = volume[idx]
        k_val = k[idx]
        d_val = d[idx]
        c = close[idx]

        # Update phi and reset hiAVWAP_s_next under specific conditions
        if d_val < lowerBand and state != 1:
            phi = h
            hiAVWAP_s_next = 0.0
            hiAVWAP_v_next = 0.0
        elif h > phi:
            phi = h  # Update phi without resetting hiAVWAP_s_next

        # Update hi and reset hiAVWAP_s when h > hi
        if h > hi:
            hi = h
            hiAVWAP_s = 0.0
            hiAVWAP_v = 0.0

        # Update state
        prev_state = state
        if state != -1 and d_val < lowerBand:
            state = -1
        elif state != 1 and d_val > upperBand:
            state = 1

        # VWAP calculations
        if useHiLow:
            vwapHi = h
            vwapLo = l
        else:
            vwapHi = (h + l + c) / 3.0
            vwapLo = vwapHi

        # Accumulate hiAVWAP_s and hiAVWAP_s_next under different conditions
        if state == 1:
            hiAVWAP_s += vwapHi * vol
            hiAVWAP_v += vol
        else:
            hiAVWAP_s_next += vwapHi * vol
            hiAVWAP_v_next += vol

        # Similar adjustments for loAVWAP_s and loAVWAP_s_next
        if l < lo:
            lo = l
            loAVWAP_s = 0.0
            loAVWAP_v = 0.0

        if d_val > upperBand and state != -1:
            plo = l
            loAVWAP_s_next = 0.0
            loAVWAP_v_next = 0.0
        elif l < plo:
            plo = l

        if state == -1:
            loAVWAP_s += vwapLo * vol
            loAVWAP_v += vol
        else:
            loAVWAP_s_next += vwapLo * vol
            loAVWAP_v_next += vol

        # Conditional assignments
        if hi > phi and state == 1 and k_val < d_val and k_val < lowerReversal:
            hi = phi
            hiAVWAP_s = hiAVWAP_s_next
            hiAVWAP_v = hiAVWAP_v_next

        if lo < plo and state == -1 and k_val > d_val and k_val > upperReversal:
            lo = plo
            loAVWAP_s = loAVWAP_s_next
            loAVWAP_v = loAVWAP_v_next

        # Calculate AVWAPs
        hiAVWAP = hiAVWAP_s / hiAVWAP_v if hiAVWAP_v != 0.0 else np.nan
        loAVWAP = loAVWAP_s / loAVWAP_v if loAVWAP_v != 0.0 else np.nan
        hiAVWAP_next = hiAVWAP_s_next / hiAVWAP_v_next if hiAVWAP_v_next != 0.0 else np.nan
        loAVWAP_next = loAVWAP_s_next / loAVWAP_v_next if loAVWAP_v_next != 0.0 else np.nan

        # Store in arrays
        hiAVWAP_arr[idx] = hiAVWAP
        loAVWAP_arr[idx] = loAVWAP
        hiAVWAP_next_arr[idx] = hiAVWAP_next
        loAVWAP_next_arr[idx] = loAVWAP_next

    return hiAVWAP_arr, loAVWAP_arr, hiAVWAP_next_arr, loAVWAP_next_arr


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
        
        # Calculate Stochastic RSI parameters
        lengthRSI = 64
        lengthStoch = 48
        smoothK = 4
        smoothD = 4
        if len(df['Close']) >= lengthRSI:  # Check for minimum required data points
            # Calculate Stochastic RSI
            stoch_rsi = ta.stochrsi(
                close=df['Close'],
                length=lengthStoch,
                rsi_length=lengthRSI,
                k=smoothK,
                d=smoothD
            )

            # Assign the columns to the DataFrame using the correct column names
            k_col = stoch_rsi.columns[0]
            d_col = stoch_rsi.columns[1]
            
            # Convert columns to NumPy arrays
            high = df['High'].values.astype(np.float64)
            low = df['Low'].values.astype(np.float64)
            volume = df['Volume'].values.astype(np.float64)
            k = stoch_rsi[k_col].values.astype(np.float64)
            d = stoch_rsi[d_col].values.astype(np.float64)
            close = df['Close'].values.astype(np.float64)

            # Use Numba-optimized function
            useHiLow = True  # Set based on your preference

            hiAVWAP_arr, loAVWAP_arr, hiAVWAP_next_arr, loAVWAP_next_arr = compute_avwap(
                high, low, volume, k, d, close, useHiLow
            )

            # Assign results back to the DataFrame
            df['hiAVWAP'] = hiAVWAP_arr
            df['loAVWAP'] = loAVWAP_arr
            df['hiAVWAP_next'] = hiAVWAP_next_arr
            df['loAVWAP_next'] = loAVWAP_next_arr
        
        else:
            print(f"Insufficient data for StochRSI{file_path} calculation. Need at least {lengthRSI} records.") 

    else:
        print(f"Volume data not available or insufficient for VWAP in {file_path}")
    
     # Calculate Rolling Minima, Maxima, Standard Deviation, and Volatility
    window_size = 100  # Adjust based on your preference
    if len(df) >= window_size:
        # Rolling Minima and Maxima
        df[f'Rolling_Min_{window_size}'] = df['Low'].rolling(window=window_size).min()
        df[f'Rolling_Max_{window_size}'] = df['High'].rolling(window=window_size).max()
        
        # Rolling Standard Deviation
        df[f'Rolling_STD_{window_size}'] = df['Close'].rolling(window=window_size).std()
        
        # Volatility (using logarithmic returns)
        df['Log_Returns'] = np.log(df['Close'] / df['Close'].shift(1))
        df[f'Volatility_{window_size}'] = df['Log_Returns'].rolling(window=window_size).std() * np.sqrt(252)  # Adjust annualization factor as needed
        
        # Drop 'Log_Returns' column if not needed
        df.drop(columns=['Log_Returns'], inplace=True)
    else:
        print(f"Not enough data to compute rolling calculations for {file_path}")

    # Add Volume-Based Indicators
    # On-Balance Volume (OBV)
    if 'Volume' in df.columns and not df['Volume'].isnull().all():
        obv_series = ta.obv(close=df['Close'], volume=df['Volume'])
        if not obv_series.isnull().all():
            df['OBV'] = obv_series
        else:
            print(f"OBV calculation returned all NaN for {file_path}")
    else:
        print(f"Volume data not available for OBV in {file_path}")
    
    # Accumulation/Distribution Line (A/D)
    if required_columns.union({'Volume'}).issubset(df.columns):
        ad_series = ta.ad(high=df['High'], low=df['Low'], close=df['Close'], volume=df['Volume'])
        if not ad_series.isnull().all():
            df['AccDist'] = ad_series
        else:
            print(f"Accumulation/Distribution calculation returned all NaN for {file_path}")
    else:
        print(f"Required columns for Accumulation/Distribution not found in {file_path}")
    
    # Chaikin Money Flow (CMF)
    cmf_period = 20
    if len(df) >= cmf_period and required_columns.union({'Volume'}).issubset(df.columns):
        cmf_series = ta.cmf(high=df['High'], low=df['Low'], close=df['Close'], volume=df['Volume'], length=cmf_period)
        if not cmf_series.isnull().all():
            df['CMF'] = cmf_series
        else:
            print(f"CMF calculation returned all NaN for {file_path}")
    else:
        print(f"Not enough data to compute CMF or required columns missing for {file_path}")
    

    # Remove columns that are all NaN (indicators that couldn't be calculated)
    df.dropna(axis=1, how='all', inplace=True)
    # Remove Timestamp column if present (it's for BTC i want clean data at this point)
    if 'Timestamp' in df.columns:
        df.drop(columns=['Timestamp'], inplace=True)

    # Reset index to save 'Formatted_Time' as a column
    df.reset_index(inplace=True)
    
    # Save the DataFrame with indicators
    df.to_csv(output_file, index=False)
    print(f"Indicators calculated and saved to {output_file}")

if __name__ == "__main__":
    # Define the input directories for different time frames
    base_input_dir = '../timing/resampled_data'
    base_output_dir = 'indicators_data'
    tickers = ['BTC', 'DXY', 'GOLD', 'NDQ', 'US02Y', 'US10Y', 'VIX', 'SPX']
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
    for ticker in tickers:
        for time_frame, time_name in time_frames:
            input_dir = os.path.join(base_input_dir, time_frame)
            output_dir = os.path.join(base_output_dir, time_frame)
            os.makedirs(output_dir, exist_ok=True)
            
            input_file = os.path.join(input_dir, f'Processed_{ticker}_1' + time_name + '.csv')
            output_file = os.path.join(output_dir, f'Processed_{ticker}_with_indicators_1' + time_name + '.csv')
            
            if os.path.exists(input_file):
                print(f"Processing {input_file}...")
                calculate_indicators(input_file, output_file)
            else:
                print(f"File {input_file} not found.")
