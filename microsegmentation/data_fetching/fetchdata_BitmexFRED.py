import sys
import requests
import pandas as pd
import ta
import os
import time
from fredapi import Fred
import yfinance as yf

# Replace with your actual BitMEX API key
bitmex_api_key = 'TlKnPffHH1bIjX7Ea_A1xKLW'
# Replace with your actual FRED API key
fred_api_key = 'babcf0538b741e4c985fde31a43ed37c'

# Initialize FRED API
fred = Fred(api_key=fred_api_key)

url = 'https://www.bitmex.com/api/v1/trade/bucketed'

# Function to fetch FRED data
def fetch_fred_data(series_id):
    try:
        data = fred.get_series(series_id)
        return data
    except Exception as e:
        print(f"Error fetching FRED data for series {series_id}: {e}")
        return None

#recovers historycal data of the last 8 years
# initialDate = '2023-03-01' #if you want to test things and get faster execution time
initialDate = '2015-09-01'
start_time = pd.Timestamp(initialDate) #you can update this param to check if you want only updated values
end_time = start_time + pd.Timedelta(hours=1000)
params = {
    'binSize': '1h',
    'partial': 'false',
    'symbol': 'XBTUSD',
    'count': 1000,
    'reverse': 'false',
    'startTime': start_time.isoformat(),
    'endTime': end_time.isoformat(),
    'api_key': bitmex_api_key
}

data_frames = []
loop = False
while end_time <= pd.Timestamp.now():
    print(f"requesting data for start-time: {start_time} , end-time: {end_time}")
    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code == 200:
        df_new = pd.DataFrame(data)
        data_frames.append(df_new)

        # # Save raw data to file
        # if not os.path.exists('PriceData'):
        #     os.makedirs('PriceData')
        # with open('PriceData/raw_data.csv', 'a', encoding="utf-8") as f:
        #     df_new.to_csv(f, header=f.tell() == 0, index=False)

        # Update time range for next API request
        if(loop): # loop escape from looping in last hour with the while
            break
        start_time = end_time
        if(start_time + pd.Timedelta(hours=1000) >= pd.Timestamp.now()): #checks to not go over current hour, to recover it
            end_time = pd.Timestamp.now()
            loop = True
        else:
            end_time = start_time + pd.Timedelta(hours=1000)
        params['startTime'] = start_time.isoformat()
        params['endTime'] = end_time.isoformat()
        print("waiting 2 seconds to avoid rate limit errors")
        time.sleep(2)  # Add a delay to avoid rate limit errors
    else:
        print(f"Error: API request failed with status code {response.status_code}")
        print(response.text)
        sys.exit(1)


# Concatenate data frames and preprocess data
df_all = pd.concat(data_frames, ignore_index=True)
df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
df_all.set_index('timestamp', inplace=True)

# Calculate technical indicators
df_all['SMA_10'] = ta.trend.SMAIndicator(df_all['close'], window=10).sma_indicator()
df_all['SMA_30'] = ta.trend.SMAIndicator(df_all['close'], window=30).sma_indicator()
df_all['RSI'] = ta.momentum.RSIIndicator(df_all['close']).rsi()

macd = ta.trend.MACD(df_all['close'])
df_all['MACD'] = macd.macd()
df_all['MACD_signal'] = macd.macd_signal()
df_all['MACD_diff'] = macd.macd_diff()

# Fetch FRED data
eur_data = fetch_fred_data('DEXUSEU')
us10y_data = fetch_fred_data('GS10')
us02y_data = fetch_fred_data('GS2')
m2_data = fetch_fred_data('WM2NS')
fedbalance_data = fetch_fred_data('WALCL')

# Print FRED data series
print("EUR data:")
print(eur_data)
print("\nUS 10-Year Treasury Yield data:")
print(us10y_data)
print("\nUS 2-Year Treasury Yield data:")
print(us02y_data)
print("\nM2 Money Supply data:")
print(m2_data)
print("\nFederal Reserve Balance Sheet data:")
print(fedbalance_data)

eur_data_hourly = eur_data.resample('H').ffill()
us10y_data_hourly = us10y_data.resample('H').ffill()
us02y_data_hourly = us02y_data.resample('H').ffill()
m2_data_hourly = m2_data.resample('H').ffill()
fedbalance_data_hourly = fedbalance_data.resample('H').ffill()

# Fetch hourly DXY data from Yahoo Finance starting from 2015
dxy_data = yf.download(tickers='DX-Y.NYB', start=initialDate)
dxy_data = dxy_data['Close']
# Resample daily data to hourly data
dxy_data = dxy_data.resample('1H').interpolate(method='time')
# Merge DXY data with the main DataFrame
df_all = df_all.merge(dxy_data.tz_localize('UTC').rename('DXY'), left_index=True, right_index=True, how='left')

# Merge FRED data with the main DataFrame
df_all = df_all.merge(eur_data_hourly.tz_localize('UTC').rename('EUR'), left_index=True, right_index=True, how='left')
df_all = df_all.merge(us10y_data_hourly.tz_localize('UTC').rename('US10Y'), left_index=True, right_index=True, how='left')
df_all = df_all.merge(us02y_data_hourly.tz_localize('UTC').rename('US02Y'), left_index=True, right_index=True, how='left')
df_all = df_all.merge(m2_data_hourly.tz_localize('UTC').rename('M2'), left_index=True, right_index=True, how='left')
df_all = df_all.merge(fedbalance_data_hourly.tz_localize('UTC').rename('FedBalance'), left_index=True, right_index=True, how='left')

# Interpolate missing values for FRED data and DXY
df_all['DXY'] = df_all['DXY'].interpolate(method='time')
df_all['EUR'] = df_all['EUR'].interpolate(method='time')
df_all['US10Y'] = df_all['US10Y'].interpolate(method='time')
df_all['US02Y'] = df_all['US02Y'].interpolate(method='time')
df_all['M2'] = df_all['M2'].interpolate(method='time')
df_all['FedBalance'] = df_all['FedBalance'].interpolate(method='time')

# Save processed data to a CSV file
if not os.path.exists('PriceData'):
    os.makedirs('PriceData')
df_all.to_csv('PriceData/price_report.csv', index_label='time')
print("Processed data saved to 'price_report.csv'")

