import pandas as pd

def adjust_volume(df):
    # Vectorized comparison of all OHLC columns at once
    mask = (df[['Open', 'High', 'Low', 'Close']].shift(1) == 
            df[['Open', 'High', 'Low', 'Close']]).all(axis=1)
    df.loc[mask, 'Volume'] = 0
    return df

# Read and process the file
df = pd.read_csv('output.csv')
df_adjusted = adjust_volume(df)
df_adjusted.to_csv('adjusted_output.csv', index=False)
