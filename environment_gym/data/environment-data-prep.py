import pandas as pd
import os



# List of 1H
files1H = ['../../vectoring_data/indicators/indicators_data/1 hour/Processed_BTC_with_indicators_1H.csv',
         '../../vectoring_data/indicators/indicators_data/1 hour/Processed_DXY_with_indicators_1H.csv',
        '../../vectoring_data/indicators/indicators_data/1 hour/Processed_GOLD_with_indicators_1H.csv',
        '../../vectoring_data/indicators/indicators_data/1 hour/Processed_NDQ_with_indicators_1H.csv',
        '../../vectoring_data/indicators/indicators_data/1 hour/Processed_SPX_with_indicators_1H.csv',
        '../../vectoring_data/indicators/indicators_data/1 hour/Processed_US02Y_with_indicators_1H.csv',
        '../../vectoring_data/indicators/indicators_data/1 hour/Processed_US10Y_with_indicators_1H.csv',
        '../../vectoring_data/indicators/indicators_data/1 hour/Processed_VIX_with_indicators_1H.csv',
         ]
# List of 1D
files1D = ['../../vectoring_data/indicators/indicators_data/1 day/Processed_BTC_with_indicators_1D.csv',
         '../../vectoring_data/indicators/indicators_data/1 day/Processed_DXY_with_indicators_1D.csv',
        '../../vectoring_data/indicators/indicators_data/1 day/Processed_GOLD_with_indicators_1D.csv',
        '../../vectoring_data/indicators/indicators_data/1 day/Processed_NDQ_with_indicators_1D.csv',
        '../../vectoring_data/indicators/indicators_data/1 day/Processed_SPX_with_indicators_1D.csv',
        '../../vectoring_data/indicators/indicators_data/1 day/Processed_US02Y_with_indicators_1D.csv',
        '../../vectoring_data/indicators/indicators_data/1 day/Processed_US10Y_with_indicators_1D.csv',
        '../../vectoring_data/indicators/indicators_data/1 day/Processed_VIX_with_indicators_1D.csv',
         ]
# List of 1W
files1W = ['../../vectoring_data/indicators/indicators_data/1 week/Processed_BTC_with_indicators_1W.csv',
         '../../vectoring_data/indicators/indicators_data/1 week/Processed_DXY_with_indicators_1W.csv',
        '../../vectoring_data/indicators/indicators_data/1 week/Processed_GOLD_with_indicators_1W.csv',
        '../../vectoring_data/indicators/indicators_data/1 week/Processed_NDQ_with_indicators_1W.csv',
        '../../vectoring_data/indicators/indicators_data/1 week/Processed_SPX_with_indicators_1W.csv',
        '../../vectoring_data/indicators/indicators_data/1 week/Processed_US02Y_with_indicators_1W.csv',
        '../../vectoring_data/indicators/indicators_data/1 week/Processed_US10Y_with_indicators_1W.csv',
        '../../vectoring_data/indicators/indicators_data/1 week/Processed_VIX_with_indicators_1W.csv',
         ]
# List of 1M
files1M = ['../../vectoring_data/indicators/indicators_data/1 year/Processed_BTC_with_indicators_1M.csv',
         '../../vectoring_data/indicators/indicators_data/1 year/Processed_DXY_with_indicators_1M.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_GOLD_with_indicators_1M.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_NDQ_with_indicators_1M.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_SPX_with_indicators_1M.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_US02Y_with_indicators_1M.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_US10Y_with_indicators_1M.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_VIX_with_indicators_1M.csv',
         ]   
# List of 1Y
files1Y = ['../../vectoring_data/indicators/indicators_data/1 year/Processed_BTC_with_indicators_1Y.csv',
         '../../vectoring_data/indicators/indicators_data/1 year/Processed_DXY_with_indicators_1Y.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_GOLD_with_indicators_1Y.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_NDQ_with_indicators_1Y.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_SPX_with_indicators_1Y.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_US02Y_with_indicators_1Y.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_US10Y_with_indicators_1Y.csv',
        '../../vectoring_data/indicators/indicators_data/1 year/Processed_VIX_with_indicators_1Y.csv',
         ]         

files1m = ['../../vectoring_data/indicators/indicators_data/1 minute/Processed_BTC_with_indicators_1m.csv']

def merger(fileList, outputname):
    # List to hold DataFrames
    dfs = []

    for file in fileList:
        # Extract prefix from file name
        tokens = os.path.basename(file).split('_')
        prefix = os.path.splitext(tokens[1] + tokens[4])[0] + '_'
        
        # Read the CSV file
        df = pd.read_csv(file)
        
        # Set "Formatted_Time" as the index
        df.set_index('Formatted_Time', inplace=True)
        
        # Rename columns with the prefix
        df.columns = [prefix + col for col in df.columns]
        
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate DataFrames horizontally
    merged_df = pd.concat(dfs, axis=1)

    # THE NETWORK IS GOING CRAZY WITH NAN
    # Replace NaN values with zero
    merged_df.fillna(0, inplace=True)

    # # Sort the index if necessary
    merged_df.sort_index(inplace=True)

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(outputname + ".csv")

if __name__ == "__main__":
    #TODO:
    # -> istruzione che recupera ogni minuto i dati da live e li appende a un file che rispetta il processing adatto al modello (vedi codice sopra)
    #praticamente questo codice deve ogni ora (-> in realtà quando vectoring data ha finito di processare i dati con indicatori etc. #TODO rimuovi la creazione di indicators per btc 1m) eseguire le istruzioni sotto 
    
    # merger(files1m, "merged_data_1m") -> no longer used we swith to take 1 minute live data from live #TODO if you are not in prod this will be used to have storic data (indicators are useless but the rest is good)
    print("** merging 1H **")
    merger(files1H, "merged_data_1H")
    print("** merging 1D **")
    merger(files1D, "merged_data_1D")
    print("** merging 1W **")
    merger(files1W, "merged_data_1W")
    print("** merging 1M **")
    merger(files1Y, "merged_data_1M")
    print("** merging 1Y **")
    merger(files1Y, "merged_data_1Y")

    print("STATUS OK")