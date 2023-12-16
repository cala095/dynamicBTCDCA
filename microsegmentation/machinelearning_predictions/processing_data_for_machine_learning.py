import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import joblib

try:
    # Create MachineLearningData directory if it doesn't exist
    if not os.path.exists('MachineLearningData'):
        os.makedirs('MachineLearningData')

    # Load the dataset
    data = pd.read_csv('DataCombined/combined_data.csv', index_col='time', parse_dates=True)
    data.drop("symbol", axis=1, inplace=True)
    # Fill missing values
    data.fillna(method='ffill', inplace=True)
    data.fillna(method='bfill', inplace=True)

    # Scale the features
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(data)

    # Create a DataFrame from the scaled data
    scaled_df = pd.DataFrame(scaled_data, columns=data.columns, index=data.index)

    # Save the preprocessed data to a CSV file
    scaled_df.to_csv('MachineLearningData/preprocessed_dataset.csv')

    # Save the scaler to a file
    joblib.dump(scaler, 'MachineLearningData/scaler.joblib')

    # a message to communicate success
    print("new data to make accurate predictions has been processed")

except Exception as e:
        print("Error in processing_data_for_machine_learning:", e)