import argparse
import joblib
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

preprocessed_data = pd.read_csv('MachineLearningData/preprocessed_dataset.csv', index_col=0)

# Get the most recent data from preprocessed_data
recent_data = preprocessed_data.iloc[-1].to_dict()

def generate_next_hour_features(recent_feature_set, high_pred, low_pred, close_pred):
    next_hour_features = recent_feature_set.copy()
    next_hour_features["open"] = recent_feature_set["close"]
    next_hour_features["high"] = high_pred
    next_hour_features["low"] = low_pred
    next_hour_features["close"] = close_pred
    return next_hour_features

def make_n_predictions_rf(n, recent_data):
    recent_feature_set = pd.DataFrame([recent_data])

    # Load your trained models
    high_model = joblib.load("80_trained_models_forestV2/high_rf_model.joblib")
    low_model = joblib.load("80_trained_models_forestV2/low_rf_model.joblib")
    close_model = joblib.load("80_trained_models_forestV2/close_rf_model.joblib")

    predictions = []

    for i in range(n):
        # Make predictions for the next hour
        high_pred = high_model.predict(recent_feature_set)
        low_pred = low_model.predict(recent_feature_set)
        close_pred = close_model.predict(recent_feature_set)
        
        # Store the predictions
        predictions.append({"hour": i + 1, "high": high_pred[0], "low": low_pred[0], "close": close_pred[0]})
        
        # Update the feature set with the predicted values
        recent_feature_set = generate_next_hour_features(recent_feature_set, high_pred, low_pred, close_pred)

    return predictions

parser = argparse.ArgumentParser(description='Make n predictions for stock prices')
parser.add_argument('n', type=int, help='Number of predictions to make')
args = parser.parse_args()

n = args.n

predictions = make_n_predictions_rf(n, recent_data)

print(f"Predictions for the next {n} hours:")
for prediction in predictions:
    print(f"Hour {prediction['hour']} - High: {prediction['high']}, Low: {prediction['low']}, Close: {prediction['close']}")

# Load the saved scaler
scaler = joblib.load('MachineLearningData/scaler.joblib')

# Prepare the predictions as an array with the same shape as the original data
prediction_array = np.zeros((n, 25))
high_idx = preprocessed_data.columns.get_loc("high")
low_idx = preprocessed_data.columns.get_loc("low")
close_idx = preprocessed_data.columns.get_loc("close")

for i, prediction in enumerate(predictions):
    prediction_array[i, high_idx] = prediction['high']
    prediction_array[i, low_idx] = prediction['low']
    prediction_array[i, close_idx] = prediction['close']

# Revert the predictions to their original values
inverse_predictions = scaler.inverse_transform(prediction_array)

# Print the reverted predictions
start_timestamp = datetime.strptime(preprocessed_data.index[-1], '%Y-%m-%d %H:%M:%S%z')
print(f"Reverted predictions for the next {n} hours:")
for i, prediction in enumerate(inverse_predictions):
    print(f"Hour {start_timestamp + timedelta(hours=i+1)} - High: {prediction[high_idx]}, Low: {prediction[low_idx]}, Close: {prediction[close_idx]}")

# Create predictions directory if it doesn't exist
if not os.path.exists('predictions'):
    os.makedirs('predictions')
with open('predictions/predictions_RF.txt', 'a') as file:
    file.write(f"completed at {datetime.now()}\n")
    file.write("Timestamp,High,Low,Close\n")
    for i, prediction in enumerate(inverse_predictions):
        timestamp = start_timestamp + timedelta(hours=i+1)
        file.write(f"{timestamp},{prediction[high_idx]},{prediction[low_idx]},{prediction[close_idx]}\n")