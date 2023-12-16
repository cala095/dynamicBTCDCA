import argparse
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
import joblib
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

def make_n_predictions(n, recent_data):
    recent_feature_set = pd.DataFrame([recent_data])

    # Load your trained models LSTM
    high_model = load_model("80_trained_models_lstmV2/high_lstm_model.h5")
    low_model = load_model("80_trained_models_lstmV2/low_lstm_model.h5")
    close_model = load_model("80_trained_models_lstmV2/close_lstm_model.h5")

    predictions = []

    for i in range(n):
        # Make predictions for the next hour
        lstm_input = recent_feature_set.to_numpy().reshape(1, -1, 1)  # Reshape the input data to match the LSTM's input shape
        high_pred = high_model.predict(lstm_input)
        low_pred = low_model.predict(lstm_input)
        close_pred = close_model.predict(lstm_input)
        
        # Store the predictions
        predictions.append({"hour": i + 1, "high": high_pred[0][0], "low": low_pred[0][0], "close": close_pred[0][0]})
        
        # Update the feature set with the predicted values
        recent_feature_set = generate_next_hour_features(recent_feature_set, high_pred, low_pred, close_pred)

    return predictions

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make predictions for the next n hours.')
    parser.add_argument('n', type=int, help='number of hours to make predictions for')
    args = parser.parse_args()

    predictions = make_n_predictions(args.n, recent_data)

    print(f"Predictions for the next {args.n} hours:")
    for prediction in predictions:
        print(f"Hour {prediction['hour']} - High: {prediction['high']}, Low: {prediction['low']}, Close: {prediction['close']}")

    # Load the saved scaler
    scaler = joblib.load('MachineLearningData/scaler.joblib')

    # Prepare the predictions as an array with the same shape as the original data
    prediction_array = np.zeros((args.n, 25))
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
    print(f"Reverted predictions for the next {args.n} hours:")
    for i, prediction in enumerate(inverse_predictions):
        print(f"Hour {i + 1} - High: {prediction[high_idx]}, Low: {prediction[low_idx]}, Close: {prediction[close_idx]}")

    # Create predictions directory if it doesn't exist
    if not os.path.exists('predictions'):
        os.makedirs('predictions')
    with open('predictions/predictions_LSTM.txt', 'a') as file:
        file.write(f"completed at {datetime.now()}\n")
        file.write("Timestamp,High,Low,Close\n")
        for i, prediction in enumerate(inverse_predictions):
            start_timestamp = datetime.strptime(preprocessed_data.index[-1], '%Y-%m-%d %H:%M:%S%z')
            timestamp = start_timestamp + timedelta(hours=i+1)
            file.write(f"{timestamp},{prediction[high_idx]},{prediction[low_idx]},{prediction[close_idx]}\n")
            print(f"{timestamp} - High: {prediction[high_idx]}, Low: {prediction[low_idx]}, Close: {prediction[close_idx]}")

