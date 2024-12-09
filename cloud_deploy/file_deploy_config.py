import os
import shutil

# Define the source directory
source_dir = "data"

# Define the files to move and their destinations
file_destinations = {
    "Processed_BTC.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_DXY.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_GOLD.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_NDQ.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_SPX.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_US02Y.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_US10Y.csv": "../fetching_data/history/LIVE PROCESSED",
    "Processed_VIX.csv": "../fetching_data/history/LIVE PROCESSED",
    "BTCUSD_data.csv": "../fetching_data/live/PriceData",
    "DXY_data.csv": "../fetching_data/live/PriceData",
    "GOLD_data.csv": "../fetching_data/live/PriceData",
    "NDQ_data.csv": "../fetching_data/live/PriceData",
    "SPX_data.csv": "../fetching_data/live/PriceData",
    "US02Y_data.csv": "../fetching_data/live/PriceData",
    "US10Y_data.csv": "../fetching_data/live/PriceData",
    "VIX_data.csv": "../fetching_data/live/PriceData",
    "apikey.txt": "../fetching_data/live",
    "gmail_credential.json": "../fetching_data/live",
    "token.json": "../fetching_data/live"
}

# Move the files
for filename, destination in file_destinations.items():
    source_path = os.path.join(source_dir, filename)
    destination_path = os.path.join(destination, filename)
    
    # Check if the source file exists
    if os.path.exists(source_path):
        # Create the destination directory if it doesn't exist
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        
        # Move the file
        shutil.move(source_path, destination_path)
        print(f"Moved {filename} to {destination}")
    else:
        print(f"File {filename} not found in the source directory")
