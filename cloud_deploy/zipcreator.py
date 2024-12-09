import os
import zipfile

def create_zip(file_paths, zip_name):
    with zipfile.ZipFile(zip_name, 'w') as zipf:
        for file_name, file_dir in file_paths.items():
            file_path = os.path.join(file_dir, file_name)
            if os.path.exists(file_path):
                zipf.write(file_path, arcname=file_name)
            else:
                print(f"File not found: {file_path}")

file_paths = {
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
    "gmail_credential.json": "gmail credentials",
    "token.json": "gmail credentials"
}

zip_name = "files_archive.zip"
create_zip(file_paths, zip_name)

print(f"Zip file '{zip_name}' created successfully.")
