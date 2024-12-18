import os
import sys
import time
from google.cloud import storage

# Get the list of CSV files in the input directory -> for size check
def is_valid_csv(filename):
    return (filename.endswith('.csv') and 
            "duplicated_records" not in filename and 
            "filtered_1m_btc_training" not in filename and #in theory useless -> the file is present only on local repo for model training
            "processed_btc_last_line_1m" not in filename and #usefull
            "missing_minutes" not in filename)

def is_valid_csv_upload(filename): #-> for upload (discard what we dont want from bkp and env files upload) (now they are equal but in the future...)
    return (filename.endswith('.csv') and 
            "duplicated_records" not in filename and 
            "filtered_1m_btc_training" not in filename and #in theory useless -> the file is present only on local repo for model training
            "processed_btc_last_line_1m" not in filename and #usefull
            "missing_minutes" not in filename)


def get_files_size(dirpath, files):
    total_size = 0
    for f in files:
        fp = os.path.join(dirpath, f)
        total_size += os.path.getsize(fp)
    return total_size

def upload_csv_files(local_directory, bucket_name, prefix=""):
    """
    Upload all .csv files from `local_directory` to the specified GCS bucket.
    If a file is not found at the time of listing, it simply prints a message and continues.

    :param local_directory: str, path to the local directory containing .csv files.
    :param bucket_name: str, the name of your GCS bucket.
    :param prefix: str, optional prefix (folder path) in the bucket.
    """
    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Ensure prefix ends with a slash if provided
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    # List all .csv files in the local_directory
    for filename in os.listdir(local_directory):
        if filename.lower().endswith(".csv"):
            file_path = os.path.join(local_directory, filename)
            if os.path.exists(file_path) and is_valid_csv_upload(filename):
                blob = bucket.blob(prefix + filename)
                blob.upload_from_filename(file_path)
                print(f"Uploaded {filename} to gs://{bucket_name}/{prefix}{filename}")
            elif not is_valid_csv_upload(filename):
                print(f"File {filename} is inside discard list (->is_valid_csv_upload). Skipping.")
            else:
                # This branch theoretically shouldn’t happen since we got the filename from os.listdir,
                # but if a file disappears mid-execution, we handle it gracefully.
                print(f"File {filename} was not found. Skipping.")


if __name__ == "__main__":

    folder_path_bkp = '../fetching_data/history/LIVE PROCESSED'  # Replace with the actual folder path
    csv_files = [f for f in os.listdir(folder_path_bkp) if is_valid_csv(f)]

    # Read the last recorded size from a file
    try:
        with open('last_size_LIVE-PROCESSED.txt', 'r') as file:
            last_size = int(file.read().strip())
    except FileNotFoundError:
        last_size = 2252950726  # Default value if file not found

    #WE CAN HAVE THE PROGRAM STUCK WHEN processer.py is rewriting the files #TODO imlement lock to avoid possibile starting uploads with incomplete files that goes over the last dimensions
    start_time = time.time()
    while True:
        # Check if 15 minutes have passed
        elapsed_time = time.time() - start_time
        # Get the current folder size
        current_size = get_files_size(folder_path_bkp, csv_files)

        # Compare the current size with the last recorded size
        if current_size >= last_size:
            print(f"The folder size is: {current_size} bytes")
            break
        else:
            print(f"The folder size is: {current_size} bytes")
            print(f"DIMENSION LESS THAN EXPECTED, wait 10s, time lapsed:{elapsed_time}/900 ")
        if elapsed_time >= 15 * 60:  # 15 minutes * 60 seconds
            print(f"time lapsed:{elapsed_time} on 900 -> exiting")
            sys.exit(1)
        # Wait 10 seconds before the next check
        time.sleep(10)


    

    # Write the current size to the file
    with open('last_size_LIVE-PROCESSED.txt', 'w') as file:
        file.write(str(current_size))

    # usage:
    # local_directory: The folder on your VM where CSV files are located.
    # bucket_name: The name of your GCS bucket.
    # prefix: (optional) A 'directory' within the bucket to store files under.
    local_directory_bkp = "../fetching_data/history/LIVE PROCESSED"
    local_directory_env = "../environment_gym/data"
    bucket_name_bkp = "backup-data-processed_timing"  # You must create this bucket beforehand
    bucket_name_env = "data-processed-prod-env"  # You must create this bucket beforehand
    prefix = "" #"optional/subfolder"

    print("** uploading bkp**")
    upload_csv_files(local_directory_bkp, bucket_name_bkp, prefix)
    print("** bkp uploaded**")
    print("** uploading env**")
    upload_csv_files(local_directory_env, bucket_name_env, prefix)
    print("** env uploaded**")


    print('STATUS OK') #-> without this we stop