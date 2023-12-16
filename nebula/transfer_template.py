import os
import subprocess
import time

# Set source and destination details
src_folder = "PriceData"
dest_user = "dynamicbtcdca"
dest_host = "10.0.0.5"
dest_path = "/home/dynamicbtcdca/PriceData"

if os.path.exists(src_folder) is False:
    print(f"{src_folder} not found")
while True:
        # Check if source folder exists
        subprocess.run(["python3", "fetchdata_BitmexFRED.py"])
        # Check if source folder exists
        if os.path.exists(src_folder):
            # Get list of files in source folder
            files = os.listdir(src_folder)
            # Transfer each file using scp
            for file in files:
                print(f"found: {file}")
                subprocess.run(["scp", os.path.join(src_folder, file), f"{dest_user}@{dest_host}:{dest_path}"])
                subprocess.run(["rm", os.path.join(src_folder, file)])
                print(f"transfered: {file}")

        time.sleep(120)
