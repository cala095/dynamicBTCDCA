#TODO also implement missing minute checker (at the monet just check dimension)
import os

def get_folder_size(folder_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

folder_path = 'resampled_data'  # Replace with the actual folder path

# Read the last recorded size from a file
try:
    with open('last_size.txt', 'r') as file:
        last_size = int(file.read().strip())
except FileNotFoundError:
    last_size = 2322366118  # Default value if file not found

# Get the current folder size
current_size = get_folder_size(folder_path)

# Compare the current size with the last recorded size
if current_size >= last_size:
    print(f"The folder size is: {current_size} bytes")
    print("STATUS OK")
else:
    print(f"The folder size is: {current_size} bytes")
    print("DIMENSION LESS THAN EXPECTED")

# Write the current size to the file
with open('last_size.txt', 'w') as file:
    file.write(str(current_size))