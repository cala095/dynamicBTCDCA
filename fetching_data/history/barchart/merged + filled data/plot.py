import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Read the CSV file
df = pd.read_csv('tnx_filled_data.csv')

# Convert Time column to datetime
df['Time'] = pd.to_datetime(df['Time'])

# Create the plot
plt.figure(figsize=(12, 6))
plt.plot(df['Time'], df['Last'], linewidth=2)

# Customize the plot
plt.title('Estimated Yield Over Time', fontsize=14, pad=15)
plt.xlabel('Time', fontsize=12)
plt.ylabel('Estimated Yield (%)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Show the plot
plt.show()
