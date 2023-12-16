import pandas as pd
import os

# Create the 'DataCombined' directory if it doesn't exist
if not os.path.exists("DataCombined"):
    os.makedirs("DataCombined")

# Assuming you have the following data files:
price_data_file = "PriceData/price_report.csv"  # Columns: time, open, high, low, close, volumefrom, volumeto, SMA_10, SMA_30, RSI, MACD, MACD_signal, MACD_diff
sentiment_report_file = "SocialData/sentiment_report.txt" #3 lines: data 2023-04-16 13:00:00\n Twitter average sentiment score: 0.07431439432410639 \nReddit average sentiment score: -0.034375 \nNews average sentiment score: N/A 


# Load the price data and technical indicators into a DataFrame
price_data = pd.read_csv(price_data_file, parse_dates=["time"], index_col="time")

# Load the sentiment data from the file
with open(sentiment_report_file, "r", encoding="utf-8") as file:
    data_lines = file.readlines()

# Extract the date and sentiment scores from each line
sentiment_data = []
idx = 0
while idx < len(data_lines):
    date = pd.to_datetime(data_lines[idx].strip())
    idx += 1

    ######################## BLOCKED######################## BLOCKED######################## BLOCKED
    # twitter_avg_sentiment = data_lines[idx].split(": ")[1].strip()
    # twitter_avg_sentiment = float(twitter_avg_sentiment) if twitter_avg_sentiment != "N/A" else None
    # idx += 1
    ######################## BLOCKED######################## BLOCKED######################## BLOCKED

    reddit_avg_sentiment = data_lines[idx].split(": ")[1].strip()
    reddit_avg_sentiment = float(reddit_avg_sentiment) if reddit_avg_sentiment != "N/A" else None
    idx += 1

    news_avg_sentiment = data_lines[idx].split(": ")[1].strip()
    news_avg_sentiment = float(news_avg_sentiment) if news_avg_sentiment != "N/A" else None
    idx += 2

    # sentiment_data.append((date, twitter_avg_sentiment, reddit_avg_sentiment, news_avg_sentiment)) ######################## BLOCKED
    sentiment_data.append((date, reddit_avg_sentiment, news_avg_sentiment))


# Create a DataFrame from the sentiment data
sentiment_df = pd.DataFrame(sentiment_data, columns=["time", "Reddit_Sentiment", "News_Sentiment"])
sentiment_df.set_index("time", inplace=True)

# Merge the price data with the sentiment data
combined_data = price_data.join(sentiment_df.tz_localize('UTC'))

# Save the combined data to a new CSV file
combined_data.to_csv("DataCombined/combined_data.csv")

print("Combined data saved to 'combined_data.csv'")
