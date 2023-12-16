import datetime
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from textblob import TextBlob


def check_and_download_resources(resources):
    for resource in resources:
        try:
            nltk.data.find(resource)
        except LookupError:
            print(f"{resource} not found, downloading now...")
            nltk.download(resource.split('/')[-1])
            print(f"{resource} downloaded successfully")

# Call the function to check and download resources if necessary
check_and_download_resources(["tokenizers/punkt", "corpora/stopwords"])


outputFileTwitter = "SocialData/twitter_processed_data.txt"
outputFileReddit = "SocialData/reddit_processed_data.txt"
outputFileNews = "SocialData/news_processed_data.txt"

outputFileReport = "SocialData/sentiment_report.txt"

twitterPath = "SocialData/tweets.txt"
redditPath = "SocialData/reddit_posts.txt"
newsPath = "SocialData/news_articles.txt"

def preprocess_data(data):
    # Tokenization
    tokens = word_tokenize(data)

    # Lowercasing
    tokens = [token.lower() for token in tokens]

    # Remove URLs
    tokens = [re.sub(r'http\S+', '', token) for token in tokens]

    # Remove special characters and numbers
    tokens = [re.sub(r'\W+|\d+', '', token) for token in tokens]

    # Remove empty tokens
    tokens = [token for token in tokens if token]

    # Remove stop words
    stop_words = set(stopwords.words("english"))
    tokens = [token for token in tokens if token not in stop_words]

    # Stemming
    stemmer = PorterStemmer()
    tokens = [stemmer.stem(token) for token in tokens]

    return tokens

def calculateSentiment(data_lines):
    preprocessed_data = []
    for line in data_lines:
        if not line.strip():
            continue

        try:
            # Separate timestamp and text
            timestamp, text = line.split(" ", 1)
            # Extract hour and second from text
            hour_second, text = text.split(" ", 1)
            # Combine date and hour_second into a new timestamp string
            new_timestamp = f"{timestamp} {hour_second}"
            # Update timestamp variable with the new timestamp string
            timestamp = new_timestamp
        except ValueError:
            print(f"Skipping line: {line}")
            continue

        # Preprocess text
        preprocessed_text = preprocess_data(text)

        # Sentiment analysis
        sentiment = TextBlob(text).sentiment.polarity

        # Add preprocessed data to the list
        preprocessed_data.append((timestamp, preprocessed_text, sentiment))
    
    return preprocessed_data

# Read data from files
 ##################################################################BLOCKED
# with open(twitterPath, "r", encoding="utf-8") as file:
#     twitter_data = file.read()
#     #removes duplicates
#     distinct_twitter_data = twitter_data.split("\n")
#     distinct_twitter_data = list(set(distinct_twitter_data))
#     #calculate sentiment
#     twitter_processed_data = calculateSentiment(distinct_twitter_data)
#     with open(outputFileTwitter, 'a', encoding="utf-8") as f:
#         for processed in twitter_processed_data:
#             f.write(f"{processed}\n")
#             # print(processed)
##################################################################BLOCKED

with open(redditPath, "r", encoding="utf-8") as file:
    reddit_data = file.read()
    distinct_reddit_data = reddit_data.split("\n")
    distinct_reddit_data = list(set(distinct_reddit_data))
    reddit_processed_data = calculateSentiment(distinct_reddit_data)
    with open(outputFileReddit, 'a', encoding="utf-8") as f:
        for processed in reddit_processed_data:
            f.write(f"{processed}\n")
            

with open(newsPath, "r", encoding="utf-8") as file:
    news_data = file.read()
    distinct_news_data = news_data.split("\n")
    distinct_news_data = list(set(distinct_news_data))
    news_processed_data = calculateSentiment(distinct_news_data)
    with open(outputFileNews, 'a', encoding="utf-8") as f:
        for processed in news_processed_data:
            f.write(f"{processed}\n")

print("reddit news data processed")


def extract_hourly_data(data_lines):
    hourly_data = {}

    for line in data_lines:
        try:
            record = eval(line)
            date_time = datetime.datetime.fromisoformat(record[0].replace('+00:00', ''))
            hour = date_time.replace(minute=0, second=0)
            sentiment_score = record[2]

            if hour not in hourly_data:
                hourly_data[hour] = {"sum": 0, "count": 0}

            hourly_data[hour]["sum"] += sentiment_score
            hourly_data[hour]["count"] += 1
        except ValueError as e:
            print(f"Skipping line: {line.strip()} due to error: {e}")

    return hourly_data

def calculate_hourly_average(hourly_data):
    hourly_average = {}
    for hour, data in hourly_data.items():
        hourly_average[hour] = data["sum"] / data["count"]
    return hourly_average

hourly_data_twitter = {}
hourly_data_reddit = {}
hourly_data_news = {}

##################################################################BLOCKED
# with open(outputFileTwitter, "r", encoding="utf-8") as file:
#     data_lines = file.readlines()
#     hourly_data_twitter = extract_hourly_data(data_lines)
#     tw_hourly_average = calculate_hourly_average(hourly_data_twitter)
##################################################################BLOCKED

with open(outputFileReddit, "r", encoding="utf-8") as file:
    data_lines = file.readlines()
    hourly_data_reddit = extract_hourly_data(data_lines)
    rd_hourly_average = calculate_hourly_average(hourly_data_reddit)

with open(outputFileNews, "r", encoding="utf-8") as file:
    data_lines = file.readlines()
    hourly_data_news = extract_hourly_data(data_lines)
    nws_hourly_average = calculate_hourly_average(hourly_data_news)

with open(outputFileReport, "w", encoding="utf-8") as file:
    # for hour in sorted(set(tw_hourly_average) | set(rd_hourly_average) | set(nws_hourly_average)): ##################################################################BLOCKED
    for hour in sorted(set(rd_hourly_average) | set(nws_hourly_average)):
        file.write(f"{hour}\n")
        # file.write(f"Twitter average sentiment score: {tw_hourly_average.get(hour, 'N/A')}\n") ##################################################################BLOCKED
        file.write(f"Reddit average sentiment score: {rd_hourly_average.get(hour, 'N/A')}\n")
        file.write(f"News average sentiment score: {nws_hourly_average.get(hour, 'N/A')}\n\n")

print("Hourly report written")

# # calculate the average for everyfile
# twAverage = 0
# rdAverage = 0
# nwsAverage = 0
# # Read the data from the file
# with open(outputFileTwitter, "r") as file:
#     data_lines = file.readlines()
#     # Extract the sentiment scores from each line
#     sentiment_scores = [eval(line)[2] for line in data_lines]
#     # Calculate the average sentiment score
#     twAverage = sum(sentiment_scores) / len(sentiment_scores)
# with open(outputFileReddit, "r") as file:
#     data_lines = file.readlines()
#     # Extract the sentiment scores from each line
#     sentiment_scores = [eval(line)[2] for line in data_lines]
#     # Calculate the average sentiment score
#     rdAverage = sum(sentiment_scores) / len(sentiment_scores)
# with open(outputFileNews, "r") as file:
#     data_lines = file.readlines()
#     # Extract the sentiment scores from each line
#     sentiment_scores = [eval(line)[2] for line in data_lines]
#     # Calculate the average sentiment score
#     nwsAverage = sum(sentiment_scores) / len(sentiment_scores)

# # Open the report file in append mode
# with open(outputFileReport, "a") as file:
#     # Write the current date to the report file
#     current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     file.write(f"{current_date}\n")
#     # Write the Twitter average to the report file
#     file.write(f"Twitter average sentiment score: {twAverage}\n")
#     # Write the Reddit average to the report file
#     file.write(f"Reddit average sentiment score: {rdAverage}\n")
#     # Write the News average to the report file
#     file.write(f"News average sentiment score: {nwsAverage}\n\n")

# print("report written")