import requests
import base64
import datetime
import snscrape.modules.twitter as sntwitter
import os
import operator
import praw
from newsapi import NewsApiClient

print("Starting fetchingTwitterRedditNews.py")

# Reddit API credentials
reddit_client_id = 'fImp2bIF-caJyYO9xDAhsQ'
reddit_client_secret = 'fo_IqKNV-rhS90XgIFa8B4xwwV-tcA'
user_agent = 'MyRedditScrpt/1.0 by Lorenzo Calarota (cala95@gmail.com)'

# News API key
news_api_key = 'f523589a4c564228bd14cdade0bafac2'

# Set up the snscrape Twitter scraper
def fetch_tweets(query, count):
    scraper = sntwitter.TwitterSearchScraper(query)
    tweets = []
    for tweet in scraper.get_items():
        if len(tweets) >= count:
            break
        tweets.append(tweet)
    return tweets

# Set up PRAW for fetching Reddit posts
reddit = praw.Reddit(client_id=reddit_client_id, client_secret=reddit_client_secret, user_agent=user_agent)

def fetch_reddit_posts(subreddit, count):
    subreddit = reddit.subreddit(subreddit)
    posts = []
    for post in subreddit.hot(limit=count):
        title = post.title
        date = datetime.datetime.utcfromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S")
        posts.append((title, date))
    return posts

# Set up the News API for fetching news articles
newsapi = NewsApiClient(api_key=news_api_key)

def fetch_news_articles(query, count):
    articles = newsapi.get_everything(q=query, language='en', sort_by='relevancy', page_size=count)
    news = []
    for article in articles['articles']:
        title = article['title']
        date = datetime.datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
        news.append((title, date))
    return news

#error handling
def safe_fetch_tweets(query, count):
    try:
        return fetch_tweets(query, count)
    except Exception as e:
        print(f"Error fetching tweets: {e}")
        return []

def safe_fetch_reddit_posts(subreddit, count):
    try:
        return fetch_reddit_posts(subreddit, count)
    except Exception as e:
        print(f"Error fetching Reddit posts: {e}")
        return []

def safe_fetch_news_articles(query, count):
    try:
        return fetch_news_articles(query, count)
    except Exception as e:
        print(f"Error fetching news articles: {e}")
        return []

def sort_file_by_date(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()


    lines.sort(key=lambda x: x.split(" ", 1)[0])

    with open(file_path, 'w', encoding="utf-8") as f:
        f.writelines(lines)

def remove_duplicates_from_file(file_path):
    with open(file_path, 'r', encoding="utf-8") as f:
        lines = f.readlines()

    seen = set()
    unique_lines = []
    for line in lines:
        if line not in seen:
            seen.add(line)
            unique_lines.append(line)

    with open(file_path, 'w', encoding="utf-8") as f:
        f.writelines(unique_lines)


# Create SocialData directory if it doesn't exist
if not os.path.exists('SocialData'):
    os.makedirs('SocialData')

# twitterPath = "SocialData/tweets.txt"
redditPath = "SocialData/reddit_posts.txt"
newsPath = "SocialData/news_articles.txt"

#ordering if file already exists        
# if os.path.exists(twitterPath):
#     sort_file_by_date(twitterPath)
# else:
#     print('{twitterPath} does not exist')
if os.path.exists(redditPath):
    sort_file_by_date(redditPath)
else:
    print('{redditPath} does not exist')
if os.path.exists(newsPath):
    sort_file_by_date(newsPath)
else:
    print('{newsPath} does not exist')    
    
# Collect data
# bitcoin_tweets = safe_fetch_tweets("bitcoin", 100)
bitcoin_posts = safe_fetch_reddit_posts('bitcoin', 100)
bitcoin_news = safe_fetch_news_articles('bitcoin', 100)

# Prepare data for sorting
# tweets_data = [(tweet.date, tweet.rawContent.replace("\n", "")) for tweet in bitcoin_tweets]
reddit_data = [(datetime.datetime.strptime(post[1], "%Y-%m-%d %H:%M:%S"), post[0]) for post in bitcoin_posts]
news_data = [(datetime.datetime.strptime(article[1], "%Y-%m-%d %H:%M:%S"), article[0]) for article in bitcoin_news]

# Sort the data by time
# tweets_data.sort(key=operator.itemgetter(0))
reddit_data.sort(key=operator.itemgetter(0))
news_data.sort(key=operator.itemgetter(0))

# Write sorted data to files
# print(f"example: File for twitter written at {os.getcwd()}/{twitterPath}")
# with open(twitterPath, 'a', encoding="utf-8") as f:
#     for date, content in tweets_data:
#         f.write(f"{date} {content}\n")

print("current directory:", os.getcwd())        
print(f"example: File for reddit written at {os.getcwd()}/{redditPath}")
with open(redditPath, 'a', encoding="utf-8") as f:
    for date, title in reddit_data:
        f.write(f"{date} {title}\n")

with open(newsPath, 'a', encoding="utf-8") as f:
    for date, title in news_data:
        f.write(f"{date} {title}\n")

#removing eventual duplicates
# remove_duplicates_from_file(twitterPath)
remove_duplicates_from_file(redditPath)
remove_duplicates_from_file(newsPath)