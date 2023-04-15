from pandas import  to_datetime
from data_extractor.reddit.reddit_extractor import RedditExtractor

r_client = RedditExtractor()

subreddit = r_client.reddit.subreddit("wallstreetbets")
for comment in subreddit.stream.comments(skip_existing=True):
    print(f'{to_datetime(comment.created_utc, unit="s")}: \t-link_id: {comment.link_id}\t-body: {comment.body}\t- ')
