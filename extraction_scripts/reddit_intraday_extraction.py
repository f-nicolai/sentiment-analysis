import time
import logging
from pathlib import Path

from data_extractor.reddit.reddit_extractor import RedditExtractor

logging.basicConfig(
    filename=Path(__file__).parent/f'logs/reddit_intraday_extraction.log',
    format='%(asctime)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.INFO
)

r_client = RedditExtractor()
while True:
    # for subreddit in ['wallstreetbets', 'investing', 'WallStreetbetsELITE']:
    for subreddit in ['wallstreetbets']:
        r_client.last_submissions_data(
            subreddit=subreddit,
            submissions_sorting = 'new',
            ignore_flairs=['Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost','Daily Discussion', 'Weekend Discussion'],
        )
    time.sleep(10*60)



    # TODO for daily & weekly threads :
    # r_client.last_submissions_data(
    #     subreddit=subreddit,
    #     submissions_sorting = 'new',
    #     ignore_flairs=['Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost'],
    #     focus_on_flairs = ['Daily Discussion', 'Weekend Discussion']
    #     submissions_creation_date_limit = datetime.date.today() - datetime.timedelta(days=1)
    # )