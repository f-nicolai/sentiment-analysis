import datetime
import logging
import sys
from pathlib import Path
sys.path.append(f'{Path(__file__).parent.parent}')

from data_extractor.reddit.reddit_extractor.reddit import RedditExtractor

logging.basicConfig(
    filename=Path(__file__).parent/f'logs/reddit_historical_extraction.log',
    format='%(asctime)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.INFO
)
r_client = RedditExtractor()
for subreddit in ['wallstreetbets', 'investing', 'WallStreetbetsELITE']:
    r_client.historical_submissions_and_comments(
        subreddit=subreddit,
        start_date=datetime.datetime(2021,11, 1),
        end_date=datetime.datetime(2022, 10, 1),
        ignore_flairs=['Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost'],
        batch_size=1000,
    )
    logging.info(f'Last extracted Submission date: {datetime.datetime.fromtimestamp(r_client.current)}')
