import datetime
import logging
from pathlib import Path

from data_extractor.reddit.reddit_extractor import RedditExtractor

logging.basicConfig(
    filename=Path(__file__).parent/f'logs/reddit_historical_extraction.log',
    format='%(asctime)s - %(message)s',
    datefmt='yyyy-MM-dd HH:mm:ss,SSS',
    level=logging.INFO
)

r_client = RedditExtractor()
for subreddit in ['wallstreetbets', 'investing', 'WallStreetbetsELITE']:
    r_client.historical_submissions_and_comments(
        subreddit=subreddit,
        start_date=datetime.datetime(2018,12, 31),
        # end_date=datetime.datetime(2023, 3, 11,1,46,51),
        end_date=datetime.datetime(2023, 3, 28),
        ignore_flairs=['Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost'],
        batch_size=1000,
    )
    logging.info(f'Last extracted Submission date: {datetime.datetime.fromtimestamp(r_client.current)}')
