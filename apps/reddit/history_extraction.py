import datetime
import logging
import sys
from pathlib import Path
import argparse

from reddit_extractor.reddit import RedditExtractor
from sa_tools.gcp import remove_previous_reddit_data_updates, upload_dict_to_gcs
from helpers.failure_recovery import retrieve_logs_and_setup_dates_for_historical
from helpers.subreddit_information import subreddit_flairs_to_ignore


parser = argparse.ArgumentParser()
parser.add_argument("-m", "--mode", help="mode for historical data retrieval: full or ltw (last two weeks)")

logging.basicConfig(
    format='%(asctime)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    level=logging.INFO
)

if __name__ == '__main__':
    args = parser.parse_args()

    # Retrieving past run logs
    start_date, current_date, initial_date, subreddits = retrieve_logs_and_setup_dates_for_historical(mode=args.mode)

    r_client = RedditExtractor()

    for idx,subreddit in enumerate(subreddits):
        r_client.historical_submissions_and_comments(
            subreddit=subreddit,
            start_date=start_date,
            current_date=current_date if idx == 0 else initial_date,
            initial_date=initial_date,
            ignore_flairs=subreddit_flairs_to_ignore[subreddit],
            batch_size=1000,
            mode=args.mode
        )

        for tbl in ['authors', 'submissions', 'comments']:
            remove_previous_reddit_data_updates(table=tbl, project='sentiment-analysis-379718')

    # Clearing logs
    upload_dict_to_gcs(
        dictionary={
            'start_date': None,
            'current_date': None,
            'initial_date': None,
            'current_subreddit': None
        },
        bucket_name='intraday-data-extraction',
        file_name=f'reddit/historical_extraction/log_last_current_date_{args.mode}.json',
        project='sentiment-analysis-379718'
    )

    print(f'Historical extraction {args.mode}: DONE')