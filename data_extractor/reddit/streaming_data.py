import sys
from pathlib import Path

sys.path.append(f'{Path(__file__).parent.parent.parent}')

import argparse
import logging
from reddit_extractor.reddit import RedditExtractor
parser = argparse.ArgumentParser()
parser.add_argument("-t", "--type", help="type: submissions or comments")
parser.add_argument("-s", "--subreddit", help="subreddit to listen to")

r_client = RedditExtractor()

if __name__ == '__main__':
    args = parser.parse_args()

    logging.basicConfig(
        filename=Path(__file__).parent / f'logs/streaming_{args.type}.log',
        format='%(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.INFO
    )

    if args.type == 'submissions':
        try:
            r_client.stream_submissions_to_gcs(subreddit=args.subreddit)
        except:
            r_client.stream_submissions_to_gcs(subreddit=args.subreddit)

    elif args.type =='comments':
        try:
            r_client.stream_comments_to_gcs(subreddit=args.subreddit)
        except:
            r_client.stream_comments_to_gcs(subreddit=args.subreddit)