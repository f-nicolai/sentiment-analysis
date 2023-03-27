from json import loads
import datetime
import logging

from reddit_extractor.reddit import RedditExtractor
import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()

def reddit_extraction(request):
    request_args = request.get_data()
    request_args = loads(request_args.decode())

    r_client = RedditExtractor()

    ignore_flairs = request_args.get('ignore_flairs')
    if not bool(ignore_flairs):
        ignore_flairs = []

    try :
        if request_args['type'] == 'intraday':


            r_client.last_submissions_data(
                subreddit=request_args['subreddit'],
                submissions_sorting=request_args['submissions_sorting'],
                ignore_flairs = ignore_flairs
                # ignore_flairs=['Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost', 'Daily Discussion',
                #                'Weekend Discussion'],
            )
        else:
            r_client.historical_submissions_and_comments(
                subreddit=request_args['subreddit'],
                start_date=datetime.datetime.strptime(request_args['start_date'], '%Y-%m-%d'),
                end_date=datetime.datetime.strptime(request_args['end_date'], '%Y-%m-%d %H:%M:S'),
                ignore_flairs=['Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost'],
                batch_size=1000,
            )
        return 'DONE',200

    except:
        logging.error("Exception occurred", exc_info=True)
        return 'ERROR', 500