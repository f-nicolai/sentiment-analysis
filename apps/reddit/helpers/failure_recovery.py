from datetime import datetime, timedelta
from google.api_core.exceptions import NotFound
from json import loads

from sa_tools.gcp import download_file_as_string_from_gcs
from helpers.subreddit_information import all_subreddits

def retrieve_logs_and_setup_dates_for_historical(mode: str) -> (datetime, datetime, list[str]):
    try:
        logs = loads(download_file_as_string_from_gcs(
            storage_file_name=f'reddit/historical_extraction/log_last_current_date_{mode}.json',
            bucket_name='intraday-data-extraction',
            project='sentiment-analysis-379718'
        ))
    except NotFound:
        logs = {'start_date': None, 'current_date': None, 'initial_date':None, 'current_subreddit': None}

    if bool(logs.get('start_date')):
        start_date = datetime.fromtimestamp(logs['start_date'])
    else:
        if mode == 'full':
            start_date = datetime(2018, 1, 1)
        else:
            start_date = datetime.now() - timedelta(days=15)

    if bool(logs.get('current_date')):
        current_date = datetime.fromtimestamp(logs['current_date'])
    else:
        current_date = datetime.now()

    if bool(logs.get('initial_date')):
        initial_date = datetime.fromtimestamp(logs['initial_date'])
    else:
        initial_date = current_date

    subreddits = all_subreddits.copy()
    if logs.get('current_subreddit'):
        subreddits = subreddits[subreddits.index(logs['current_subreddit']):]

    return start_date, current_date, initial_date, subreddits
