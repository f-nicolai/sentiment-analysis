import praw
import time
import logging
import requests
import uuid

from os import environ
from joblib import Parallel, delayed
from pandas import DataFrame, to_datetime, concat, json_normalize
from datetime import datetime, timedelta

from utils.miscellaneous import timeit
from utils.gcp import create_bq_table_from_dataframe, upload_dataframe_to_gcs, upload_dict_to_gcs
from utils.ticker_detection import detect_tickers

if environ.get('GCF_ENV'):
    REDDIT_PERSONAL_USE_SCRIPT = environ.get('REDDIT_PERSONAL_USE_SCRIPT')
    REDDIT_SECRET = environ.get('REDDIT_SECRET')
    REDDIT_PERSONAL_PASSWORD = environ.get('REDDIT_PERSONAL_PASSWORD')
    REDDIT_USERNAME = environ.get('REDDIT_USERNAME')
else:
    from api_secrets import *

comments_features = ['body', 'link_id', 'id', 'fullname', 'parent_id', 'body', 'score', 'ups', 'downs', 'created_utc',
                     'author_fullname', 'author']
submissions_features = [
    'id', 'fullname', 'link_flair_text', 'author_fullname', 'title', 'selftext', 'created_utc', 'distinguished',
    'is_self',
    'permalink', 'score', 'upvote_ratio', 'ups', 'downs', 'stickied', 'num_comments', 'author', 'flair'
]


class RedditExtractor():
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=REDDIT_PERSONAL_USE_SCRIPT,
            client_secret=REDDIT_SECRET,
            password=REDDIT_PERSONAL_PASSWORD,
            username=REDDIT_USERNAME,
            user_agent='python:sentiment-analysis:0.0.1 (by u/_L2G_)',
        )

        self.subreddit_id: str = None
        self.until: int = None
        self.since: int = None
        self.current: int = None
        self.current_recovery: int = None
        self.extract_day: str = None
        self.extract_timestamp: str = None
        self.submissions_sorting: str = None
        self.run_id = uuid.uuid4().hex
        self.run_dt = datetime.now()
        self.mode = None

    def stream_submissions_to_gcs(self, subreddit: str):
        logging.info(f'Starting streaming submissions for : {subreddit.upper()}')

        subreddit_r = self.reddit.subreddit(subreddit)
        self.subreddit_id = subreddit_r.id

        for sub in subreddit_r.stream.submissions(skip_existing=True):
            new_submission,_ = self._submission_data_extraction(submission=sub,with_comments=False)

            if bool(new_submission):
                new_submission, author = [x.to_dict(orient='records')[0] for x in self._format_submissions([new_submission], is_daily=True)]

                file_date = new_submission['created_utc']
                new_submission['created_utc'] = new_submission['created_utc'].isoformat()

                logging.info(f'Upload submission id : {new_submission["id"]} - created_utc: {new_submission["created_utc"]}')

                upload_dict_to_gcs(
                    dictionary=new_submission,
                    bucket_name='intraday-data-extraction',
                    file_name=f'reddit/streaming/submissions/{file_date.strftime("%Y/%m/%d/%H:%M:%S")}_{new_submission["id"]}.json',
                    project='sentiment-analysis-379718'
                )
                upload_dict_to_gcs(
                    dictionary=author,
                    bucket_name='intraday-data-extraction',
                    file_name=f'reddit/streaming/authors/{file_date.strftime("%Y/%m/%d/%H:%M:%S")}_{author["fullname"]}.json',
                    project='sentiment-analysis-379718'
                )

    def stream_comments_to_gcs(self, subreddit: str):
        logging.info(f'Starting streaming comments for : {subreddit.upper()}')

        subreddit_r = self.reddit.subreddit(subreddit)
        self.subreddit_id = subreddit_r.id

        for com in subreddit_r.stream.comments(skip_existing=True):
            new_comment = self._comment_data_extraction(comment=com)

            if bool(new_comment):
                new_comment, author = [x.to_dict(orient='records')[0] for x in self._format_comments([new_comment], is_daily=True)]

                file_date = new_comment['created_utc']
                new_comment['created_utc'] = new_comment['created_utc'].isoformat()

                logging.info(f'Upload comment id : {new_comment["id"]} - created_utc: {new_comment["created_utc"]}')

                upload_dict_to_gcs(
                    dictionary=new_comment,
                    bucket_name='intraday-data-extraction',
                    file_name=f'reddit/streaming/comments/{file_date.strftime("%Y/%m/%d/%H:%M:%S")}_{new_comment["id"]}.json',
                    project='sentiment-analysis-379718'
                )
                upload_dict_to_gcs(
                    dictionary=author,
                    bucket_name='intraday-data-extraction',
                    file_name=f'reddit/streaming/authors/{file_date.strftime("%Y/%m/%d/%H:%M:%S")}_{author["fullname"]}.json',
                    project='sentiment-analysis-379718'
                )

    def historical_submissions_and_comments(self, subreddit: str, start_date: datetime, end_date: datetime, mode: str,
                                            ignore_flairs: list = None, batch_size: int = 1000):
        logging.info(f'Retrieving Submissions for {subreddit.upper()} from {start_date} to {end_date}')

        self.until = int(end_date.timestamp())
        self.since = int(start_date.timestamp())
        self.current = self.until
        self.current_recovery = self.current
        self.mode = mode

        self.subreddit_id = self.reddit.subreddit(subreddit).id

        self._iterate_over_submissions_until_limit(
            subreddit=subreddit,
            ignore_flairs=ignore_flairs,
            batch_size=batch_size
        )
        logging.info(f'Retrieving Submissions DONE')

    @timeit
    def last_submissions_data(self, subreddit: str, submissions_sorting: str, ignore_flairs: list = None,
                              focus_on_flairs: list = None):
        logging.info(f'Update {subreddit.upper()} submissions metadata - {submissions_sorting.upper()} ...')

        self.submissions_sorting = submissions_sorting
        self.extract_day = datetime.now().strftime('%Y/%m/%d')
        self.timemstamp = datetime.now().strftime('%H-%M-%S')

        _, submissions_data, _ = self._subreddit_data(
            subreddit=self.reddit.subreddit(subreddit),
            submissions_sorting=submissions_sorting,
            ignore_flairs=ignore_flairs,
            focus_on_flairs=focus_on_flairs,
            with_comments=False
        )

        if len(submissions_data) >0 :
            submissions_data, _ = self._format_submissions(submissions=submissions_data, is_daily=True)
            submissions_data['upload_datetime'] = to_datetime(self.run_dt)

            self._save_intraday_data(subreddit=subreddit,submissions=submissions_data)

        logging.info(f'Intraday extraction for {subreddit.upper()} - {submissions_sorting.upper()} DONE')

    @timeit
    def _subreddit_data(self, subreddit: praw.models.reddit.subreddit.Subreddit, submissions_sorting: str,
                        ignore_flairs: list = None, with_comments: bool = True, focus_on_flairs: list = None,
                        submissions_creation_date_limit: datetime = None) -> (dict, list, list):

        self.subreddit_id = subreddit.id
        subreddit_data = {}
        subreddit_data['id'] = subreddit.id
        subreddit_data['subreddit'] = subreddit.title
        subreddit_data['fullname'] = subreddit.fullname

        if submissions_sorting == 'new':
            new_submissions = subreddit.new(limit=100)
        elif submissions_sorting == 'hot':
            new_submissions = subreddit.hot(limit=100)
        else:
            _, time_filter = submissions_sorting.split('-')
            new_submissions = subreddit.top(time_filter=time_filter)

        submissions_and_comments_data = [
            self._submission_data_extraction(submission=submission, with_comments=with_comments,
                                             comments_sorting=submissions_sorting)
            for submission in new_submissions
            if submission.link_flair_text not in ignore_flairs
        ]

        submissions_data, comments_data = [], []
        for tu in submissions_and_comments_data:
            submissions_data.append(tu[0])
            comments_data.append(tu[1])

        comments_data = [item for sublist in comments_data for item in sublist]

        return subreddit_data, submissions_data, comments_data

    def _submission_data_extraction(self, submission: praw.models.reddit.submission.Submission,
                                    comments_sorting: str = None, with_comments: bool = True) -> (dict, list):
        submission_data = {}
        submission_data['id'] = submission.id
        submission_data['fullname'] = submission.fullname
        submission_data['flair'] = submission.link_flair_text
        submission_data['author'] = self.author_data_extractor(author=submission.author, comment=submission)
        submission_data['title'] = submission.title
        submission_data['selftext'] = submission.selftext
        submission_data['created_utc'] = to_datetime(submission.created_utc, unit='s')
        submission_data['distinguished'] = submission.distinguished
        submission_data['is_self'] = submission.is_self
        submission_data['permalink'] = submission.permalink
        submission_data['score'] = submission.score
        submission_data['upvote_ratio'] = submission.upvote_ratio
        submission_data['ups'] = submission.ups
        submission_data['downs'] = submission.downs
        submission_data['stickied'] = submission.stickied
        submission_data['num_comments'] = submission.num_comments

        if with_comments:
            submission.comment_sort = comments_sorting
            submission.comments.replace_more(limit=10)

            comments_data = [
                self._comment_data_extraction(comment=comment)
                for comment in submission.comments.list()
                if comment.author and ((comment.author.name != 'VisualMod') or (comment.author.fullname != 't2_6hf2z55l'))]
        else:
            comments_data = []

        return submission_data, comments_data

    def _comment_data_extraction(self, comment: praw.models.reddit.comment.Comment) -> dict:
        if comment.author and ((comment.author.name != 'VisualMod') or (comment.author.fullname != 't2_6hf2z55l')):
            comment_data = {}
            comment_data['body'] = comment.body
            comment_data['id'] = comment.id
            comment_data['link_id'] = comment.link_id
            comment_data['fullname'] = comment.fullname
            comment_data['parent_id'] = comment.parent_id
            comment_data['score'] = comment.score
            comment_data['ups'] = comment.ups
            comment_data['downs'] = comment.downs
            comment_data['created_utc'] = to_datetime(comment.created_utc, unit='s')
            comment_data['author'] = self.author_data_extractor(author=comment.author, comment=comment)

            return comment_data

    @staticmethod
    def author_data_extractor(
            author: praw.models.reddit.redditor.Redditor,
            submission: praw.models.reddit.submission.Submission = None,
            comment: praw.models.reddit.comment.Comment = None
    ) -> dict:
        author_data = {}

        required_attributes = {'name', 'created_utc', 'is_suspended', 'is_blocked', 'created_utc', 'id', 'fullname',
                               'premium', 'is_mod', 'is_gold'}

        try:
            for attribute in set(vars(author).keys()).intersection(required_attributes):
                author_data[attribute] = author.__getattribute__(attribute)

            for attribute in required_attributes.difference(set(vars(author).keys())):
                try:
                    author_data[attribute] = comment.__getattribute__(f'author_{attribute}') if bool(
                        comment) else submission.__getattribute__(f'author_{attribute}')
                except:
                    author_data[attribute] = None
        except:
            author_data.update({x: None for x in required_attributes if x not in author_data.keys()})

        return author_data

    def _save_intraday_data(self, subreddit: str, submissions: DataFrame, authors: DataFrame = None, comments: DataFrame = None):
        logging.info(f'Uploading submissions : {submissions.shape[0]}')
        upload_dataframe_to_gcs(
            dataframe=submissions,
            bucket_name='intraday-data-extraction',
            file_name=f'reddit/mini_batch/submissions/{self.extract_day}/{subreddit}_{self.submissions_sorting}_{self.timemstamp}.csv',
            index=False,
            encoding='utf-8-sig'
        )

        if isinstance(authors, DataFrame):
            logging.info(f'Uploading authors : {authors.shape[0]}')
            upload_dataframe_to_gcs(
                dataframe=authors,
                bucket_name='intraday-data-extraction',
                file_name=f'reddit/mini_batch/authors/{self.extract_day}/{self.submissions_sorting}_{self.timemstamp}.csv',
                index=False,
                encoding='utf-8-sig'
            )

        if isinstance(comments, DataFrame):
            logging.info(f'Uploading comments : {comments.shape[0]}')
            upload_dataframe_to_gcs(
                dataframe=comments,
                bucket_name='intraday-data-extraction',
                file_name=f'reddit/mini_batch/comments/{self.extract_day}/{self.submissions_sorting}_{self.timemstamp}.csv',
                index=False,
                encoding='utf-8-sig'
            )

    def _save_batch_data(self, submissions: DataFrame, authors: DataFrame, comments: DataFrame):
        logging.info(f'\t- Added submissions: {submissions.shape[0]}')
        create_bq_table_from_dataframe(
            dataframe=submissions,
            table_name='sentiment-analysis-379718.reddit.submissions',
            truncate=False,
            project='sentiment-analysis-379718'
        )
        logging.info(f'\t- Added comments: {comments.shape[0]}')
        create_bq_table_from_dataframe(
            dataframe=comments,
            table_name='sentiment-analysis-379718.reddit.comments',
            truncate=False,
            project='sentiment-analysis-379718'
        )
        logging.info(f'\t- Added authors: {authors.shape[0]}')
        create_bq_table_from_dataframe(
            dataframe=authors,
            table_name='sentiment-analysis-379718.reddit.authors',
            truncate=False,
            project='sentiment-analysis-379718'
        )

    def save_current_date_to_gcs(self, subreddit: str):
        upload_dict_to_gcs(
            dictionary={
                'start_date': int(self.since),
                'current_date': int(self.current),
                'current_subreddit': subreddit
            },
            bucket_name='intraday-data-extraction',
            file_name=f'reddit/historical_extraction/log_last_current_date_{self.mode}.json',
            project='sentiment-analysis-379718'
        )

    def _iterate_over_submissions_until_limit(
            self,
            subreddit: str,
            batch_size: int,
            ignore_flairs: list = None):
        while self.current >= self.since:

            self.save_current_date_to_gcs(subreddit=subreddit)

            try:
                start = time.time()
                submissions, authors_s = self._format_submissions(
                    submissions=self._search_and_format_submissions(
                        subreddit=subreddit,
                        ignore_flairs=ignore_flairs,
                        batch_size=batch_size
                    )
                )
                logging.info(f'Retrieved {submissions.shape[0]} submissions in: {str(timedelta(seconds=time.time() - start))}')

                comments, authors_c = self._format_comments(
                    comments=self._search_comments_from_submissions_ids(
                        submissions_ids=submissions['id'].to_list(),
                    )
                )
                logging.info(f'Retrieved {comments.shape[0]} comments in: {str(timedelta(seconds=time.time() - start))}')

                submissions['upload_datetime'] = self.run_dt
                authors_s['upload_datetime'] = self.run_dt
                comments['upload_datetime'] = self.run_dt
                authors_c['upload_datetime'] = self.run_dt

                self._save_batch_data(submissions=submissions, comments=comments,
                                      authors=concat([authors_s, authors_c]))

                logging.info(
                    f'Current timestamp = {datetime.fromtimestamp(self.current)} - Elapsed time = {str(timedelta(seconds=time.time() - start))}')

            except Exception as e:
                self.current = self.current_recovery
                logging.error("Exception occurred", exc_info=True)
                logging.error(
                    f'FOR RECOVERY : Current Epoch (current timestamp) = {self.current} ({datetime.fromtimestamp(self.current)})')

    def _search_and_format_submissions(self, subreddit: str, batch_size: int, ignore_flairs: list = None) -> list:
        submissions = requests.get(
            url=f'https://api.pushshift.io/reddit/submission/search?subreddit={subreddit}&size={batch_size}&before={self.current}')
        return [x for x in submissions.json()['data'] if
                not (bool(ignore_flairs)) or x['link_flair_text'] not in ignore_flairs]

    def _format_submissions(self, submissions: list, is_daily: bool = False) -> (DataFrame, DataFrame):
        try:
            data = DataFrame([{k: v for k, v in x.items() if k in submissions_features} for x in submissions])
        except:
            print('yolo')

        data = data.loc[~data['selftext'].isin(['[deleted]', '[removed]'])].copy()

        self.current_recovery = self.current
        self.current = data['created_utc'].min()

        data['full_id'] = self.subreddit_id + data['id']
        data = data.rename({'link_flair_text': 'flair'}, axis=1)

        data = self._format_common_data(data=data, columns_for_ticker_detection=['title', 'selftext'])

        if is_daily:
            authors = json_normalize(data['author'])
            data = data.drop('author', axis=1)
        else:
            data, authors = self._format_author_data(data=data)

        return data, authors

    def _format_comments(self, comments: list, is_daily: bool = False) -> (DataFrame, DataFrame):
        data = DataFrame([{k: v for k, v in x.items() if k in comments_features} for x in comments])

        data['subreddit.submission.id'] = data['link_id'].str.split('_', expand=True)[1]
        data['full_id'] = self.subreddit_id + data['subreddit.submission.id'] + data['id']

        data = data.drop('link_id', axis=1)

        data = self._format_common_data(data=data, columns_for_ticker_detection=['body'])

        if is_daily:
            authors = json_normalize(data['author'])
            data = data.drop('author', axis=1)
        else:
            data, authors = self._format_author_data(data=data)

        return data, authors

    def _format_common_data(self, data: DataFrame, columns_for_ticker_detection: list) -> DataFrame:
        data['created_utc'] = to_datetime(data['created_utc'], unit='s')
        data['subreddit.id'] = self.subreddit_id

        data['tmp'] = data[columns_for_ticker_detection[0]]
        for col in columns_for_ticker_detection[1:]:
            data['tmp'] += ' ' + data[col]
        data['detected_tickers'] = detect_tickers(data['tmp'])

        data['number_detected_tickers'] = 0
        data.loc[~data['detected_tickers'].isnull(), 'number_detected_tickers'] = data.loc[~data['detected_tickers'].isnull()]['detected_tickers'].str.split('|').apply(len)

        data = data.drop('tmp', axis=1)

        return data

    def _format_author_data(self, data: DataFrame) -> (DataFrame, DataFrame):
        if 'author_fullname' not in data.columns:
            data['author_fullname'] = None

        data = data.rename({'author_fullname': 'author.fullname', 'author': 'name'}, axis=1)
        authors = data[['author.fullname', 'name']].drop_duplicates().rename({'author.fullname': 'fullname'}, axis=1)
        data = data.drop('name', axis=1)

        return data, authors

    @timeit
    def _search_comments_from_submissions_ids(self, submissions_ids: list) -> list:
        return [item for sublist in Parallel(n_jobs=10, prefer="threads")(
            delayed(self._search_comments_from_submissions_id)(sub_id) for sub_id in submissions_ids) for item in
                sublist]

    def _search_comments_from_submissions_id(self, submission_id: str, until: int = None) -> list:
        url = f'https://api.pushshift.io/reddit/comment/search?link_id={int(submission_id, 36)}&size=1000&sort_type=desc&sort=created_utc'

        if bool(until): url += f'&until={until}'

        try:
            comments = self._request_pushshift_and_retry(url=url).json()['data']
        except Exception as e:
            return self._search_comments_from_submissions_id(submission_id=submission_id, until=until)

        if len(comments) > 990:
            min_date = min([x['created_utc'] for x in comments])

            comments += self._search_comments_from_submissions_id(submission_id=submission_id, until=min_date)

        return comments

    def _request_pushshift_and_retry(self, url) -> requests.Response:
        response = requests.get(url=url)

        while response.status_code in (429, 524):
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - {response.status_code} - {datetime.fromtimestamp(self.current)} - {url}')
            time.sleep(2)
            response = requests.get(url=url)

        return response
