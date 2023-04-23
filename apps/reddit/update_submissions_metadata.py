from reddit_extractor.reddit import RedditExtractor
from helpers.subreddit_information import all_subreddits, submission_sorting, subreddit_flairs_to_ignore


if __name__ == '__main__':

    for subreddit in all_subreddits:
        r_client = RedditExtractor()

        for sort in submission_sorting:
            r_client.last_submissions_data(
                subreddit=subreddit,
                submissions_sorting=sort,
                ignore_flairs = subreddit_flairs_to_ignore[subreddit]
            )
