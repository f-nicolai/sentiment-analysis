with ticker_perim as (
    select id,
           created_utc,
         subreddit__id,
         ticker,
         title,
         selftext,
        score,
        upvote_ratio,
        num_comments,
        flair,
        number_detected_tickers
  from `reddit.submissions`
     , UNNEST(SPLIT(detected_tickers, '|')) AS ticker
  WHERE detected_tickers != ''
),
subreddits as (
    select id as subreddit__id,
            subreddit
    from  `reddit.subreddits`
)
select *
from ticker_perim
left join subreddits
using(subreddit__id)
