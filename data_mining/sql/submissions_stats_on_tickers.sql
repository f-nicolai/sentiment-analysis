with ticker_perim as (
    select id,
        created_utc,
        date(created_utc) as day,
        extract(hour from created_utc) as hour,
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
select subreddit,
       day,
       count(distinct id) as n_submissions,
       count(*) as n_mentionned_tickers,
       count(distinct ticker) as n_dist_tickers,
       APPROX_TOP_COUNT(ticker,2) as most_frequent_ticker,
--        APPROX_TOP_COUNT(ticker,2)[OFFSET(1)] as second_frequent_ticker,
       count(distinct ticker)/count(*) as ratio_distinct_total_tickers
from ticker_perim
left join subreddits
using(subreddit__id)
group by 1,2