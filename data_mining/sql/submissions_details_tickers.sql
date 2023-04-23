with subs_perim as (
    select id as subreddit__submission__id,
         created_utc,
         date(created_utc) as day,
         extract(hour from created_utc) as hour,
         subreddit__id,
         ticker,
         case when ticker = '{{ticker}}' then 1 else 0 end as focus_ticker,
--          title,
--          selftext,
        flair,
        score,
        upvote_ratio,
        num_comments,
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
from subs_perim
left join subreddits
using(subreddit__id)
