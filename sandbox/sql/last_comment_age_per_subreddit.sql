select id,
    created_utc as sub_date,
    max_comment_dt,
    TIMESTAMP_DIFF(max_comment_dt,created_utc,minute) as last_com_age
from   `reddit.submissions`
left join
(
  select 	subreddit__submission__id as id,
      cast(max(created_utc) as timestamp) as max_comment_dt
  from `reddit.comments`
  where created_utc >= '2023-01-01'
  group by 1
)
using(id)
where flair is null or flair not in ('Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost','Daily Discussion', 'Weekend Discussion')
  and created_utc >= '2023-01-01'

