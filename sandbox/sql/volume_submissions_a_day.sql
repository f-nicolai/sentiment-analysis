select cast(created_utc as date) date_sub,
    count(distinct id),
from   `reddit.submissions`
where created_utc >= '2023-01-01'
    and subreddit__id = '2th52'
    and flair not in ('Gain', 'YOLO', 'Earnings Thread', 'Loss', 'MEME', 'Shitpost','Daily Discussion', 'Weekend Discussion')
group by 1
order by 1