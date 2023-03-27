with perim as (
      select *, 
        _TABLE_SUFFIX,
         max(created_utc) over(partition by _TABLE_SUFFIX) as dt_max,
         cast(concat('2023-03-28',' ',replace(_TABLE_SUFFIX,'_',':'),' ','UTC') as timestamp) as run_time
    from `trash.submissions_new_20230328_*` 
)

select _TABLE_SUFFIX, count(distinct full_id) as n_new_submission, min(created_utc) as dt_min, max(dt_max) as dt_max
from perim
where TIMESTAMP_DIFF(run_time, created_utc, MINUTE) < 10
group by 1
order by 1
