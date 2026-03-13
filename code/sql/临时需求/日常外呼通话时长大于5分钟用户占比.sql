-- 日常外呼通话时长>5分钟用户占比（按月）
-- 2026-03-12

select
  concat(substr(cast(day as string), 1, 4), '-', substr(cast(day as string), 5, 2)) as `月份`
  ,count(distinct user_id) as `外呼用户量`
  ,count(distinct case when call_time_length > 300 then user_id end) as `通话时长大于5分钟用户量`
  ,round(count(distinct case when call_time_length > 300 then user_id end) * 1.0
         / count(distinct user_id), 4) as `占比`
from dw.fact_call_history
where day between 20250101 and 20260312
group by concat(substr(cast(day as string), 1, 4), '-', substr(cast(day as string), 5, 2))
order by `月份`
limit 100000
