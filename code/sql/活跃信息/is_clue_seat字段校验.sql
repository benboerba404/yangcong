-- is_clue_seat 字段校验
-- 对比表自带字段与通过线索表计算的在库状态

with t_active as (
  -- 取一个月样本数据
  select 
    day
    ,u_user
    ,is_clue_seat
  from dws.topic_user_active_detail_day
  where day = 20260115  -- 取一天样本
    and mid_stage_name in ('初中', '高中')
)

, t_clue as (
  -- 线索在库状态（活跃日在领取日和过期日之间）
  select 
    user_id
    ,substr(created_at, 1, 10) as clue_start_date
    ,substr(clue_expire_time, 1, 10) as clue_end_date
  from aws.clue_info
  where user_sk > 0
    and worker_id <> 0
    and workplace_id in (4, 400, 702)
    and regiment_id not in (0, 303, 546)
)

, t_compare as (
  select 
    a.u_user
    ,a.is_clue_seat as table_is_clue_seat
    ,case when c.user_id is not null then 1 else 0 end as calc_is_clue_seat
  from t_active a
  left join t_clue c 
    on a.u_user = c.user_id
    and '2026-01-15' >= c.clue_start_date
    and '2026-01-15' <= c.clue_end_date
)

select 
  '2026-01-15' as `校验日期`
  ,count(distinct u_user) as `活跃用户量`
  ,count(distinct case when table_is_clue_seat = 1 then u_user end) as `表字段在库量`
  ,count(distinct case when calc_is_clue_seat = 1 then u_user end) as `计算在库量`
  ,count(distinct case when table_is_clue_seat = calc_is_clue_seat then u_user end) as `匹配用户量`
  ,count(distinct case when table_is_clue_seat <> calc_is_clue_seat then u_user end) as `不匹配用户量`
  ,round(count(distinct case when table_is_clue_seat = calc_is_clue_seat then u_user end) * 100.0 
    / count(distinct u_user), 2) as `匹配率(%)`
from t_compare
