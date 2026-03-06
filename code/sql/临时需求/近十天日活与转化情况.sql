-- 近十天日活与转化情况
with dau as (
  select
    concat(substr(cast(day as string), 1, 4), '-', substr(cast(day as string), 5, 2), '-', substr(cast(day as string), 7, 2)) as dt
    ,count(distinct u_user) as dau
  from dws.topic_user_active_detail_day
  where day between cast(date_format(date_sub(current_date, 10), 'yyyyMMdd') as int)
                and cast(date_format(date_sub(current_date, 1), 'yyyyMMdd') as int)
    and product_id = '01'
    and client_os in ('android', 'ios', 'harmony')
    and active_user_attribution in ('中学用户', '小学用户', 'c')
  group by day
)

, order_summary as (
  select
    substr(paid_time, 1, 10) as dt
    ,count(distinct case when business_gmv_attribution = '电销' then u_user end) as dx_paid_cnt
    ,count(distinct case when business_gmv_attribution = '商业化' then u_user end) as biz_paid_cnt
    ,round(sum(case when business_gmv_attribution = '电销' then sub_amount else 0 end), 2) as dx_paid_amount
    ,round(sum(case when business_gmv_attribution = '商业化' then sub_amount else 0 end), 2) as biz_paid_amount
  from dws.topic_order_detail
  where substr(paid_time, 1, 10) between date_sub(current_date, 10) and date_sub(current_date, 1)
    and status = '支付成功'
    and is_test_user = 0
  group by substr(paid_time, 1, 10)
)

select
  a.dt as `日期`
  ,a.dau as `日活量`
  ,coalesce(b.dx_paid_cnt, 0) as `电销转化量`
  ,coalesce(b.biz_paid_cnt, 0) as `商业化转化量`
  ,coalesce(b.dx_paid_amount, 0) as `电销转化金额`
  ,coalesce(b.biz_paid_amount, 0) as `商业化转化金额`
from dau a
left join order_summary b on a.dt = b.dt
order by a.dt
limit 100000
