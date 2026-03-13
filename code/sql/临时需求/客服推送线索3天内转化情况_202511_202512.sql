-- 客服推送线索3天内转化情况（2025年01月 至 2026年03月）
-- 口径：不限制谁领取谁转化，按user_id关联订单，不匹配worker_id
-- 转化窗口：领取当天+后3天

with leads as (
  select
    user_id
    ,substr(created_at, 1, 7) as created_month
    ,min(substr(created_at, 1, 19)) as first_created_at
  from aws.clue_info
  where clue_source = 'custom_service_manual'
    and substr(created_at, 1, 7) between '2025-01' and '2026-03'
    and workplace_id in (4, 400, 702)
  group by user_id, substr(created_at, 1, 7)
)

, orders as (
  select
    user_id
    ,substr(pay_time, 1, 19) as pay_time
    ,amount
    ,order_id
  from aws.crm_order_info
  where substr(pay_time, 1, 10) between '2025-01-01' and '2026-03-16'
    and workplace_id in (4, 400, 702)
    and regiment_id not in (0, 303, 546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
)

, matched as (
  select
    l.created_month
    ,l.user_id
    ,o.order_id
    ,o.amount
  from leads l
  left join orders o
    on l.user_id = o.user_id
    and o.pay_time > l.first_created_at
    and to_date(o.pay_time) <= date_add(to_date(l.first_created_at), 3)
)

select
  created_month as `月份`
  ,count(distinct user_id) as `客服推送线索量`
  ,count(distinct case when order_id is not null then user_id end) as `3天内转化用户量`
  ,count(distinct order_id) as `3天内转化订单量`
  ,round(sum(amount), 2) as `3天内转化金额`
  ,concat(round(count(distinct case when order_id is not null then user_id end) * 100.0 / count(distinct user_id), 2), '%') as `3天内转化率`
from matched
group by created_month
order by created_month
limit 100000
