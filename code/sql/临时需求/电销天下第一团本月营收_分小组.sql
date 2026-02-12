-- 电销"天下第一团"2026年2月营收_分小组
-- 日期：2026-02-12
-- 用途：查询电销"天下第一团"本月分小组营收情况

with t_order as (
  select 
    a.team_id
    ,a.order_id
    ,a.user_id
    ,a.amount
  from aws.crm_order_info a
  left join dw.dim_crm_organization d on a.regiment_id = d.id
  where 
    substr(a.pay_time, 1, 7) = '2026-02'
    and a.workplace_id in (4, 400, 702)
    and a.regiment_id not in (0, 303, 546)
    and a.worker_id <> 0
    and a.is_test = false
    and a.in_salary = 1
    and a.status = '支付成功'
    and d.regiment_name = '天下第一团'
)

select 
  f.team_name as `小组名称`
  ,count(t.order_id) as `订单量`
  ,count(distinct t.user_id) as `转化用户量`
  ,round(sum(t.amount), 2) as `营收金额`
from t_order t
left join dw.dim_crm_organization f on t.team_id = f.id
group by f.team_name
order by sum(t.amount) desc
limit 100000
