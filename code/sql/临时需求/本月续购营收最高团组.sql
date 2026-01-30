-- 本月"业绩最红团"的续购营收查询
-- 查询时间：2026年1月

select 
  substr(pay_time, 1, 7) as `月份`
  ,a.regiment_id as `团组ID`
  ,d.regiment_name as `团组名称`
  ,sum(case when business_good_kind_name_level_2 = '续购' 
            or string(strategy_type) regexp '历史大会员续购策略|多孩策略' 
       then amount else 0 end) as `续购营收`
  ,count(distinct case when business_good_kind_name_level_2 = '续购' 
                       or string(strategy_type) regexp '历史大会员续购策略|多孩策略' 
                  then order_id end) as `续购订单数`
  ,count(distinct case when business_good_kind_name_level_2 = '续购' 
                       or string(strategy_type) regexp '历史大会员续购策略|多孩策略' 
                  then user_id end) as `续购用户数`
from aws.crm_order_info a
left join dw.dim_crm_organization d on a.regiment_id = d.id
where 
  substr(pay_time, 1, 7) = '2026-01'
  and a.worker_id <> 0
  and a.workplace_id in (4, 400, 702)
  and a.in_salary = 1
  and a.is_test = false
  and a.status = '支付成功'
  and d.regiment_name = '业绩最红团'
group by substr(pay_time, 1, 7), a.regiment_id, d.regiment_name
