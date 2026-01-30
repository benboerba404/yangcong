with t0 as 
(
  select 
  substr(pay_time,1,19) pay_time 
  ,substr(pay_time,1,7) pay_ym 
  ,worker_id
  ,order_id
  ,case when string(strategy_type) regexp '多孩策略' then '多孩策略'
    when string(strategy_type) regexp '高中囤课策略' then '高中屯课策略'
    when string(strategy_type) regexp '历史大会员续购策略' then '历史大会员续购'
    when business_good_kind_name_level_3 = '学习机加购' or string(strategy_type) regexp '学习机加购策略' then '学习机加购策略'
    else business_good_kind_name_level_3 end business_good_kind_name_level_3
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2 
  ,user_id
  ,amount
  ,mid_grade
  ,mid_stage_name
  ,strategy_type
  ,fix_good_year
  from aws.crm_order_info a
  where
  substr(pay_time,1,10)  between '${start_date1}' and '${end_date1}'
  and worker_id <> 0
  and workplace_id in (4,400,702)
  -- and in_salary = 1
  and is_test = false
  and (business_good_kind_name_level_2 = '续购' or string(strategy_type) regexp '历史大会员续购策略|多孩策略')
  and business_user_pay_status_business = '高净值用户'
) --续购营收

, t1 as 
(--大会员用户
  select u_user,min(substr(paid_time,1,19)) paid_time
  from dws.topic_order_detail 
  where substr(paid_time,1,10) <= '2025-03-31'
  and user_sk is not null
  and status='支付成功'
  and good_kind_name_level_2='全价大会员'
  group by u_user
)

, t2 as 
(--组合品用户
  select u_user,min(substr(paid_time,1,19)) paid_time
  from dws.topic_order_detail 
  where substr(paid_time,1,10) <= '2026-01-14'
  and user_sk is not null
  and status='支付成功'
  and (business_good_kind_name_level_1='组合品' or business_good_kind_name_level_3 = '学段加购')
  group by u_user
)

select 
substr(pay_time,1,7) month
,mid_grade
,mid_stage_name
,business_good_kind_name_level_3
,fix_good_year
,case when t1.u_user is not null then '全价大会员' 
      when t2.u_user is not null then '组合品用户'
      else '' end tag
,sum(amount) amount
from t0 
left join t1 on user_id = t1.u_user and t1.paid_time < pay_time
left join t2 on user_id = t2.u_user and t2.paid_time < pay_time
group by 1,2,3,4,5,6