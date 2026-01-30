select 
pay_ym
,pay_time
,regiment_name
,t0.user_id
,t2.onion_id
,action_id
,order_id
,business_good_kind_name_level_3
,mid_grade
,mid_stage_name
,fix_good_year
,good_name
,amount
,call_created_at
,call_time_length
from
(
  select 
  substr(pay_time,1,7) pay_ym 
  ,substr(pay_time,1,19) pay_time 
  ,d.regiment_name
  ,user_id
  ,order_id
  ,business_good_kind_name_level_3
  ,mid_grade
  ,mid_stage_name
  ,fix_good_year
  ,good_name
  ,amount
  from aws.crm_order_info a
  left join dw.dim_crm_organization as d on a.regiment_id = d.id
  where
  substr(pay_time,1,10)  between '${start_date1}' and '${end_date1}'
  and worker_id <> 0
  and a.workplace_id in (4,400,702)
  -- and in_salary = 1
  and is_test = false
  and status = '支付成功'
  and business_good_kind_name_level_3 in ('初高品','小初高品','学段加购')
 ) t0
 left join 
 (
  select 
  user_id
  ,substr(created_at,1,19) call_created_at
  ,action_id
  ,call_time_length
  from dw.fact_call_history
  where substr(created_at,1,10) >='2025-09-20' and call_time_length >= 120
 ) t1 
 on t0.user_id = t1.user_id and date_add(substr(call_created_at,1,10),7) <= substr(pay_time,1,10)
 left join 
 (
  select u_user,onion_id
  from dw.dim_user
 ) t2
 on t1.user_id = t2.u_user
 where t1.user_id is not null
 

 
