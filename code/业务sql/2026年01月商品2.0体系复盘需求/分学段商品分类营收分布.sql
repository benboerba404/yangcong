
with t0 as 
(
  select 
  substr(pay_time,1,19) pay_time 
  ,substr(pay_time,1,7) pay_ym 
  ,worker_id
  ,order_id
  ,case when business_good_kind_name_level_1='组合品' and string(strategy_type) regexp '多孩策略' then '多孩策略'
        when business_good_kind_name_level_1='组合品' and string(strategy_type) regexp '历史大会员续购策略' then '历史大会员续购'
        when business_good_kind_name_level_1='续购' and business_good_kind_name_level_3 = '学习机加购' then '学习机加购策略'
        when business_good_kind_name_level_1='续购' and business_good_kind_name_level_3 regexp '学段加购|培优课加购' then '学段加购'
        when business_good_kind_name_level_1='续购' and business_good_kind_name_level_3='普通续购' then '普通续购'
        when business_good_kind_name_level_1 = '组合品' then '组合品'
        else '其他' end business_good_kind_name_level_3
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
  (substr(pay_time,1,10)  between '${start_date1}' and '${end_date1}'
  or substr(pay_time,1,10)  between '${start_date2}' and '${end_date2}'
  or substr(pay_time,1,10)  between '${start_date3}' and '${end_date3}'
  or substr(pay_time,1,10)  between '${start_date4}' and '${end_date4}')
  and worker_id <> 0
  and workplace_id in (4,400,702)
  -- and in_salary = 1
  and amount > 98
  and is_test = false
  and status = '支付成功'
) --整体营收

,t1 as 
(
  select 
  distinct u_user
  ,substr(paid_time,1,19) paid_time
  ,order_id
  ,good_name
  ,business_gmv_attribution
  ,fix_good_year
  ,mid_grade
  from dws.topic_order_detail 
  where 
  business_good_kind_name_level_1 = '组合品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' 
  and status='支付成功'
  and u_user is not null
)

,t2 as --拆分多孩分布
(
  select
  pay_ym
  ,pay_time
  ,mid_stage_name
  ,mid_grade
  ,order_id
  ,amount
  ,paid_time
  ,datediff(pay_time,paid_time) interval_day
  ,case when business_good_kind_name_level_3 = '多孩策略' and datediff(pay_time,paid_time) <=30 then '30天内多孩策略'
        when business_good_kind_name_level_3 = '多孩策略' and datediff(pay_time,paid_time) > 30 then '30天外多孩策略'
        else business_good_kind_name_level_3 end business_good_kind_name_level_3
  from
  (
    select 
    t0.pay_ym
    ,t0.pay_time
    ,t0.business_good_kind_name_level_3
    ,t0.mid_stage_name
    ,t0.mid_grade
    ,t0.order_id
    ,t0.amount
    ,t1.paid_time
    ,row_number() over (partition by t0.user_id order by t1.paid_time desc) as aa
    from t0
    left join t1 
    on user_id = u_user and paid_time < pay_time
  ) a 
  where aa = 1
)

select 
pay_ym
,business_good_kind_name_level_3
,mid_stage_name
,mid_grade
,sum(amount) amount
from t2
group by 1,2,3,4
