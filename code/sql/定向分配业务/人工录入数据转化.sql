with t0 as 
(
  select distinct substr(paid_time,1,10) paid_time,ab_name,good_type,u_user 
  from tmp.lidanping_quanyu_198test_goodtype
  where channel = 'C端'
  and good_type in ('3个月同步课198','线索品198','12个月同步课498')
  and substr(paid_time,1,10) between '2025-09-18' and '2025-10-08'
  union all 
  select distinct substr(paid_time,1,10) paid_time,ab_name,good_type,u_user 
  from tmp.lidanping_quanyu_198test_2_goodtype
  where channel = 'C端'
  and good_type in ('3个月同步课198','线索品198','12个月同步课498')
  and substr(paid_time,1,10) between '2025-10-20' and '2025-11-21'
) --分配下去的线索标签

,t1 as
(
  select user_id,created_at,clue_source,worker_id
  from aws.clue_info
  where substr(created_at,1,10)>='2025-10-30'
  and clue_source = 'mid_school_manual' --mid_school_manual:中学业务-人工；manual：人工录入
) --分配的线索

,t2 as --转化订单表
(
  select 
  substr(pay_time,1,19) pay_time 
  ,substr(pay_time,1,7) pay_ym 
  ,worker_id
  ,order_id
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2 
  ,user_id
  ,amount
  from aws.crm_order_info a
  where
  substr(pay_time,1,10)  between '2025-10-30' and date_sub(current_date,1)
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
  and status = '支付成功'
)

select good_type
,count(distinct u_user) `售卖量`
,count(distinct case when t1.user_id is not null then u_user end) `分配量`
,count(distinct case when t2.user_id is not null then t1.user_id end ) `转化量`
,sum(t2.amount) `转化金额`
from t0
left join t1
on t0.u_user = t1.user_id
left join t2 
on t1.user_id = t2.user_id and pay_time >= created_at
group by 1
