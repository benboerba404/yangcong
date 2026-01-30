DROP TABLE IF EXISTS tmp.wuhan_crm_channel_register_add_month；
CREATE TABLE  IF NOT EXISTS tmp.wuhan_crm_channel_register_add_month AS(
with  t0 as (
select 
  substr(from_unixtime(unix_timestamp(cast(day as string),'yyyyMMdd'),'yyyy-MM-dd'),1,7) month
  ,u_user 
  ,min(from_unixtime(unix_timestamp(cast(day as string),'yyyyMMdd'),'yyyy-MM-dd')) day
from aws.user_increase_new_add_day --新注册用户id
  where day between  20220117 and cast(regexp_replace(date_sub(current_date,1), '-', '') as int)
  and u_from in ('ios','android')
  and user_attribution in ('中学用户','小学用户','c')
group by   
  substr(from_unixtime(unix_timestamp(cast(day as string),'yyyyMMdd'),'yyyy-MM-dd'),1,7) 
  ,u_user 
)

, t1 as (
select 
  a.month
  ,a.created_at
  ,a.external_user_id
  ,b.user_id
  ,a.level_1
  ,a.level_2
from
(
select month,external_user_id,level_1,level_2,channel_id,created_at 
from (
  select distinct 
    substr(created_at,1,7) month
    ,external_user_id
    ,level_1,level_2
    ,a.channel_id
    ,substr(created_at,1,10) created_at
    ,row_number() over(partition by external_user_id,substr(created_at,1,7)  order by created_at) rk
  from crm.contact_log a
  left join tmp.wuhan_wecom_channel_id  b on a.channel_id = b.id
    where source=3 
      and change_type='add_external_contact'
      and SUBSTR(created_at,1,10) between  '2022-01-17' and date_sub(current_date,1)
      and b.type regexp '企微'
      )
   where rk = 1
) a--添加微信量
left join 
(
  select distinct
    substr(created_at,1,7) month
    ,user_id 
    ,external_user_id
    ,channel_id
  from crm.new_user 
    where channel=3
      and substr(created_at,1,10) between '2022-01-17' and date_sub(current_date,1)
      and length(external_user_id)>0
) b--取user_id
  on a.month = b.month and a.external_user_id = b.external_user_id and  b.channel_id = a.channel_id
where length(b.user_id)>0
)


, t2 as (
SELECT distinct
  substr(pay_time,1,7) month
  ,substr(pay_time,1,10) pay_time
  ,user_id pay_user_id
  ,order_id
  ,amount
FROM aws.crm_order_info
WHERE SUBSTR(pay_time,1,10) between '2022-01-17' and date_sub(current_date,1)
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
--  and ((substr(pay_time,1,10) between '2022-01-17'  and '2023-12-31') or (substr(pay_time,1,10) between '2024-01-01' and date_sub(current_date,1) and worker_id <> 0 ))
)

, t3 as (
select distinct
t0.month
,t0.day
,t0.u_user 
,t1.level_1,t1.level_2
,t1.user_id
,t2.pay_user_id
,t2.order_id
,t2.amount
from t0
left join t1 on t0.month = t1.month and t1.created_at >= t0.day and t0.u_user = t1.user_id
left join t2 on t1.month = t2.month and t2.pay_time >= t1.created_at and t1.user_id = t2.pay_user_id
)


select 
  month 
  ,count(distinct u_user) register_cnt
  ,count(distinct case when user_id is not null then u_user end) add_cnt
  ,count(distinct case when pay_user_id is not null then u_user end) paid_cnt
  ,count(distinct order_id) ord_cnt
  ,ifnull(sum(amount),0) amount
from t3 
group by month