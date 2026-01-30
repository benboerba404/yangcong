with y1 as 
(
  select distinct substr(paid_time,1,19) paid_time,ab_name,good_type,u_user 
  from tmp.lidanping_quanyu_198test_goodtype
  where channel = 'C端'
  and good_type in ('3个月同步课198','线索品198','12个月同步课498')
  and substr(paid_time,1,10) between '2025-09-18' and '2025-10-08'
  -- and u_user = '65ebec5dd65732000194f8e6'
) --同一天一个商品多次购买算一次


,y3 as --转化订单表
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
  substr(pay_time,1,10)  between '2025-09-01' and date_sub(current_date,1)
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
  and status = '支付成功'
)

select 
substr(paid_time,1,10) paid_time,ab_name,good_type
,datediff(substr(pay_time,1,10),substr(paid_time,1,10)) interval_day
,count(distinct user_id) cnt
,sum(amount) amount
from y1 
left join y3 
on y1.u_user = y3.user_id and pay_time > paid_time
where user_id is not null
group by 1,2,3,4
