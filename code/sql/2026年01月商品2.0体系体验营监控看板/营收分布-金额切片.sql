--SR语法
select 
paid_day
,case when net_pay_amt <=1000 then '[0,1000]'
      when net_pay_amt <=2000 then '(1000,2000]'
      when net_pay_amt <=3000 then '(2000,3000]'
      when net_pay_amt <=4000 then '(3000,4000]'
      when net_pay_amt <=5000 then '(4000,5000]'
      when net_pay_amt <=6000 then '(5000,6000]'
      else '6000以上' end amount_cut
,qw_user_name
,auth_name
,group_name
,business_good_kind_name_level_1
,business_good_kind_name_level_2
,business_good_kind_name_level_3
,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
,sum(net_pay_amt) as net_pay_amt
from 
(
  select
  substr(paid_time,1,10) paid_day
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,user_id
  ,order_id
  ,status
  ,amount/100 - ifnull(refund_amount/100,0) net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团') --有坐席归属
) t0
group by 1,2,3,4,5,6,7,8
