  --SR语法
  select
  substr(paid_time,1,10) paid_day
  ,'无结营时间' operate_cut
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  and operate_at is null
  group by 1,2,3,4,5,6,7,8
union all
  select
  substr(paid_time,1,10) paid_day
  ,'结营7天内' operate_cut
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  and (operate_at is null or to_date(substr(paid_time,1,10)) <= to_date(date_add(substr(operate_at,1,10),7)))
  group by 1,2,3,4,5,6,7,8
union all
  select
  substr(paid_time,1,10) paid_day
  ,'结营14天内' operate_cut
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  and (operate_at is null or to_date(substr(paid_time,1,10)) <= to_date(date_add(substr(operate_at,1,10),14)))
  group by 1,2,3,4,5,6,7,8
union all
  select
  substr(paid_time,1,10) paid_day
  ,'结营30天内' operate_cut
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  and (operate_at is null or to_date(substr(paid_time,1,10)) <= to_date(date_add(substr(operate_at,1,10),30)))
  group by 1,2,3,4,5,6,7,8
union all
  select
  substr(paid_time,1,10) paid_day
  ,'结营60天内' operate_cut
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  and (operate_at is null or to_date(substr(paid_time,1,10)) <= to_date(date_add(substr(operate_at,1,10),60)))
  group by 1,2,3,4,5,6,7,8
union all
  select
  substr(paid_time,1,10) paid_day
  ,'结营至今' operate_cut
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  group by 1,2,3,4,5,6,7,8