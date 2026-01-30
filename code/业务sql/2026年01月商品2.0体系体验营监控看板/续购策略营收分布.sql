--SR语法
  select
  substr(paid_time,1,10) paid_day
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,course_timing_kind --时长型、到期型
  ,course_group_kind --公域主推品、私域主推品
  ,good_kind_name_level_3
  ,case when cast(strategy_type as VARCHAR) regexp '多孩策略' then '多孩策略'
        when cast(strategy_type as VARCHAR) regexp '高中囤课策略' then '高中屯课策略'
        when cast(strategy_type as VARCHAR) regexp '历史大会员续购策略' then '历史大会员续购'
        when business_good_kind_name_level_3 = '学习机加购' or cast(strategy_type as VARCHAR) regexp '学习机加购策略' then '学习机加购策略'
        else business_good_kind_name_level_3 end business_good_kind_name_level_3
  ,count(distinct case when status = "支付成功" then order_id else null end) net_pay_order_num
  ,count(distinct case when status = "支付成功" then user_id else null end) net_pay_user_num
  ,sum(amount/100) - ifnull(sum(refund_amount/100),0) as net_pay_amt
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2024-01-01' and date_sub(current_date,1)
  and order_name not like "%测试%"
  and group_name != "产研测试"
  and group_name in ('体验营','新兵营','体验营2团')
  and (business_good_kind_name_level_2 = '续购' or cast(strategy_type as VARCHAR) regexp '多孩策略|历史大会员续购策略')
  group by 1,2,3,4,5,6,7,8