drop table if exists tmp.tanchen_v2_tunke_zhuanhualv_all_tyy;
create table if not exists tmp.tanchen_v2_tunke_zhuanhualv_all_tyy(
  paid_day string,
  group_label string,
  paid_user_stage_name string,
  paid_user_grade string,
  zuhepin_cnt string,
  paid_order_cnt_15day string,
  paid_order_cnt_30day string,
  paid_order_cnt_45day string,
  paid_user_cnt_15day string,
  paid_user_cnt_30day string,
  paid_user_cnt_45day string,
  paid_amount_15day string,
  paid_amount_30day string,
  paid_amount_45day string
) partitioned by (dt int) 
row format delimited fields terminated by "\t";

with t0 as
(
  select
  substr(paid_time,1,19) paid_day
  ,user_id
  ,case when group_name in ('体验营','新兵营','体验营2团') then '有坐席' else '无坐席' end as group_label
  ,paid_user_stage_name --支付时用户学段
  ,paid_user_grade --支付时用户年级
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2026-01-01' and date_sub(current_date,1)
  and business_good_kind_name_level_1 = '组合品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略'
) --体验营售卖的组合品

,t1 as 
(
  select
  substr(paid_time,1,19) paid_day
  ,user_id
  ,case when group_name in ('体验营','新兵营','体验营2团') then '有坐席' else '无坐席' end as group_label
  ,qw_user_name --坐席名称
  ,auth_name --小组名称
  ,group_name --团名称
  ,strategy_type --策略名称
  ,order_id
  ,amount
  ,status
  ,refund_amount
  from aws.training_camp_order_detail
  where substr(paid_time,1,10) between '2026-01-01' and date_sub(current_date,1)
  and string(strategy_type) regexp '高中囤课策略'
) --体验营通过高中囤课策略售卖的订单

insert overwrite table tmp.tanchen_v2_tunke_zhuanhualv_all_tyy partition(dt)
select 
substr(t0.paid_day,1,10) paid_day
,t0.group_label
,t0.paid_user_stage_name
,t0.paid_user_grade
,count(distinct t0.user_id) zuhepin_cnt
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),15) then t1.order_id else null end) paid_order_cnt_15day --15天内转化订单量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then t1.order_id else null end) paid_order_cnt_30day --30天内转化订单量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),45) then t1.order_id else null end) paid_order_cnt_45day --45天内转化订单量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),15) then t1.user_id else null end) paid_user_cnt_15day --15天内转化用户量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then t1.user_id else null end) paid_user_cnt_30day --30天内转化用户量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),45) then t1.user_id else null end) paid_user_cnt_45day --45天内转化用户量
,sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then amount/100 else 0 end) - ifnull(sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),15) then refund_amount/100 else 0 end),0) as paid_amount_15day --15天内转化金额
,sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then amount/100 else 0 end) - ifnull(sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then refund_amount/100 else 0 end),0) as paid_amount_30day --30天内转化金额
,sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then amount/100 else 0 end) - ifnull(sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),45) then refund_amount/100 else 0 end),0) as paid_amount_45day --45天内转化金额
,date_format(to_date(t0.paid_day),'yyyyMMdd') dt
from t0 
left join t1 
on t0.user_id = t1.user_id and t0.group_label = t1.group_label and t1.paid_day > t0.paid_day
group by 
substr(t0.paid_day,1,10),t0.group_label
,t0.paid_user_stage_name
,t0.paid_user_grade
,date_format(to_date(t0.paid_day),'yyyyMMdd')
union all
select 
substr(t0.paid_day,1,10) paid_day
,'整体' group_label
,t0.paid_user_stage_name
,t0.paid_user_grade
,count(distinct t0.user_id) zuhepin_cnt
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),15) then t1.order_id else null end) paid_order_cnt_15day --15天内转化订单量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then t1.order_id else null end) paid_order_cnt_30day --30天内转化订单量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),45) then t1.order_id else null end) paid_order_cnt_45day --45天内转化订单量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),15) then t1.user_id else null end) paid_user_cnt_15day --15天内转化用户量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then t1.user_id else null end) paid_user_cnt_30day --30天内转化用户量
,count(distinct case when t1.status = "支付成功" and substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),45) then t1.user_id else null end) paid_user_cnt_45day --45天内转化用户量
,sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then amount/100 else 0 end) - ifnull(sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),15) then refund_amount/100 else 0 end),0) as paid_amount_15day --15天内转化金额
,sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then amount/100 else 0 end) - ifnull(sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then refund_amount/100 else 0 end),0) as paid_amount_30day --30天内转化金额
,sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),30) then amount/100 else 0 end) - ifnull(sum(case when substr(t1.paid_day,1,10) <= date_add(substr(t0.paid_day,1,10),45) then refund_amount/100 else 0 end),0) as paid_amount_45day --45天内转化金额
,date_format(to_date(t0.paid_day),'yyyyMMdd') dt
from t0 
left join t1 
on t0.user_id = t1.user_id and t1.paid_day > t0.paid_day --不限制员工是否相等
group by 
substr(t0.paid_day,1,10),t0.group_label
,t0.paid_user_stage_name
,t0.paid_user_grade
,date_format(to_date(t0.paid_day),'yyyyMMdd')

