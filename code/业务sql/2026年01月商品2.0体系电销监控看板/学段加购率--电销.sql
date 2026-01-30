drop table if exists tmp.tanchen_v2_duohai_zhuanhualv_dx;
create table if not exists tmp.tanchen_v2_duohai_zhuanhualv_dx(
pay_day string,
mid_grade string,
mid_stage_name string,
business_good_kind_name_level_3 string,
zuhe_cnt string,
paid_duohai_cnt_30day string,
paid_duohai_amount_30day string
) partitioned by (dt int) 
row format delimited fields terminated by "\t";

--2.0商品体系多孩策略/学段加购率
with t0 as
(
  select distinct
  substr(pay_time,1,19) pay_day
  ,user_id
  ,mid_grade
  ,mid_stage_name
  ,business_good_kind_name_level_3
  from aws.crm_order_info a
  where 
  substr(pay_time,1,10) between '2026-01-01' and  date_sub(current_date,1)
  and a.workplace_id in (4,400,702) --职场归属武汉电销和长沙电销
  and a.regiment_id  not in (303,0,546) --剔除体验营、私域阿拉丁、无团队归属
  and a.worker_id <> 0
  and a.is_test = false
  and a.in_salary =1 
  and a.business_good_kind_name_level_1 = '组合品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' 
-- and a.status = '支付成功'
) --购买组合品的用户

,t1 as
(
  select distinct
  substr(pay_time,1,19) pay_day
  ,user_id
  ,amount
  from aws.crm_order_info a
  where 
  substr(pay_time,1,10) between '2026-01-01' and  date_sub(current_date,1)
  and a.workplace_id in (4,400,702) --职场归属武汉电销和长沙电销
  and a.regiment_id  not in (303,0,546) --剔除体验营、私域阿拉丁、无团队归属
  and a.worker_id <> 0
  and a.is_test = false
  -- and a.in_salary =1 多孩第二笔可能支付298，不算一线业绩
  and a.amount >= 98
  and string(a.strategy_type) regexp '多孩策略'
) --转化数据

insert overwrite table tmp.tanchen_v2_duohai_zhuanhualv_dx partition(dt)
select 
substr(pay_day,1,10) pay_day
,mid_grade
,mid_stage_name
,business_good_kind_name_level_3
,count(distinct user_id) zuhe_cnt
,count(distinct case when paid_user_id is not null and substr(paid_day,1,10) <= date_add(substr(pay_day,1,10),30) then user_id end) paid_duohai_cnt_30day
,sum(case when substr(paid_day,1,10) <= date_add(substr(pay_day,1,10),30) then amount end) paid_duohai_amount_30day
,date_format(to_date(pay_day),'yyyyMMdd') dt
from 
(
  select 
  t0.pay_day
  ,t0.user_id
  ,t0.mid_grade
  ,t0.mid_stage_name
  ,t0.business_good_kind_name_level_3
  ,t1.pay_day paid_day
  ,t1.user_id paid_user_id
  ,t1.amount
  from t0 
  left join t1
  on t0.user_id = t1.user_id and t0.pay_day < t1.pay_day
) a
group by 
substr(pay_day,1,10)
,mid_grade
,mid_stage_name
,business_good_kind_name_level_3
,date_format(to_date(pay_day),'yyyyMMdd')