drop table if exists tmp.tanchen_v2_xiaoxuepin_zhuanhualv_dx;
create table if not exists tmp.tanchen_v2_xiaoxuepin_zhuanhualv_dx(
  pay_day string,
  mid_grade string,
  mid_stage_name string,
  business_good_kind_name_level_3 string,
  xiaoxuepin_cnt string,
  paid_current_month_cnt string,
  paid_next1_month_cnt string,
  paid_next2_month_cnt string,
  paid_next3_month_cnt string,
  paid_next6_month_cnt string,
  paid_next12_month_cnt string,
  paid_cnt string,
  paid_current_month_amount string,
  paid_next1_month_amount string,
  paid_next2_month_amount string,
  paid_next3_month_amount string,
  paid_next6_month_amount string,
  paid_next12_month_amount string,
  paid_amount string
) partitioned by (dt int) 
row format delimited fields terminated by "\t";


--小学品升级率_整体不分团组
with t0 as
(
  select distinct
  substr(pay_time,1,19) pay_day
  ,user_id
  ,good_name
  ,mid_grade
  ,mid_stage_name
  ,business_good_kind_name_level_3
  from aws.crm_order_info a
  where 
  substr(pay_time,1,10) between '2023-03-01' and  date_sub(current_date,1)
  and a.workplace_id in (4,400,702) --职场归属武汉电销和长沙电销
  and a.regiment_id  not in (303,0,546) --剔除体验营、私域阿拉丁、无团队归属
  and a.worker_id <> 0
  and a.is_test = false
  and a.in_salary =1 
  and (a.business_good_kind_name_level_3 = '小学品' or good_kind_name_level_3 = '小初品-4年同步课')
-- and a.status = '支付成功'
) --购买小学品的用户

,t1 as
(
  select distinct
  substr(pay_time,1,19) pay_day
  ,user_id
  ,amount
  from aws.crm_order_info a
  where 
  substr(pay_time,1,10) between '2023-03-01' and  date_sub(current_date,1)
  and a.workplace_id in (4,400,702) --职场归属武汉电销和长沙电销
  and a.regiment_id  not in (303,0,546) --剔除体验营、私域阿拉丁、无团队归属
  and a.worker_id <> 0
  and a.is_test = false
  -- and a.in_salary =1 
  and a.amount >= 98 --多孩298的不算业绩算转化
  and a.business_good_kind_name_level_1 in ('组合品','续购')
) --转化组合品、续购品数据

insert overwrite table tmp.tanchen_v2_xiaoxuepin_zhuanhualv_dx partition(dt)
select 
substr(pay_day,1,10) pay_day
,mid_grade
,mid_stage_name
,business_good_kind_name_level_3
,count(distinct user_id) xiaoxuepin_cnt
,count(distinct case when paid_user_id is not null and substr(pay_day,1,7)=substr(paid_day,1,7) then user_id end) paid_current_month_cnt
,count(distinct case when paid_user_id is not null and trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),1) then user_id end) paid_next1_month_cnt
,count(distinct case when paid_user_id is not null and trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),2) then user_id end) paid_next2_month_cnt
,count(distinct case when paid_user_id is not null and trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),3) then user_id end) paid_next3_month_cnt
,count(distinct case when paid_user_id is not null and trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),6) then user_id end) paid_next6_month_cnt
,count(distinct case when paid_user_id is not null and trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),12) then user_id end) paid_next12_month_cnt
,count(distinct case when paid_user_id is not null then user_id end) paid_cnt
,sum(case when substr(pay_day,1,7)=substr(paid_day,1,7) then amount end) paid_current_month_amount
,sum(case when trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),1) then amount end) paid_next1_month_amount
,sum(case when trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),2) then amount end) paid_next2_month_amount
,sum(case when trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),3) then amount end) paid_next3_month_amount
,sum(case when trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),6) then amount end) paid_next6_month_amount
,sum(case when trunc(substr(paid_day,1,10),'MM') <= add_months(trunc(substr(pay_day,1,10),'MM'),12) then amount end) paid_next12_month_amount
,sum(amount) paid_amount
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