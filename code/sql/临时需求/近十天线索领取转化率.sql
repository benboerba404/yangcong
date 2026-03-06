-- 近十天线索领取转化率（日报口径）
with t1 as (
  select 
    a.user_id
    ,substr(a.created_at,1,19) as created_at
    ,case when user_type_name = '续费' then '续费' 
          when user_type_name = '老未' then '老未' 
          when user_type_name = '新增' and substr(created_at,1,7) = substr(regist_time,1,7) then '新增-当月注册'
          when user_type_name = '新增' then '新增-非当月注册'
      end as user_type_name
    ,clue_stage
    ,clue_grade
    ,b.clue_source_name
    ,b.clue_source_name_level_1
    ,business_user_pay_status_business
    ,case when substr(worker_join_at,1,10) = trunc(worker_join_at, 'month') and substr(worker_join_at,1,10) < add_months(substr(a.created_at,1,7),-2) then '老人' 
          when substr(worker_join_at,1,10) > trunc(worker_join_at, 'month') and substr(worker_join_at,1,10) < add_months(substr(a.created_at,1,7),-3) then '老人' 
     else '新人' end as worker
    ,a.worker_id
    ,a.workplace_id
    ,a.department_id
    ,a.regiment_id
    ,a.team_id
  from aws.clue_info a
  left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
  where 
    substr(a.created_at,1,10) between date_sub(current_date,10) and date_sub(current_date,1)
    and user_sk > 0
    and worker_id <> 0
    and a.workplace_id in (4,400,702)
    and a.regiment_id not in (0,303,546)
)

, t2 as (
  select 
    substr(pay_time,1,19) as pay_time
    ,worker_id
    ,user_id
    ,amount
  from aws.crm_order_info
  where
    substr(pay_time,1,10) between date_sub(current_date,10) and date_sub(current_date,1)
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
)

-- Step 1: 按日报粒度聚合
, detail as (
  select
    substr(t1.created_at,1,10) as created_at
    ,t1.user_type_name
    ,t1.business_user_pay_status_business
    ,t1.clue_stage
    ,t1.clue_grade
    ,t1.clue_source_name
    ,t1.clue_source_name_level_1
    ,t1.worker
    ,t1.worker_id
    ,t1.workplace_id
    ,t1.department_id
    ,t1.regiment_id
    ,t1.team_id
    ,count(distinct t1.user_id) as recieve_cnt
    ,count(distinct t2.user_id) as paid_cnt
    ,sum(t2.amount) as paid_amount
  from t1
  left join t2 on t1.user_id = t2.user_id and t1.worker_id = t2.worker_id and t1.created_at < t2.pay_time
  group by
    substr(t1.created_at,1,10)
    ,t1.user_type_name
    ,t1.business_user_pay_status_business
    ,t1.clue_stage
    ,t1.clue_grade
    ,t1.clue_source_name
    ,t1.clue_source_name_level_1
    ,t1.worker
    ,t1.worker_id
    ,t1.workplace_id
    ,t1.department_id
    ,t1.regiment_id
    ,t1.team_id
)

-- Step 2: 按日期维度汇总
select
  created_at as `日期`
  ,sum(recieve_cnt) as `领取量`
  ,sum(paid_cnt) as `转化量`
  ,round(sum(paid_amount), 2) as `转化金额`
  ,concat(round(sum(paid_cnt) / sum(recieve_cnt) * 100, 2), '%') as `转化率`
  ,round(sum(paid_amount) / sum(paid_cnt), 2) as `客单价`
  ,round(sum(paid_amount) / sum(recieve_cnt), 2) as `ARPU`
from detail
group by created_at
order by created_at
limit 100000
