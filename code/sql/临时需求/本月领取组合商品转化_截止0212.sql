-- 领取组合商品转化_202510_202602（每月截止12号，日报口径）
-- 2026-02-13

with t1 as (
  select 
    a.user_id
    ,substr(a.created_at,1,19) created_at
    ,substr(a.created_at,1,10) created_date
    ,substr(a.created_at,1,7) created_ym
    ,case when user_type_name = '续费' then '续费' 
          when user_type_name = '老未' then '老未' 
          when user_type_name = '新增' and substr(created_at,1,7) = substr(regist_time,1,7) then '新增-当月注册'
          when user_type_name = '新增' then '新增-非当月注册'
      end user_type_name
    ,clue_stage
    ,clue_grade
    ,b.clue_source_name
    ,b.clue_source_name_level_1
    ,business_user_pay_status_business
    ,case when substr(worker_join_at,1,10) = TRUNC(worker_join_at, 'month') and substr(worker_join_at,1,10) < add_months(substr(a.created_at,1,7),-2) then '老人' 
          when substr(worker_join_at,1,10) > TRUNC(worker_join_at, 'month') and substr(worker_join_at,1,10) < add_months(substr(a.created_at,1,7),-3) then '老人' 
     else '新人' end worker
    ,worker_id
    ,a.workplace_id
    ,a.department_id
    ,a.regiment_id
    ,a.team_id
  from aws.clue_info a
  left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
  where 
    substr(a.created_at,1,10) between '2025-10-01' and '2026-02-12'
    and substr(a.created_at,9,2) <= '12'
    and user_sk > 0
    and worker_id <> 0
    and a.workplace_id in (4,400,702)
    and a.regiment_id not in (0,303,546)
)

, t2 as (
  select 
    substr(pay_time,1,19) pay_time
    ,worker_id
    ,case when string(strategy_type) regexp '多孩策略|历史大会员续购策略' then '续购'
          when course_group_kind = '公域主推品' then '公域品' 
        else business_good_kind_name_level_1 end business_good_kind_name_level_1_modify
    ,user_id
    ,amount
  from aws.crm_order_info
  where
    substr(pay_time,1,10) between '2025-10-01' and '2026-02-12'
    and substr(pay_time,9,2) <= '12'
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
)

-- Step 1: 按日报粒度计算
, detail as (
  select
    t1.created_ym
    ,t1.created_date
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
    ,coalesce(sum(t2.amount), 0) as paid_amount
    ,count(distinct case when t2.business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) as group_cnt
    ,sum(case when t2.business_good_kind_name_level_1_modify = '组合品' then t2.amount else 0 end) as group_amount
  from t1
  left join t2 on t1.user_id = t2.user_id
    and t1.worker_id = t2.worker_id
    and t1.created_at < t2.pay_time
    and substr(t2.pay_time,1,10) <= concat(t1.created_ym, '-12')
  group by
    t1.created_ym
    ,t1.created_date
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

-- Step 2a: 每月截止12号整体营收（不限于领取转化）
, monthly_rev as (
  select
    substr(pay_time,1,7) as pay_ym
    ,round(sum(amount), 2) as total_revenue
    ,count(distinct user_id) as total_paid_users
    ,count(distinct order_id) as total_orders
  from aws.crm_order_info
  where
    substr(pay_time,1,10) between '2025-10-01' and '2026-02-12'
    and substr(pay_time,9,2) <= '12'
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
  group by substr(pay_time,1,7)
)

-- Step 2b: 聚合到月度 + 计算转化率/客单价/ARPU + 营收
select
  d.created_ym as `领取月份`
  -- 营收
  ,r.total_revenue as `当月营收(截止12号)`
  ,r.total_paid_users as `当月付费用户数`
  ,r.total_orders as `当月订单数`
  -- 领取
  ,sum(d.recieve_cnt) as `领取量`
  -- 整体转化
  ,sum(d.paid_cnt) as `整体转化量`
  ,round(sum(d.paid_amount), 2) as `整体转化金额`
  ,round(sum(d.paid_cnt) / sum(d.recieve_cnt) * 100, 2) as `整体转化率(%)`
  ,round(sum(d.paid_amount) / sum(d.paid_cnt), 2) as `整体客单价`
  ,round(sum(d.paid_amount) / sum(d.recieve_cnt), 2) as `整体ARPU`
  -- 组合品转化
  ,sum(d.group_cnt) as `组合品转化量`
  ,round(sum(d.group_amount), 2) as `组合品转化金额`
  ,round(sum(d.group_cnt) / sum(d.recieve_cnt) * 100, 2) as `组合品转化率(%)`
  ,round(sum(d.group_amount) / sum(d.group_cnt), 2) as `组合品客单价`
  ,round(sum(d.group_amount) / sum(d.recieve_cnt), 2) as `组合品ARPU`
from detail d
left join monthly_rev r on d.created_ym = r.pay_ym
group by d.created_ym, r.total_revenue, r.total_paid_users, r.total_orders
order by d.created_ym
limit 100000
