-- 本月营收与当配营收_分团_分来源
-- 需求：本月产生的营收中，当月领取线索产生的营收比例、非当月领取线索营收比例、无领取记录的线索营收比例
-- 指标：营收金额、当配营收金额、非当月领取线索营收金额、无领取记录营收金额
-- 维度：分团、分线索来源（成交前最近一次被销售领取时的来源）
-- 日期：2026-02

-- 当配营收：当月领取的线索在当月转化产生的营收
-- 非当配营收：非当月领取但当月成交的营收
-- 无领取记录营收：无任何领取记录但当月成交的营收

with t_order as (
  -- 本月全量订单
  select 
    regiment_id
    ,order_id
    ,amount
    ,recent_info_uuid  -- 成交前该坐席最近一次领取的info_uuid
  from aws.crm_order_info
  where 
    substr(pay_time, 1, 7) = '2026-02'
    and workplace_id in (4, 400, 702)
    and regiment_id not in (0, 303, 546)
    and worker_id <> 0
    and is_test = false
    and in_salary = 1
    and status = '支付成功'
)

, t_clue as (
  -- 线索表（用于获取领取时间和来源）
  select 
    info_uuid
    ,substr(created_at, 1, 7) as created_ym
    ,clue_source
  from aws.clue_info
  where user_sk > 0
)

, t_match as (
  -- 订单关联线索，分类营收
  select 
    o.regiment_id
    ,o.order_id
    ,o.amount
    ,c.clue_source
    ,case 
      when o.recent_info_uuid is null or o.recent_info_uuid = '' then '无领取记录'
      when c.created_ym = '2026-02' then '当配'
      else '非当配'
    end as revenue_type
  from t_order o
  left join t_clue c on o.recent_info_uuid = c.info_uuid
)

select 
  d.regiment_name as `团组名称`
  ,coalesce(s.clue_source_name, '无领取记录') as `线索来源`
  ,sum(t.amount) as `营收金额`
  ,sum(case when t.revenue_type = '当配' then t.amount else 0 end) as `当配营收金额`
  ,sum(case when t.revenue_type = '非当配' then t.amount else 0 end) as `非当月领取营收金额`
  ,sum(case when t.revenue_type = '无领取记录' then t.amount else 0 end) as `无领取记录营收金额`
  ,round(sum(case when t.revenue_type = '当配' then t.amount else 0 end) / sum(t.amount) * 100, 2) as `当配营收占比(%)`
  ,round(sum(case when t.revenue_type = '非当配' then t.amount else 0 end) / sum(t.amount) * 100, 2) as `非当月领取营收占比(%)`
  ,round(sum(case when t.revenue_type = '无领取记录' then t.amount else 0 end) / sum(t.amount) * 100, 2) as `无领取记录营收占比(%)`
from t_match t
left join dw.dim_crm_organization d on t.regiment_id = d.id
left join tmp.wuhan_clue_soure_name s on t.clue_source = s.clue_source
group by d.regiment_name, coalesce(s.clue_source_name, '无领取记录')
order by d.regiment_name, sum(t.amount) desc
