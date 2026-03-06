-- 2025年01月-2026年02月各团分线索来源营收分布
-- 维度：月份、团队、线索来源（电话线索/企微线索/转介绍/其他/无领取记录）、是否当月领取
-- 线索归因口径：recent_info_uuid（一线口径）
-- 营收口径：电销订单表 aws.crm_order_info，status=支付成功

with orders as (
  select
    substr(o.pay_time, 1, 7)                          as pay_ym
    ,o.order_id
    ,o.regiment_id
    ,o.amount
    ,case
      when c.info_uuid is null                         then '无领取记录'
      when c.clue_source = 'mid_school'                then '电话线索'
      when c.clue_source = 'WeCom'                     then '企微线索'
      when c.clue_source = 'referral'                  then '转介绍'
      else '其他'
    end                                                as clue_source_group
    ,case
      when c.info_uuid is null                         then '无领取记录'
      when substr(c.created_at, 1, 7) = substr(o.pay_time, 1, 7) then '当月'
      else '非当月'
    end                                                as is_same_month
  from aws.crm_order_info o
  left join aws.clue_info c on o.recent_info_uuid = c.info_uuid
  where substr(o.pay_time, 1, 7) between '2025-01' and '2026-02'
    and o.workplace_id in (4, 400, 702)
    and o.regiment_id not in (0, 303, 546)
    and o.worker_id <> 0
    and o.in_salary = 1
    and o.is_test = false
    and o.status = '支付成功'
)

select
  o.pay_ym                                             as `月份`
  ,d.regiment_name                                     as `团队`
  ,o.clue_source_group                                 as `线索来源`
  ,o.is_same_month                                     as `是否当月领取`
  ,sum(o.amount)                                       as `营收`
  ,count(distinct o.order_id)                          as `订单量`
from orders o
left join dw.dim_crm_organization d on o.regiment_id = d.id
group by
  o.pay_ym
  ,d.regiment_name
  ,o.clue_source_group
  ,o.is_same_month
order by
  o.pay_ym
  ,d.regiment_name
limit 100000
