-- 从小学物理商品销售人员明细
-- 用途：查看2025-10-09至今哪些坐席售卖了"从小学物理"商品，用于销售访谈
-- 日期：2026-03-10

select
    d1.department_name as `学部`
    ,d2.regiment_name as `团`
    ,d3.heads_name as `主管组`
    ,d4.team_name as `小组`
    ,t.worker_id as `坐席ID`
    ,t.worker_name as `坐席名称`
    ,count(distinct t.order_id) as `销售单量`
    ,sum(t.amount) as `销售金额`
from aws.crm_order_info t
left join dw.dim_crm_organization d1 on t.department_id = d1.id
left join dw.dim_crm_organization d2 on t.regiment_id = d2.id
left join dw.dim_crm_organization d3 on t.heads_id = d3.id
left join dw.dim_crm_organization d4 on t.team_id = d4.id
where t.workplace_id in (4, 400, 702)
  and t.regiment_id not in (0, 303, 546)
  and t.worker_id <> 0
  and t.in_salary = 1
  and t.is_test = false
  and t.status = '支付成功'
  and t.good_name like '%从小学物理%'
  and substr(t.pay_time, 1, 10) >= '2025-10-09'
  and substr(t.pay_time, 1, 10) <= '2026-03-10'
group by
    d1.department_name
    ,d2.regiment_name
    ,d3.heads_name
    ,d4.team_name
    ,t.worker_id
    ,t.worker_name
order by count(distinct t.order_id) desc
limit 100000
