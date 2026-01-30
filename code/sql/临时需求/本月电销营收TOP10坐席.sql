-- 本月电销营收排名前10坐席
-- 作者：AI助手
-- 日期：2026-01-30
-- 用途：取本月电销营收排名前10的坐席，包含坐席名称、小组、团队、营收金额、订单量、客单价

SELECT 
  worker_name AS `坐席名称`
  ,team_name AS `小组`
  ,regiment_name AS `团队`
  ,ROUND(SUM(amount), 2) AS `营收金额`
  ,COUNT(DISTINCT order_id) AS `订单量`
  ,ROUND(SUM(amount) / COUNT(DISTINCT order_id), 2) AS `客单价`
FROM (
  SELECT 
    a.worker_id
    ,a.worker_name
    ,f.team_name
    ,d.regiment_name
    ,a.order_id
    ,a.amount
  FROM aws.crm_order_info a
  LEFT JOIN dw.dim_crm_organization AS d ON a.regiment_id = d.id
  LEFT JOIN dw.dim_crm_organization AS f ON a.team_id = f.id
  WHERE 
    SUBSTR(pay_time, 1, 10) BETWEEN TRUNC(CURRENT_DATE, 'MM') AND DATE_SUB(CURRENT_DATE, 1)  -- 本月1号至昨天
    AND a.workplace_id IN (4, 400, 702)  -- 武汉电销和长沙电销职场
    AND a.regiment_id NOT IN (303, 0, 546)  -- 剔除体验营、私域阿拉丁、无团队归属
    AND a.worker_id <> 0
    AND a.is_test = FALSE
    AND a.in_salary = 1  -- 计入薪资
    AND a.status = '支付成功'
) t
GROUP BY worker_name, team_name, regiment_name
ORDER BY `营收金额` DESC
LIMIT 10
