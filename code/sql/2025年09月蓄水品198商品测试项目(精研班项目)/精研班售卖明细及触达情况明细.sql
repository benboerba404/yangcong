-- 需求：精研班售卖明细 + 触达情况明细 + 触达转化明细
-- 说明：一个用户被多次触达展示多条记录；售卖时已在库也算触达
-- 转化归属：小学/初中学部按团内转化，高中学部按组内转化

WITH sale AS (
  -- 精研班售卖用户（2026-02-09 及之后）
  SELECT DISTINCT
    SUBSTR(paid_time, 1, 19) AS paid_time,
    good_type,
    u_user
  FROM tmp.lidanping_quanyu_198test_2_goodtype
  WHERE channel = 'C端'
    AND good_type = '线索品198'
    AND SUBSTR(paid_time, 1, 10) >= '2026-02-09' and SUBSTR(paid_time, 1, 10) <= '2026-03-08'
)

, touch AS (
  -- 触达记录：每次线索领取 = 一次触达
  SELECT
    user_id,
    SUBSTR(created_at, 1, 19)       AS touch_time,
    SUBSTR(clue_expire_time, 1, 19) AS clue_expire_time,
    clue_source,
    worker_name,
    workplace_id,
    department_id,
    regiment_id,
    team_id
  FROM aws.clue_info
  WHERE SUBSTR(clue_expire_time, 1, 10) >= '2026-02-09' and SUBSTR(created_at, 1, 10) <= '2026-03-08'
)

, conv AS (
  -- 转化订单
  SELECT
    user_id,
    SUBSTR(pay_time, 1, 19) AS pay_time,
    order_id,
    amount,
    good_name,
    worker_name  AS conv_worker_name,
    regiment_id  AS conv_regiment_id,
    team_id      AS conv_team_id
  FROM aws.crm_order_info
  WHERE SUBSTR(pay_time, 1, 10) >= '2026-02-09' and SUBSTR(pay_time, 1, 10) <= '2026-03-08'
    AND worker_id <> 0
    AND in_salary = 1
    AND is_test = FALSE
    AND status = '支付成功'
)

SELECT
  sale.paid_time                            AS `售卖时间`
  ,sale.u_user                              AS `用户ID`
  ,CASE
    WHEN touch.touch_time IS NULL          THEN '未触达'
    WHEN touch.touch_time <= sale.paid_time THEN '在库'
    WHEN touch.clue_source = 'WeCom'       THEN '企微触达'
    ELSE '电销触达'
  END                                       AS `触达状态`
  ,src.clue_source_name                     AS `触达来源`
  ,touch.worker_name                        AS `触达坐席`
  ,touch.touch_time                         AS `触达时间`
  ,d0.workplace_name                        AS `职场`
  ,d1.department_name                       AS `学部`
  ,d2.regiment_name                         AS `团`
  ,d4.team_name                             AS `小组`
  ,conv.pay_time                            AS `转化时间`
  ,conv.order_id                            AS `转化订单ID`
  ,conv.amount                              AS `转化金额`
  ,conv.good_name                           AS `转化商品`
  ,conv.conv_worker_name                    AS `转化坐席`
FROM sale
LEFT JOIN touch
  ON sale.u_user = touch.user_id
  AND touch.clue_expire_time > sale.paid_time   -- 线索未过期：含在库 + 售卖后新触达
LEFT JOIN tmp.wuhan_clue_soure_name src ON touch.clue_source = src.clue_source
LEFT JOIN dw.dim_crm_organization d0 ON touch.workplace_id = d0.id
LEFT JOIN dw.dim_crm_organization d1 ON touch.department_id = d1.id
LEFT JOIN dw.dim_crm_organization d2 ON touch.regiment_id = d2.id
LEFT JOIN dw.dim_crm_organization d4 ON touch.team_id = d4.id
LEFT JOIN conv
  ON sale.u_user = conv.user_id
  AND conv.pay_time > sale.paid_time            -- 售卖之后的转化
  AND (
    -- 小学/初中学部：团内人员转化即算转化
    (d1.department_name LIKE '%小学%' AND conv.conv_regiment_id = touch.regiment_id)
    OR
    (d1.department_name LIKE '%初中%' AND conv.conv_regiment_id = touch.regiment_id)
    OR
    -- 高中学部：组内人员转化才算转化
    (d1.department_name LIKE '%高中%' AND conv.conv_team_id = touch.team_id)
  )
ORDER BY sale.u_user, touch.touch_time
