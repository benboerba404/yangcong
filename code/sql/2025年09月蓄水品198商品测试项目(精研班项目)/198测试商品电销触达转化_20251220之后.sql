-- 需求：2025年12月20日之后售卖的198测试商品，分日截止当前累计被电销触达情况、转化情况
-- 作者：AI Assistant
-- 日期：2026-01-30

WITH y1 AS (
  -- 获取购买198测试商品的用户（2025-12-20之后）
  SELECT DISTINCT 
    SUBSTR(paid_time, 1, 10) AS paid_date,
    SUBSTR(paid_time, 1, 19) AS paid_time,
    ab_name,
    good_type,
    u_user 
  FROM tmp.lidanping_quanyu_198test_2_goodtype
  WHERE channel = 'C端'
    AND good_type = '线索品198'  -- 如需包含其他商品类型请修改
    AND SUBSTR(paid_time, 1, 10) >= '2025-12-20'
)

, y2 AS (
  -- 关联电销触达信息，判断触达方式
  SELECT 
    paid_date,
    paid_time,
    ab_name,
    good_type,
    u_user,
    phone,
    clue_source,
    created_at,
    clue_expire_time,
    CASE 
      WHEN created_at IS NULL THEN '未触达'
      WHEN created_at <= paid_time THEN '已在库'
      WHEN clue_source = 'WeCom' THEN '企微触达'
      ELSE '电销触达' 
    END AS contact_way,
    CASE 
      WHEN created_at IS NULL THEN ''
      WHEN created_at <= paid_time THEN paid_time
      ELSE created_at 
    END AS calculate_time
  FROM (
    SELECT 
      y1.paid_date,
      y1.paid_time,
      y1.ab_name,
      y1.good_type,
      y1.u_user,
      a.phone,
      y2.clue_source,
      SUBSTR(y2.created_at, 1, 19) AS created_at,
      SUBSTR(y2.clue_expire_time, 1, 19) AS clue_expire_time,
      ROW_NUMBER() OVER (
        PARTITION BY y1.u_user, y1.good_type, y1.paid_date 
        ORDER BY y2.created_at
      ) AS rn
    FROM y1
    LEFT JOIN (
      -- 电销触达信息
      SELECT info_uuid, user_id, created_at, clue_expire_time, clue_source
      FROM aws.clue_info
      WHERE SUBSTR(clue_expire_time, 1, 10) >= '2025-12-01'
    ) y2 
      ON y1.u_user = y2.user_id 
      AND SUBSTR(y2.clue_expire_time, 1, 19) > y1.paid_time
    LEFT JOIN (
      -- 用户手机号
      SELECT 
        u_user,
        IF(phone IS NULL, phone, 
           IF(phone RLIKE "^\\d+$", phone, CAST(UNBASE64(phone) AS STRING))) AS phone
      FROM dw.dim_user
      WHERE LENGTH(phone) > 0
    ) a ON y1.u_user = a.u_user
  ) t
  WHERE rn = 1
)

, y3 AS (
  -- 转化订单表
  SELECT 
    SUBSTR(pay_time, 1, 19) AS pay_time,
    SUBSTR(pay_time, 1, 7) AS pay_ym,
    worker_id,
    order_id,
    business_good_kind_name_level_1,
    business_good_kind_name_level_2,
    user_id,
    amount,
    good_name
  FROM aws.crm_order_info
  WHERE SUBSTR(pay_time, 1, 10) BETWEEN '2025-12-01' AND DATE_SUB(CURRENT_DATE, 1)
    AND worker_id <> 0
    AND in_salary = 1
    AND is_test = FALSE
    AND status = '支付成功'
)

-- 主查询：分日累计触达率转化率
SELECT 
  paid_date,
  ab_name,
  good_type,
  contact_way,
  COUNT(DISTINCT y2.u_user) AS sale_cnt,  -- 购买人数
  COUNT(DISTINCT CASE WHEN y3.business_good_kind_name_level_1 = '组合品' THEN y3.user_id END) AS group_convert_cnt,  -- 组合品转化人数
  COUNT(DISTINCT y3.user_id) AS convert_cnt,  -- 总转化人数
  SUM(CASE WHEN y3.business_good_kind_name_level_1 = '组合品' THEN y3.amount END) AS group_amount,  -- 组合品转化金额
  SUM(y3.amount) AS convert_amount  -- 总转化金额
FROM y2 
LEFT JOIN y3
  ON y2.u_user = y3.user_id 
  AND y2.calculate_time < y3.pay_time
GROUP BY paid_date, ab_name, good_type, contact_way
ORDER BY paid_date, ab_name, good_type, contact_way
