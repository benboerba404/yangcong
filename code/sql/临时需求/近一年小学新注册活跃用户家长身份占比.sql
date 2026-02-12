-- 近一年小学新注册活跃用户中家长身份占比
-- 条件：
--   1. 近一年注册（活跃表自带 regist_time）
--   2. 近一年有C端活跃记录
--   3. 身份 = 活跃表 role 字段（student/parents/teacher 等）
-- 口径：
--   C端活跃 = product_id='01', client_os IN (android,ios,harmony), active_user_attribution='c'
--   小学 = grade IN (一年级~六年级)

SELECT
    role
    ,COUNT(DISTINCT u_user) AS user_cnt
FROM dws.topic_user_active_detail_day
WHERE day BETWEEN 20250201 AND 20260131
  AND product_id = '01'
  AND client_os IN ('android', 'ios', 'harmony')
  AND active_user_attribution = 'c'
  AND grade IN ('一年级','二年级','三年级','四年级','五年级','六年级')
  AND substr(cast(regist_time as string), 1, 7) BETWEEN '2025-02' AND '2026-01'
GROUP BY role
ORDER BY user_cnt DESC
