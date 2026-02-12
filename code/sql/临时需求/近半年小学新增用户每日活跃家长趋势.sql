-- 近半年小学新增用户每日活跃的家长用户趋势
-- 口径：
--   新增用户 = business_user_pay_status_business = '新用户'（注册30天内且未正价付费）
--   小学 = grade IN (一年级~六年级)
--   家长 = real_identity IN ('parents', 'student_parents')（JOIN dim_user）
--   C端活跃 = product_id='01', client_os IN (android,ios,harmony), active_user_attribution='c'
--   时间范围：2025-08-01 ~ 2026-02-08

SELECT
    a.day
    ,COUNT(DISTINCT a.u_user) AS new_active_cnt
    ,COUNT(DISTINCT CASE WHEN b.real_identity IN ('parents', 'student_parents') THEN a.u_user END) AS parent_cnt
FROM dws.topic_user_active_detail_day a
LEFT JOIN dw.dim_user b ON a.u_user = b.u_user
WHERE a.day BETWEEN 20250801 AND 20260208
  AND a.product_id = '01'
  AND a.client_os IN ('android', 'ios', 'harmony')
  AND a.active_user_attribution = 'c'
  AND a.grade IN ('一年级','二年级','三年级','四年级','五年级','六年级')
  AND a.business_user_pay_status_business = '新用户'
GROUP BY a.day
ORDER BY a.day
