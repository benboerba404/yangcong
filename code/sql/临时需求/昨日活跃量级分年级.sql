-- yesterday active users by grade (C-side)
-- date: 2026-02-05
SELECT 
    mid_grade,
    COUNT(DISTINCT u_user) AS active_users
FROM dws.topic_user_active_detail_day
WHERE day = 20260205
  AND product_id = '01'
  AND client_os IN ('android', 'ios', 'harmony')
  AND active_user_attribution IN ('中学用户', '小学用户', 'c')
  AND is_active_user = 1
GROUP BY mid_grade
ORDER BY active_users DESC
