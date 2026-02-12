-- 近三年（2024/2025/2026）01月02月 每日DAU + 每月MAU
-- 口径：C端活跃（product_id='01', 移动端, C端用户归属）

WITH daily_dau AS (
    SELECT
        day,
        COUNT(DISTINCT u_user) AS dau
    FROM dws.topic_user_active_detail_day
    WHERE product_id = '01'
      AND client_os IN ('android', 'ios', 'harmony')
      AND active_user_attribution IN ('中学用户', '小学用户', 'c')
      AND is_active_user = 1
      AND SUBSTR(CAST(day AS STRING), 1, 4) IN ('2024', '2025', '2026')
      AND SUBSTR(CAST(day AS STRING), 5, 2) IN ('01', '02')
    GROUP BY day
),
monthly_mau AS (
    SELECT
        month,
        COUNT(DISTINCT u_user) AS mau
    FROM dws.topic_user_active_detail_month
    WHERE product_id = '01'
      AND client_os IN ('android', 'ios', 'harmony')
      AND active_user_attribution IN ('中学用户', '小学用户', 'c')
      AND active_cnt > 0
      AND month IN (202401, 202402, 202501, 202502, 202601, 202602)
    GROUP BY month
)
SELECT
    CONCAT(SUBSTR(CAST(d.day AS STRING), 1, 4), '-', SUBSTR(CAST(d.day AS STRING), 5, 2)) AS `年月`,
    CONCAT(SUBSTR(CAST(d.day AS STRING), 1, 4), '-', SUBSTR(CAST(d.day AS STRING), 5, 2), '-', SUBSTR(CAST(d.day AS STRING), 7, 2)) AS `日期`,
    d.dau AS `DAU`,
    m.mau AS `当月MAU`
FROM daily_dau d
LEFT JOIN monthly_mau m
    ON CAST(SUBSTR(CAST(d.day AS STRING), 1, 6) AS INT) = m.month
ORDER BY d.day
