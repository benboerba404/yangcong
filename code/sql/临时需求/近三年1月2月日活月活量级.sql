-- 近三年（2024/2025/2026）01月02月 日活和月活用户量级
-- 口径：C端活跃（product_id='01', 移动端, C端用户归属）
-- DAU：日均活跃用户数（从日表聚合）
-- MAU：月活跃用户数（从月表聚合）

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
dau_monthly AS (
    SELECT
        CONCAT(SUBSTR(CAST(day AS STRING), 1, 4), '-', SUBSTR(CAST(day AS STRING), 5, 2)) AS year_month,
        ROUND(AVG(dau)) AS avg_dau,
        MIN(dau) AS min_dau,
        MAX(dau) AS max_dau,
        COUNT(*) AS data_days
    FROM daily_dau
    GROUP BY CONCAT(SUBSTR(CAST(day AS STRING), 1, 4), '-', SUBSTR(CAST(day AS STRING), 5, 2))
),
mau AS (
    SELECT
        CONCAT(SUBSTR(CAST(month AS STRING), 1, 4), '-', SUBSTR(CAST(month AS STRING), 5, 2)) AS year_month,
        COUNT(DISTINCT u_user) AS mau
    FROM dws.topic_user_active_detail_month
    WHERE product_id = '01'
      AND client_os IN ('android', 'ios', 'harmony')
      AND active_user_attribution IN ('中学用户', '小学用户', 'c')
      AND active_cnt > 0
      AND month IN (202401, 202402, 202501, 202502, 202601, 202602)
    GROUP BY CONCAT(SUBSTR(CAST(month AS STRING), 1, 4), '-', SUBSTR(CAST(month AS STRING), 5, 2))
)
SELECT
    COALESCE(d.year_month, m.year_month) AS `年月`,
    d.avg_dau AS `日均DAU`,
    d.min_dau AS `最低DAU`,
    d.max_dau AS `最高DAU`,
    d.data_days AS `统计天数`,
    m.mau AS `MAU`
FROM dau_monthly d
FULL OUTER JOIN mau m ON d.year_month = m.year_month
ORDER BY `年月`
