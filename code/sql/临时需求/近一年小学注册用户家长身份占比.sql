-- 近一年小学注册用户中家长身份的用户量级及占比
-- 时间范围：2025-02 ~ 2026-01（近12个月）
-- 口径：
--   小学 = grade IN (一年级~六年级)
--   身份 = role + is_parents + real_identity 综合判断

SELECT
    substr(regist_time, 1, 7) AS regist_month
    ,CASE
        WHEN role = 'student' AND is_parents = false THEN '纯学生'
        WHEN role = 'student' AND is_parents = true  THEN '学生家长共用'
        WHEN real_identity = 'parents'               THEN '纯家长'
        ELSE '其他'
    END AS identity
    ,COUNT(DISTINCT u_user) AS user_cnt
FROM dw.dim_user
WHERE substr(regist_time, 1, 7) BETWEEN '2025-02' AND '2026-01'
  AND grade IN ('一年级','二年级','三年级','四年级','五年级','六年级')
GROUP BY
    substr(regist_time, 1, 7)
    ,CASE
        WHEN role = 'student' AND is_parents = false THEN '纯学生'
        WHEN role = 'student' AND is_parents = true  THEN '学生家长共用'
        WHEN real_identity = 'parents'               THEN '纯家长'
        ELSE '其他'
    END
ORDER BY regist_month, user_cnt DESC
