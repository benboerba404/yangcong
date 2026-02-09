-- 指定员工01月营收及当月注册用户营收
-- 作者：AI助手
-- 日期：2026-02-05
-- 用途：统计指定7位员工在2026年01月的总营收及当月注册用户贡献的营收

SELECT 
    worker_name AS `员工姓名`
    ,ROUND(SUM(amount), 2) AS `01月营收`
    ,ROUND(SUM(CASE WHEN SUBSTR(regist_time, 1, 7) = '2026-01' THEN amount ELSE 0 END), 2) AS `当月注册用户营收`
FROM aws.crm_order_info
WHERE 
    SUBSTR(pay_time, 1, 7) = '2026-01'  -- 01月支付
    AND status = '支付成功'
    AND is_test = FALSE
    AND worker_name IN ('薛胜男', '徐哲', '李静月', '段银玉02', '郑思琪', '谭希军02', '石斐')
GROUP BY worker_name
ORDER BY `01月营收` DESC
