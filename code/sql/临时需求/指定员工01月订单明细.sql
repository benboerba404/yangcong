-- 指定员工01月订单明细
-- 作者：AI助手
-- 日期：2026-02-05
-- 用途：统计指定7位员工在2026年01月的订单明细，包含线索领取信息

SELECT 
    o.pay_time AS `支付时间`
    ,o.order_id AS `订单号`
    ,o.user_id AS `用户ID`
    ,o.amount AS `订单金额`
    ,o.worker_name AS `坐席名称`
    ,c.created_at AS `最近一次领取时间`
    ,c.clue_source AS `领取线索来源`
    ,o.regist_time AS `用户注册时间`
FROM aws.crm_order_info o
LEFT JOIN aws.clue_info c ON o.recent_info_uuid = c.info_uuid
WHERE 
    SUBSTR(o.pay_time, 1, 7) = '2026-01'  -- 01月支付
    AND o.status = '支付成功'
    AND o.is_test = FALSE
    AND o.worker_name IN ('薛胜男', '徐哲', '李静月', '段银玉02', '郑思琪', '谭希军02', '石斐')
ORDER BY o.worker_name, o.pay_time
