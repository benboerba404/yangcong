SELECT 
    business_good_kind_name_level_1,
    business_good_kind_name_level_2,
    business_good_kind_name_level_3,
    COUNT(DISTINCT order_id) AS order_cnt,
    ROUND(SUM(amount), 2) AS revenue
FROM aws.crm_order_info
WHERE pay_time >= '2026-01-01'
  AND status = '支付成功'
  AND is_test = FALSE
GROUP BY business_good_kind_name_level_1, business_good_kind_name_level_2, business_good_kind_name_level_3
ORDER BY revenue DESC
