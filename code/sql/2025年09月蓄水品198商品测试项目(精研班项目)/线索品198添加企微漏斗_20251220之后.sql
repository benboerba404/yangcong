-- 需求：2025年12月20号之后购买线索品198的用户添加企微的整体漏斗情况，分日
-- 活码渠道：575,576,577,613,614,615,572,573,574,622,623,624,625,626,627,619,620,621,628,629,630
-- 作者：AI Assistant
-- 日期：2026-01-30

WITH t0 AS (
  -- 购买线索品198的用户
  SELECT DISTINCT 
    SUBSTR(paid_time, 1, 10) AS paid_date,
    u_user
  FROM tmp.lidanping_quanyu_198test_2_goodtype
  WHERE channel = 'C端'
    AND good_type = '线索品198'
    AND SUBSTR(paid_time, 1, 10) >= '2025-12-20'
)

, t1 AS (
  -- 企微添加漏斗数据
  SELECT DISTINCT 
    FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(day AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS day,
    task_id,
    b.channel_name,
    click_entrance_user,
    get_wechat_user,
    add_wechat_user,
    pull_wechat_user
  FROM aws.user_pay_process_add_wechat_day a
  LEFT JOIN tmp.wuhan_wecom_channel_id b ON a.task_id = b.id
  WHERE day >= 20251220
    AND click_entrance_user IS NOT NULL
    AND task_id IN (575,576,577,613,614,615,572,573,574,622,623,624,625,626,627,619,620,621,628,629,630)
)

SELECT 
  paid_date,
  COUNT(DISTINCT u_user) AS `购买人数`,
  COUNT(DISTINCT CASE WHEN click_entrance_user IS NOT NULL THEN u_user END) AS `资源位点击量`,
  COUNT(DISTINCT CASE WHEN get_wechat_user IS NOT NULL THEN u_user END) AS `坐席二维码曝光量`,
  COUNT(DISTINCT CASE WHEN add_wechat_user IS NOT NULL THEN u_user END) AS `企微添加量`,
  COUNT(DISTINCT CASE WHEN pull_wechat_user IS NOT NULL THEN u_user END) AS `拉取入库量`
FROM t0 
LEFT JOIN t1 ON u_user = click_entrance_user
GROUP BY paid_date 
ORDER BY paid_date
