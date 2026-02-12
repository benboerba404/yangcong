/**
 * 人工录入线索整体转化情况及外呼情况（历史组合品数据）
 * 口径：录入时间 2026-01-21 ~ 2026-01-22，note 包含「历史组合品数据」，转化/外呼截止 2026-01-29
 * 指标：分配量、外呼次数、外呼量、接通量、有效接通量、转化量、转化金额
 */
with t1 as
(
  /** 人工录入线索：录入时间在指定区间，note 包含「历史组合品数据」 */
  select user_id, substr(created_at, 1, 19) created_at
  from aws.clue_info
  where substr(created_at, 1, 10) between '2026-01-21' and '2026-01-22'
    and clue_source = 'manual'  /* 人工录入*/
    and (note like '%历史组合品数据%' or note regexp '历史组合品数据')
)
, t2 as
(
  /** 转化订单：支付时间截止 2026-01-29，且为录入后产生的订单 */
  select
    user_id,
    substr(pay_time, 1, 19) pay_time,
    order_id,
    amount
  from aws.crm_order_info
  where substr(pay_time, 1, 10) <= '2026-02-10'
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
    and status = '支付成功'
)
/** 外呼记录：针对本批线索，外呼时间在录入后且截止 2026-01-29 */
, t_call as
(
  select
    t1.user_id,
    count(distinct a.action_id) as `外呼次数`,
    if(sum(a.is_connect) > 0, 1, 0) as `是否接通`,
    if(sum(a.is_valid_connect) > 0, 1, 0) as `是否有效接通`
  from t1 
  left join dw.fact_call_history a 
  on t1.user_id = a.user_id and a.created_at >= t1.created_at
  where a.user_id is not null
  and substr(a.created_at, 1, 10) between '2026-01-21' and '2026-02-10'
  group by t1.user_id
)
/** 分配量、外呼量、外呼次数、接通量、有效接通量、转化量、转化金额 */

  select
  count(distinct t1.user_id) as `分配量`,
  count(distinct t_call.user_id) as `外呼量`,
  sum(t_call.`外呼次数`) as `外呼次数`,
  count(distinct case when t_call.`是否接通` = 1 then t1.user_id end) as `接通量`,
  count(distinct case when t_call.`是否有效接通` = 1 then t1.user_id end) as `有效接通量`,
  count(distinct case when t2.order_id is not null then t1.user_id end) as `转化量`,
  sum(t2.amount) as `转化金额`
  from t1
  left join t_call
  on t1.user_id = t_call.user_id
  left join t2
  on t1.user_id = t2.user_id and t2.pay_time >= t1.created_at

