with t1 as 
(
  select 
  a.user_id
  ,substr(a.created_at,1,19) created_at
  ,substr(a.created_at,1,7) created_ym
  ,clue_stage
  ,clue_grade
  ,b.clue_source_name
  ,b.clue_source_name_level_1
  ,business_user_pay_status_business
  ,case when user_type_name = '续费' then '续费' 
        when user_type_name = '老未' then '老未' 
        when user_type_name = '新增' and substr(created_at,1,7) = substr(regist_time,1,7)  then '新增-当月注册'
        when user_type_name = '新增' then '新增-非当月注册'
    end user_type_name
  ,worker_join_at
  ,worker_id
  ,worker_name
  ,workplace_id
  ,department_id
  ,regiment_id
  ,team_id
  from aws.clue_info a
  left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
  where 
  (substr(a.created_at,1,10) between '${start_date1}' and '${end_date1}'
  or substr(a.created_at,1,10) between '${start_date2}' and '${end_date2}'
  or substr(a.created_at,1,10) between '${start_date3}' and '${end_date3}')
  and user_sk > 0
  and worker_id <> 0
  and a.workplace_id in (4,400,702)
)

, t2 as 
(
  select 
  substr(pay_time,1,19) pay_time 
  ,substr(pay_time,1,7) pay_ym 
  ,worker_id
  ,order_id
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2 
  ,user_id
  ,amount
  ,strategy_type
  from aws.crm_order_info a
  where
  (substr(pay_time,1,10)  between '${start_date1}' and '${end_date1}'
  or substr(pay_time,1,10)  between '${start_date2}' and '${end_date2}'
  or substr(pay_time,1,10)  between '${start_date3}' and '${end_date3}')
  and worker_id <> 0
  and workplace_id in (4,400,702)
  -- and in_salary = 1
  and is_test = false
)

,y0 as
(
  select   
  substr(t1.created_at,1,7) created_month
  ,substr(t1.created_at,1,10) created_at
  ,t1.user_type_name
  ,t1.clue_stage
  ,t1.clue_grade
  ,t1.worker_id
  ,t1.worker_name
  ,d0.workplace_name
  ,d1.department_name
  ,d2.regiment_name
  ,d4.team_name
  ,count(distinct t1.user_id) recieve_cnt
  --整体转化量
  ,count(distinct case when t2.user_id is not null then t2.user_id end)  paid_cnt--`领取线索当日转化量`
  --组合品转化量
  ,count(distinct case when business_good_kind_name_level_1 ='组合品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' then t2.user_id end) group_cnt
  --续购品转化量
  ,count(distinct case when business_good_kind_name_level_1 ='续购' or string(strategy_type) regexp '多孩策略|历史大会员续购策略' then t2.user_id end) xugou_cnt
  --单学段商品转化量
  ,count(distinct case when business_good_kind_name_level_2 = '单学段商品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' then t2.user_id end) single_cnt
  --多学段商品转化量
  ,count(distinct case when business_good_kind_name_level_2 = '多学段商品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' then t2.user_id end) multi_cnt
  --多孩转化量
  ,count(distinct case when string(strategy_type) regexp '多孩策略' then t2.user_id end) duohai_xugou_cnt
  --大会员续购转化量
  ,count(distinct case when string(strategy_type) regexp '历史大会员续购策略' then t2.user_id end) dahuiyuan_xugou_cnt 
  --高中囤课包转化量
  ,count(distinct case when string(strategy_type) regexp '高中囤课策略' then t2.user_id end) gaozhongtunke_xugou_cnt 
  
  --整体转化营收
  ,sum(amount)  paid_amount--`领取线索转化金额`
  --组合品转化营收
  ,sum(case when business_good_kind_name_level_1 ='组合品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' then amount end) group_amount
  --续购品转化营收
  ,sum(case when business_good_kind_name_level_1 ='续购' or string(strategy_type) regexp '多孩策略|历史大会员续购策略' then amount end) xugou_amount
  --单学段商品转化营收
  ,sum(case when business_good_kind_name_level_2 = '单学段商品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' then amount end) single_amount
  --多学段商品转化营收
  ,sum(case when business_good_kind_name_level_2 = '多学段商品' and string(strategy_type) not regexp '多孩策略|历史大会员续购策略' then amount end) multi_amount
  --多孩转化营收
  ,sum(case when string(strategy_type) regexp '多孩策略' then amount end) duohai_xugou_amount
  --大会员续购转化营收
  ,sum(case when string(strategy_type) regexp '历史大会员续购策略' then amount end) dahuiyuan_xugou_amount
  --高中囤课包转化营收
  ,sum(case when string(strategy_type) regexp '高中囤课策略' then amount end) gaozhongtunke_xugou_amount
  
  from t1
  left join t2 on t1.user_id = t2.user_id and t1.worker_id = t2.worker_id and t1.created_at < t2.pay_time and substr(t1.created_at,1,7) = substr(t2.pay_time,1,7)
  left join dw.dim_crm_organization d0 on t1.workplace_id = d0.id
  left join dw.dim_crm_organization d1 on t1.department_id = d1.id
  left join dw.dim_crm_organization d2 on t1.regiment_id = d2.id
  left join dw.dim_crm_organization d4 on t1.team_id = d4.id
  group by 
  substr(t1.created_at,1,7)
  ,substr(t1.created_at,1,10)
  ,t1.user_type_name
  ,t1.clue_stage
  ,t1.clue_grade
  ,t1.clue_source_name
  ,t1.clue_source_name_level_1
  ,t1.worker_id
  ,t1.worker_name
  ,d0.workplace_name
  ,d1.department_name
  ,d2.regiment_name
  ,d4.team_name
)

select 
created_month
,user_type_name
,clue_stage
,clue_grade
,sum(recieve_cnt) `领取量`
,sum(paid_cnt) `整体转化量`
,sum(group_cnt) `组合品转化量`
,sum(xugou_cnt) `续购品转化量`
,sum(single_cnt) `单学段商品转化量`
,sum(multi_cnt) `多学段商品转化量`
,sum(duohai_xugou_cnt) `多孩转化量`
,sum(dahuiyuan_xugou_cnt) `大会员续购转化量`
,sum(gaozhongtunke_xugou_cnt) `高中囤课包转化量`
,sum(paid_amount) `整体转化营收`
,sum(group_amount) `组合品转化营收`
,sum(xugou_amount) `续购品转化营收`
,sum(single_amount) `单学段商品转化营收`
,sum(multi_amount) `多学段商品转化营收`
,sum(duohai_xugou_amount) `多孩转化营收`
,sum(dahuiyuan_xugou_amount) `大会员续购转化营收`
,sum(gaozhongtunke_xugou_amount) `高中囤课包转化营收`
from y0
group by 
created_month
,user_type_name
,clue_stage
,clue_grade