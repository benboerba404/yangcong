with t1 as (
  select 
    a.user_id
    ,substr(a.created_at,1,19) created_at
    ,substr(a.created_at,1,7) created_ym
    ,case when user_type_name = '续费' then '续费' 
          when user_type_name = '老未' then '老未' 
          when user_type_name = '新增' and substr(created_at,1,7) = substr(regist_time,1,7)  then '新增-当月注册'
          when user_type_name = '新增' then '新增-非当月注册'
      end user_type_name
    ,clue_stage
    ,clue_grade
    ,b.clue_source_name
    ,b.clue_source_name_level_1
    ,business_user_pay_status_business
    ,case when substr(worker_join_at,1,10) = TRUNC(worker_join_at, 'month') and substr(worker_join_at,1,10) < add_months(substr(a.created_at,1,7),-2) then '老人' 
          when substr(worker_join_at,1,10) > TRUNC(worker_join_at, 'month') and substr(worker_join_at,1,10) < add_months(substr(a.created_at,1,7),-3) then '老人' 
     else '新人' end worker
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
    substr(a.created_at,1,10) between date_sub(current_date,35) and date_sub(current_date,1)
    and user_sk > 0
    and worker_id <> 0
    and a.workplace_id in (4,400,702)
    and a.regiment_id not in (0,303,546)
)

, t2 as (
select 
  substr(pay_time,1,19) pay_time 
  ,substr(pay_time,1,7) pay_ym 
  ,worker_id
  ,order_id
  ,case when string(strategy_type) regexp '多孩策略|历史大会员续购策略' then '续购'
        when course_group_kind = '公域主推品' then '公域品' 
      else business_good_kind_name_level_1 end business_good_kind_name_level_1_modify
  ,case when string(strategy_type) regexp '多孩策略' then '多孩策略'  
        when string(strategy_type) regexp '历史大会员续购策略' then '历史大会员续购策略'  
        when course_group_kind = '公域主推品' then '公域主推品' 
      else business_good_kind_name_level_2 end business_good_kind_name_level_2_modify
  ,user_id
  ,amount
from aws.crm_order_info a
where
  substr(pay_time,1,10)  between date_sub(current_date,35) and date_sub(current_date,1)
  and workplace_id in (4,400,702)
  and regiment_id not in (0,303,546)
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
)

, ty1 as (
select   
  substr(t1.created_at,1,10) created_at
  ,t1.user_type_name
  ,t1.business_user_pay_status_business
  ,t1.clue_stage
  ,t1.clue_grade
  ,t1.clue_source_name
  ,t1.clue_source_name_level_1
  ,t1.worker
  ,t1.worker_id
  ,t1.worker_name
  ,d0.workplace_name
  ,d1.department_name
  ,d2.regiment_name
  ,d4.team_name
  ,count(distinct t1.user_id) recieve_cnt
  --整体转化量
  ,count(distinct case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) then t2.user_id end)  paid_cnt--`领取线索当日转化量`
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3) then t2.user_id end)  paid_cnt_3d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7) then t2.user_id end)  paid_cnt_7d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) then t2.user_id end) paid_cnt_14d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) then t2.user_id end) paid_cnt_21d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) then t2.user_id end) paid_cnt_30d
  ,count(distinct case when created_ym = pay_ym then t2.user_id end) paid_cnt_month
  --整体转化营收
  ,sum(case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) then amount else 0 end)  paid_amount--`领取线索当日转化金额`
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3) then amount else 0 end)  paid_amount_3d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7) then amount else 0 end)  paid_amount_7d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) then amount else 0 end) paid_amount_14d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) then amount else 0 end) paid_amount_21d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) then amount else 0 end) paid_amount_30d
  ,sum(case when created_ym = pay_ym then amount else 0 end) paid_amount_month
  

  --单学段商品（不含策略商品+公域主推品）转化量 
  ,count(distinct case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt_3d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt_7d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt_14d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt_21d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt_30d
  ,count(distinct case when created_ym = pay_ym and business_good_kind_name_level_2_modify = '单学段商品' then t2.user_id end) single_cnt_month
  --单学段商品（不含策略商品+公域主推品）转化营收
  ,sum(case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10)  and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount_3d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount_7d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount_14d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount_21d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount_30d
  ,sum(case when created_ym = pay_ym and business_good_kind_name_level_2_modify = '单学段商品' then amount else 0 end) single_amount_month 


   --多学段商品（不含策略商品+公域主推品）转化量
  ,count(distinct case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10)  and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt_3d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt_7d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt_14d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt_21d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt_30d
  ,count(distinct case when created_ym = pay_ym and business_good_kind_name_level_2_modify = '多学段商品' then t2.user_id end) multi_cnt_month
  --多学段商品（不含策略商品+公域主推品）转化营收
  ,sum(case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10)  and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount_3d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount_7d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount_14d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount_21d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount_30d
  ,sum(case when created_ym = pay_ym and business_good_kind_name_level_2_modify = '多学段商品' then amount else 0 end) multi_amount_month 

   --续购品 + 策略品转化量
  ,count(distinct case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt_3d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt_7d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt_14d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt_21d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt_30d
  ,count(distinct case when created_ym = pay_ym and business_good_kind_name_level_1_modify = '续购' then t2.user_id end) xugou_cnt_month
  --续购品 + 策略品转化营收
  ,sum(case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10)  and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount_3d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount_7d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount_14d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount_21d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount_30d
  ,sum(case when created_ym = pay_ym and business_good_kind_name_level_1_modify = '续购' then amount else 0 end) xugou_amount_month 
  
  
   --组合品（不含策略商品+公域主推品）转化量
  ,count(distinct case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt_3d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt_7d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt_14d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt_21d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt_30d
  ,count(distinct case when created_ym = pay_ym and business_good_kind_name_level_1_modify = '组合品' then t2.user_id end) group_cnt_month
  --组合品（不含策略商品+公域主推品）转化营收
  ,sum(case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount_3d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount_7d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount_14d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount_21d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount_30d
  ,sum(case when created_ym = pay_ym and business_good_kind_name_level_1_modify = '组合品' then amount else 0 end) group_amount_month 
  
  
   --公域品（不含策略商品）转化量
  ,count(distinct case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt_3d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt_7d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt_14d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt_21d
  ,count(distinct case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt_30d
  ,count(distinct case when created_ym = pay_ym and business_good_kind_name_level_1_modify = '公域品' then t2.user_id end) gongyu_cnt_month
  --公域品（不含策略商品）转化营收
  ,sum(case when SUBSTR(pay_time,1,10)=SUBSTR(t1.created_at,1,10) and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),3)  and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount_3d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),7)  and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount_7d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),14) and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount_14d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),21) and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount_21d
  ,sum(case when SUBSTR(pay_time,1,10)<=date_add(SUBSTR(t1.created_at,1,10),30) and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount_30d
  ,sum(case when created_ym = pay_ym and business_good_kind_name_level_1_modify = '公域品' then amount else 0 end) gongyu_amount_month 
  

  ,cast(regexp_replace(substr(t1.created_at,1,10), '-', '') as int) as dt
from t1
left join t2 on t1.user_id = t2.user_id and t1.worker_id = t2.worker_id and t1.created_at < t2.pay_time
left join dw.dim_crm_organization d0 on t1.workplace_id = d0.id
left join dw.dim_crm_organization d1 on t1.department_id = d1.id
left join dw.dim_crm_organization d2 on t1.regiment_id = d2.id
left join dw.dim_crm_organization d4 on t1.team_id = d4.id
group by  
  substr(t1.created_at,1,10),
  t1.user_type_name,
  t1.business_user_pay_status_business,
  t1.clue_stage,
  t1.clue_grade,
  t1.clue_source_name,
  t1.clue_source_name_level_1,
  t1.worker,
  t1.worker_id,
  t1.worker_name,
  d0.workplace_name,
  d1.department_name,
  d2.regiment_name,
  d4.team_name
)



insert overwrite table  tmp.niyiqiao_clue_recieve_paid_level2_day
partition(dt)  
select * from ty1;