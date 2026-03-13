with t0 as (
select 
  external_user_id
  ,worker_id,worker_name,team_name,heads_name,regiment_name,department_name			
  ,created_at,channel_id,scene_name,type_name,resource_entrance_name,clue_level_name
from (
  select 
    a.external_user_id
    ,a.worker_id,c.name worker_name
    ,a.created_at,a.channel_id,b.scene_name,b.type_name,b.resource_entrance_name,b.clue_level_name
    ,d1.department_name,d2.regiment_name,d3.heads_name,d4.team_name,group_id0
    ,row_number()over(partition by a.external_user_id,a.worker_id,a.channel_id order by a.created_at ) rk
  from crm.contact_log a
  left join dw.dim_crm_organization d1 on a.group_id1 = d1.id
  left join dw.dim_crm_organization d2 on a.group_id2 = d2.id
  left join dw.dim_crm_organization d3 on a.group_id3 = d3.id
  left join dw.dim_crm_organization d4 on a.group_id4 = d4.id
  left join  crm.qr_code_change_history b 
    on a.channel_id = b.qr_code_id and substr(a.created_at,1,19) > effective_time and substr(a.created_at,1,19) < invalid_time
  left join crm.worker c on a.worker_id = c.id
  where   a.source=3--  —1是未知,0是海报,1是短信,3渠道活码 
      and a.change_type ='add_external_contact'
      and substr(a.created_at,1,10) between  '2022-01-01' and date_sub(current_date,1)
  )
where rk = 1 and group_id0 in (4,400,702)
)

, t1 as (
select * from (
select 
  t0.external_user_id
  ,t0.worker_id,t0.worker_name,t0.team_name,t0.heads_name,t0.regiment_name,t0.department_name			
  ,t0.created_at,t0.channel_id,t0.scene_name,t0.type_name,t0.resource_entrance_name,t0.clue_level_name
  ,b.we_com_open_id  ex_user_id
  ,b.user_id userid
  ,b.user_type_name usertype
  ,substr(b.created_at,1,19) recieve_time
  ,case when t0.created_at<=substr(b.created_at,1,19) then substr(b.created_at,1,19) else t0.created_at end calcu_time --
  ,row_number() over (partition by substr(t0.created_at,1,7),t0.external_user_id,t0.worker_id,t0.channel_id order by substr(b.created_at,1,19) ) as rk
from t0
left join aws.clue_info b 
  on t0.external_user_id = b.we_com_open_id and t0.worker_id = b.worker_id 
    and b.created_at > t0.created_at and substr(b.created_at,1,10) < date_add(t0.created_at,1)  -- 入库时间 > 添加时间 且 入库时间<添加时间+1天
  )
where rk = 1
)


,t2(
  SELECT 
    substr(pay_time,1,19) pay_time
    ,user_id as paid_userid
    ,worker_id 
    ,order_id 
    ,amount
  FROM aws.crm_order_info
  WHERE SUBSTR(pay_time,1,10) between '2022-01-17' and date_sub(current_date,1)
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
--     and ((substr(pay_time,1,10) between '2022-01-17'  and '2023-12-31') or (substr(pay_time,1,10) between '2024-01-01' and date_sub(current_date,1) and  worker_id <> 0 ))
  )


select distinct
  substr(created_at,1,10) created_at,t1.channel_id,t1.scene_name,t1.type_name,t1.resource_entrance_name,t1.clue_level_name
  ,t1.worker_id,t1.worker_name,t1.team_name,t1.heads_name,t1.regiment_name,t1.department_name
  ,count(distinct t1.external_user_id) add_cnt --企微添加量
  ,count(distinct t1.ex_user_id) laqu_cnt -- 拉取量
  
,count(distinct case when SUBSTR(t2.pay_time,1,7) = substr(t1.calcu_time,1,7) then t2.paid_userid end) paid_cnt_mm
,ifnull(sum(case when SUBSTR(t2.pay_time,1,7) = substr(t1.calcu_time,1,7) then ifnull(t2.amount,0) end),0) amount_mm  

,count(distinct case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10),30) then t2.paid_userid end) paid_cnt_30
,ifnull(sum(case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10) ,30) then ifnull(t2.amount,0) end),0) amount_30

,count(distinct case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10),14) then t2.paid_userid end) paid_cnt_14
,ifnull(sum(case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10) ,14) then ifnull(t2.amount,0) end),0) amount_14

,count(distinct case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10),7) then t2.paid_userid end) paid_cnt_7
,ifnull(sum(case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10) ,7) then ifnull(t2.amount,0) end),0) amount_7

,count(distinct case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10),3) then t2.paid_userid end) paid_cnt_3
,ifnull(sum(case when SUBSTR(t2.pay_time,1,10) <= date_add(substr(t1.calcu_time,1,10),3) then ifnull(t2.amount,0) end),0) amount_3

,count(distinct case when SUBSTR(t2.pay_time,1,10) = substr(t1.calcu_time,1,10) then t2.paid_userid end) paid_cnt_1--`领取线索当日转化量`
,ifnull(sum(case when SUBSTR(t2.pay_time,1,10) = substr(t1.calcu_time,1,10)  then ifnull(t2.amount,0) end),0) amount_1 --`领取线索当日转化金额`

from  t1 
left join t2 
  on t1.userid = t2.paid_userid 
  and t2.pay_time >= t1.calcu_time 
  and t1.worker_id = t2.worker_id
group by 
  substr(created_at,1,10)
  ,t1.channel_id
  ,t1.scene_name
  ,t1.type_name
  ,t1.resource_entrance_name
  ,t1.clue_level_name
  ,t1.worker_id
  ,t1.worker_name
  ,t1.team_name
  ,t1.heads_name
  ,t1.regiment_name
  ,t1.department_name
