DROP TABLE IF EXISTS tmp.wuhan_crm_channel_add_del_log;
CREATE TABLE  IF NOT EXISTS tmp.wuhan_crm_channel_add_del_log AS(
with t1 as (
select distinct
  external_user_id
  ,a.yc_user_id
  ,channel_id
  ,substr(a.created_at,1,19) add_times
  ,a.worker_id
  ,b.name worker_name
  ,d3.heads_name
  ,d2.regiment_name
from crm.contact_log a
left join crm.worker b on a.worker_id = b.id
left join dw.dim_crm_organization d2 on a.group_id2 = d2.id
left join dw.dim_crm_organization d3 on a.group_id3 = d3.id
where source = 3
and substr(a.created_at,1,10)  between '2023-01-01' and date_sub(current_date,1) 
and change_type  = 'add_external_contact' 
and length(yc_user_id) = 24
and  yc_user_id <> '000000000000000000000001'
)

,t2 as(
select 
  external_user_id
  ,channel_id
  ,substr(created_at,1,19)   del_times
  ,worker_id
  ,yc_user_id
from crm.contact_log 
where source=3
and substr(created_at,1,10)  between '2023-01-01' and date_sub(current_date,1) 
and change_type  = 'del_follow_user'
and length(yc_user_id) = 24
and yc_user_id <> '000000000000000000000001'
)

, t3 as (
select * from 
(
  select 
    t1.external_user_id
    ,t1.yc_user_id 
    ,t1.channel_id
    ,t1.worker_id
    ,t1.worker_name
    ,t1.heads_name
    ,t1.regiment_name
    ,t1.add_times
    ,t2.yc_user_id del_user_id
    ,t2.del_times
    ,row_number() over (partition by t1.external_user_id, t1.yc_user_id, t1.worker_id, t1.channel_id, t1.add_times order by t2.del_times) as rn
  from t1 
  left join t2 
    on t1.external_user_id = t2.external_user_id 
    and t1.yc_user_id = t2.yc_user_id
    and t1.worker_id = t2.worker_id 
    and t1.channel_id = t2.channel_id
    and t2.del_times >= t1.add_times 
  )
  where rn = 1
)


select
  substr(add_times,1,10) add_times
  ,channel_id
  ,c.channel_name
  ,c.type
  ,c.level_1
  ,c.level_2
  ,regiment_name
  ,heads_name
  ,worker_name

  ,count(distinct yc_user_id ) add_user_cnt
  ,count(distinct case when del_user_id is not null then yc_user_id  end ) del_user_cnt
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 0     and 300    then yc_user_id end ) del_user_cnt_0_5m
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 301   and 600    then yc_user_id end ) del_user_cnt_5_10m
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 601   and 900    then yc_user_id end ) del_user_cnt_10_15m
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 901   and 1800   then yc_user_id end ) del_user_cnt_15_30m
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 1801  and 2700   then yc_user_id end ) del_user_cnt_30_45m
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 2701  and 3600   then yc_user_id end ) del_user_cnt_45_60m
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 3601  and 43200  then yc_user_id end ) del_user_cnt_1_12h
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 43201 and 86400  then yc_user_id end ) del_user_cnt_12_24h
  ,count(distinct case when del_user_id is not null and (unix_timestamp(del_times) - unix_timestamp(add_times)) between 0     and 86400  then yc_user_id end ) del_user_cnt_0_24h

  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),1)  then yc_user_id  end ) del_user_cnt_1d
  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),2)  then yc_user_id  end ) del_user_cnt_2d
  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),3)  then yc_user_id  end ) del_user_cnt_3d
  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),7)  then yc_user_id  end ) del_user_cnt_7d
  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),14) then yc_user_id  end ) del_user_cnt_14d
  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),21) then yc_user_id  end ) del_user_cnt_21d
  ,count(distinct case when del_user_id is not null and substr(del_times,1,10) <= date_add(substr(add_times,1,10),30) then yc_user_id  end ) del_user_cnt_30d
  ,count(distinct case when del_user_id is not null and substr(add_times,1,10) = substr(del_times,1,10) then yc_user_id  end ) del_user_cnt_day
  ,count(distinct case when del_user_id is not null and substr(add_times,1,7) = substr(del_times,1,7) then yc_user_id  end ) del_user_cnt_month

from t3
left join tmp.wuhan_wecom_channel_id  c on t3.channel_id = c.id
group by 
  substr(add_times,1,10) 
  ,channel_id
  ,c.channel_name
  ,c.type
  ,c.level_1
  ,c.level_2
  ,regiment_name
  ,heads_name
  ,worker_name