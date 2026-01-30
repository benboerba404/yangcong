DROP TABLE IF EXISTS tmp.niyiqiao_crm_clue_call_record;
CREATE TABLE  IF NOT EXISTS tmp.niyiqiao_crm_clue_call_record AS(
select 
  a.info_uuid --一个员工在某一时间点领取一条线索，对应一个Id
  ,a.user_id --用户Id
  ,c.user_type_name --用户类型
  ,a.clue_stage --用户学段
  ,d.clue_source_name --线索来源
  ,d.clue_source_name_level_1 --线索来源聚合
  ,c.clue_grade --用户年级
  ,c.city_class --用户城市线级
  ,c.province --用户省份
  ,c.city --用户省份
  ,c.business_user_pay_status_business 
  ,a.action_id --外呼id
  ,case when a.channel_id = 1   then '七陌'
        when a.channel_id = 2   then '天眼'
    	  when a.channel_id = 34  then '蘑谷云'
  		  when a.channel_id = 101 then '智鱼'
  		  when a.channel_id = 38  then '百川'
    	  when a.channel_id = 39  then '百悟'
        when a.channel_id = 102 then '天田'
      else '' end channel_id --外呼渠道
  ,case when length(if(call_phone is null,call_phone,if(call_phone rlike "^\\d+$",call_phone,cast(unbase64(call_phone) as string)))) = 11 
            then substr(if(call_phone is null,call_phone,if(call_phone rlike "^\\d+$",call_phone,cast(unbase64(call_phone) as string))),1,3)
        when length(if(call_phone is null,call_phone,if(call_phone rlike "^\\d+$",call_phone,cast(unbase64(call_phone) as string)))) = 0 then '无号码' 
      else '非11位手机号' end call_phone
  ,substr(a.created_at,1,19) call_created_at --外呼创建时间
  ,a.call_status
  ,a.deal_times
  ,a.call_time_length --外呼时长
  ,is_connect --电话是否接通
  ,is_valid_connect --电话是否有效接通
  ,a.worker_id
  ,case when f.name is null then agent_name else f.name end worker_name
  ,b0.department_name department_name
  ,case when b1.regiment_name is not null then b1.regiment_name   
      end regiment_name
  ,case when b2.heads_name is not null then b2.heads_name  
      end heads_name
  ,case when b3.team_name     is not null then b3.team_name
        when b3.team_name is null and b4.team_name is not null then b4.team_name
      end team_name
  ,case when substr(a.clue_created_at,1,10) = substr(a.created_at,1,10) then '是' else '否' end clue_created_type
  ,case when substr(a.clue_created_at,1,7) = substr(a.created_at,1,7) then '是' else '否' end clue_created_type_mon
from dw.fact_call_history a 
left join dw.dim_crm_organization as b0 on a.department_id = b0.id
left join dw.dim_crm_organization as b1 on a.regiment_id = b1.id
left join dw.dim_crm_organization as b2 on a.heads_id = b2.id
left join dw.dim_crm_organization as b3 on a.team_id = b3.id
left join (select distinct heads_name,team_name from dw.dim_crm_organization where team_name is not null) as b4 on b2.heads_name = b4.heads_name
left join crm.worker f on a.worker_id = f.id
left join (
  select 
    info_uuid,created_at clue_created_at,clue_source,clue_grade,city_class,province,city,business_user_pay_status_business
    ,case when user_type_name = '续费' then '续费' 
         when user_type_name = '老未' then '老未' 
         when user_type_name = '新增' and substr(created_at,1,7) = substr(regist_time,1,7)   then '新增-当月注册'
         when user_type_name = '新增' then '新增-非当月注册'
      end  user_type_name
  from aws.clue_info  
)c on a.info_uuid = c.info_uuid
left join  tmp.wuhan_clue_soure_name d on c.clue_source = d.clue_source
where substr(a.created_at,1,10) between '2023-01-01' and date_sub(current_date,1)
        and a.workplace_id in (4,400,702)
        and a.regiment_id not in  (0,303,546,234)
)