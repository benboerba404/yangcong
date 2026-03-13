with t0 as (
select 
  external_user_id,yc_user_id,worker_id,channel_id,is_repeated_exposure,add_created_at
from (
  select distinct
  external_user_id,yc_user_id,worker_id,channel_id
  ,case when substr(created_at,1,10) < '2025-07-26' then '历史无法区分'
        when substr(created_at,1,10) >= '2025-07-26' and is_repeated_exposure = true then '是'
        when substr(created_at,1,10) >= '2025-07-26' and is_repeated_exposure = false then '否'
      end is_repeated_exposure
  ,created_at add_created_at
  ,row_number()over(partition by  external_user_id,worker_id,channel_id,yc_user_id order by created_at ) rn
  from crm.contact_log 
  where source=3 
    and change_type='add_external_contact'
    and SUBSTR(created_at,1,10) between '2022-01-17' and date_sub(current_date,1)
    and length(yc_user_id) = 24
    and yc_user_id <> '000000000000000000000001'
)
where rn = 1
)

,t1 as (
select * from (
  select distinct
    t0.add_created_at,t0.external_user_id,t0.worker_id,t0.is_repeated_exposure,t0.channel_id
    -- ,c.channel_name,c.type,c.level_1,c.level_2
    ,c.scene_name channel_name,c.clue_level_name level_2,c.resource_entrance_name level_1,c.type_name type
    -- ,b.we_com_open_id  ex_user_id
    ,b.ex_user_id
    ,b.user_id userid
    ,b.user_type_name usertype
    ,SUBSTR(b.created_at,1,19) recieve_time
    ,substr(b.clue_expire_time,1,19) user_expire
    ,row_number() over (partition BY t0.add_created_at,t0.external_user_id,t0.worker_id,t0.channel_id ORDER BY SUBSTR(b.created_at,1,19) ) rk
  from t0 
  left join aws.clue_info b --we_com_open_id
    on t0.external_user_id = b.we_com_open_id and t0.worker_id = b.worker_id 
        and b.created_at > t0.add_created_at 
        and channel_id = qr_code_channel_id 
        and yc_user_id = user_id
        and substr(b.created_at,1,10) < date_add(t0.add_created_at,1)
          -- 入库时间 > 添加时间 且 入库时间<添加时间+1天
  -- left join tmp.wuhan_wecom_channel_id c on t0.channel_id = c.id
  left join crm.qr_code_change_history  c 
    on t0.channel_id = c.qr_code_id and SUBSTR(add_created_at,1,19) >= effective_time and  SUBSTR(add_created_at,1,19) < invalid_time
  )
where rk = 1
)

, t2 as (
SELECT 
  substr(pay_time,1,19) pay_time,
  user_id as paid_userid,
  worker_id workerid,
  order_id orderid,
  amount
FROM aws.crm_order_info
WHERE SUBSTR(pay_time,1,10) between '2022-01-17' and date_sub(current_date,1)
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
    and worker_id <> 0
    and in_salary = 1
    and is_test = false
--  and ((substr(pay_time,1,10) between '2022-01-17'  and '2023-12-31') or (substr(pay_time,1,10) between '2024-01-01' and date_sub(current_date,1) and worker_id <> 0 ))
)


select 
  SUBSTR(t1.add_created_at,1,10) created_at
  ,t1.is_repeated_exposure
  ,t1.channel_id
  ,t1.channel_name
  ,t1.type
  ,t1.level_1
  ,t1.level_2
  ,count(distinct t1.external_user_id) add_cnt --企微添加量
  ,count(distinct t1.ex_user_id) laqu_cnt -- 拉取量
  ,count(distinct t2.paid_userid) paid_cnt --累计转化量
  ,sum(ifnull(amount,0)) amount--累计转化金额
from t1
left join t2 
  on t1.userid = t2.paid_userid and t2.pay_time > t1.recieve_time
group by   
  SUBSTR(t1.add_created_at,1,10) 
  ,t1.is_repeated_exposure
  ,t1.channel_id,t1.channel_name
  ,t1.type,t1.level_1,t1.level_2
