with y1 as 
(
  select distinct substr(paid_time,1,19) paid_time,ab_name,good_type,u_user 
  from tmp.lidanping_quanyu_198test_2_goodtype
  where channel = 'C端'
  and good_type ='线索品198'
  -- and good_type in ('3个月同步课198','线索品198','12个月同步课498')
  and substr(paid_time,1,10) between '2025-12-18' and '2026-01-11'
  -- and u_user = '65ebec5dd65732000194f8e6'
) --同一天一个商品多次购买算一次

,y2 as 
(
  select paid_time,ab_name,good_type,u_user,phone,clue_source,created_at,clue_expire_time
  ,case when created_at is null then '未触达'
        when created_at <= paid_time then '已在库'
        when clue_source = 'WeCom' then '企微触达'
        else '其他触达' end contact_way
  ,case when created_at is null then ''
        when created_at <= paid_time then paid_time
        else created_at end calculate_time
  from 
  (
    select 
    y1.paid_time
    ,y1.ab_name
    ,y1.good_type
    ,y1.u_user
    ,a.phone
    ,y2.clue_source
    ,substr(y2.created_at,1,19) created_at
    ,substr(y2.clue_expire_time,1,19) clue_expire_time
    ,row_number() over (partition by y1.u_user,y1.good_type,substr(y1.paid_time,1,10) order by y2.created_at) as aa --多次触达取购买后首次触达
    from y1
    left join
    (
        select info_uuid,user_id,created_at,clue_expire_time,clue_source
        from aws.clue_info
        where substr(clue_expire_time,1,10)>='2025-11-01'
    ) y2 --电销触达信息
    on u_user = user_id and substr(clue_expire_time,1,19) > paid_time and trunc(substr(y1.paid_time,1,10),'MM') >= trunc(substr(created_at,1,10),'MM') --触达只算本月触达
    left join 
    (
      select  
      u_user,if(phone is null,phone,if(phone rlike "^\\d+$",phone,cast(unbase64(phone) as string))) AS phone
      from dw.dim_user
      where length(phone)>0
    ) a 
    on y1.u_user = a.u_user
  )
  where aa = 1
)

,y3 as --转化订单表
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
  ,good_name
  from aws.crm_order_info a
  where
  substr(pay_time,1,10)  between '2025-11-01' and date_sub(current_date,1)
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
  and status = '支付成功'
)

--累计触达率转化率
select substr(paid_time,1,10) paid_time,ab_name,good_type,contact_way
,count(distinct y2.u_user) sale_cnt
,count(distinct case when business_good_kind_name_level_1 ='组合品' then y3.user_id end) group_cnt
,count(distinct y3.user_id) paid_cnt
,sum(case when business_good_kind_name_level_1 ='组合品' then amount end) group_amount
,sum(amount) paid_amount
from y2 
left join y3
on y2.u_user = y3.user_id and calculate_time < pay_time and substr(calculate_time,1,7) = substr(pay_time,1,7)
group by substr(paid_time,1,10),ab_name,good_type,contact_way
order by paid_time,ab_name,good_type,contact_way

--累计触达转化明细
select substr(paid_time,1,19) paid_time,ab_name,good_type,contact_way,y2.phone
,y2.u_user
,y3.pay_time
,y3.good_name
,y3.amount
from y2 
left join y3
on y2.u_user = y3.user_id and calculate_time < pay_time -- and substr(calculate_time,1,7) = substr(pay_time,1,7)
where y3.pay_time is not null


--未触达明细
select paid_time,ab_name,good_type,u_user,phone
from y2
where contact_way = '未触达'


--成单周期,转化剔除退款，只看组合品的成单周期
select substr(paid_time,1,10) paid_time,ab_name,good_type,contact_way
,y2.u_user
,y2.created_at
,y3.pay_time
,case when contact_way = '已在库' then datediff(substr(y3.pay_time,1,10),substr(paid_time,1,10))
      else datediff(substr(y3.pay_time,1,10),substr(y2.created_at,1,10)) end interval_day
from y2 
left join y3
on y2.u_user = y3.user_id and calculate_time < pay_time -- and substr(calculate_time,1,7) = substr(pay_time,1,7)
where y3.pay_time is not null and contact_way <> '未触达' and business_good_kind_name_level_1 ='组合品'

