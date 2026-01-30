with t1 as (
select  
  a.user_id
  ,a.created_at
  ,a.ym
  ,a.user_type_name 
  ,a.business_user_pay_status_business
  ,a.clue_stage
  ,a.clue_grade
  ,b.clue_source_name
  ,b.clue_source_name_level_1
  ,c.clue_source_name first_clue_source_name
  ,c.clue_source_name_level_1 first_clue_source_name_level_1
from (
  select distinct
    user_id
    ,substr(created_at,1,19) created_at
    ,substr(created_at,1,7) ym
    ,case when user_type_name = '续费' then '续费' 
          when user_type_name = '老未' then '老未' 
          when user_type_name = '新增' and substr(created_at,1,7) = substr(regist_time,1,7)  then '新增-当月注册'
          when user_type_name = '新增' then '新增-非当月注册'
      end user_type_name
    ,clue_stage
    ,clue_grade
    ,clue_source
    ,first_clue_source
    ,business_user_pay_status_business
    ,row_number() over(partition by user_id,substr(created_at,1,7)  order by  created_at ) rk 
  from aws.clue_info --线索消耗表
  where (substr(created_at,1,10) between '${start_date1}' and '${end_date1}'
  or substr(created_at,1,10) between '${start_date2}' and '${end_date2}')
    and user_sk > 0
    and worker_id <> 0
    and workplace_id in (4,400,702)
    and regiment_id not in (0,303,546)
  ) a
left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
left join tmp.wuhan_clue_soure_name c on a.first_clue_source = c.clue_source
where rk = 1 --取线索消耗表中统计周期内首次被消耗的记录
)

, t2 as (
  select distinct
  substr(pay_time,1,7) pay_ym 
  ,substr(pay_time,1,19) pay_time 
  ,order_id
  ,case when business_good_kind_name_level_1='组合品' and string(strategy_type) regexp '多孩策略' then '多孩策略'
        when business_good_kind_name_level_1='组合品' and string(strategy_type) regexp '历史大会员续购策略' then '历史大会员续购'
        when business_good_kind_name_level_1='续购' and business_good_kind_name_level_3 = '学习机加购' then '学习机加购策略'
        when business_good_kind_name_level_1='续购' and business_good_kind_name_level_3 regexp '学段加购|培优课加购' then '学段加购'
        when business_good_kind_name_level_1='续购' and business_good_kind_name_level_3='普通续购' then '普通续购'
        when business_good_kind_name_level_1 = '组合品' then '组合品'
        else '其他' end business_good_kind_name_level_3
  ,user_id
  ,amount
  from aws.crm_order_info 
  where (substr(pay_time,1,10) between '${start_date1}' and '${end_date1}'
  or substr(pay_time,1,10) between '${start_date2}' and '${end_date2}')
  and workplace_id in (4,400,702)
  and regiment_id not in (0,303,546)
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
) --电销营收

select   
t1.ym
,t1.clue_stage
,t1.clue_grade
,count(distinct t1.user_id) recieve_cnt --线索领取量/消耗量
-- ,count(distinct t2.user_id ) paid_cnt --整体转化量
-- ,sum( amount ) amount --整体转化金额
,count(distinct case when business_good_kind_name_level_3 = '组合品' then t2.user_id end ) group_cnt --组合品不含策略商品
,sum(case when business_good_kind_name_level_3 = '组合品' then amount else 0 end ) group_amount --组合品不含策略商品

from t1
left join t2  on t1.user_id = t2.user_id and t1.ym = t2.pay_ym and t1.created_at < t2.pay_time
group by 1,2,3