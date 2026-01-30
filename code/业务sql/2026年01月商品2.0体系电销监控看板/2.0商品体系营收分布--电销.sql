drop table if exists tmp.tanchen_v2_aws_crm_order_amount;
create table if not exists tmp.tanchen_v2_aws_crm_order_amount(
pay_day string,
pay_month string,
worker_id string,
worker_name string,
team_id string,
team_name string,
heads_id string,
heads_name string,
regiment_id string,
regiment_name string,
department_id string,
department_name string,
workplace_id string,
workplace_name string,
is_pad string,
pad_name string,
sync_type string,
business_good_kind_name_level_1 string,
business_good_kind_name_level_2 string,
business_good_kind_name_level_3 string,
business_good_kind_name_level_1_modify string,
business_good_kind_name_level_2_modify string,
business_good_kind_name_level_3_modify string,
course_timing_kind string,
course_group_kind string,
mid_grade string,
mid_stage_name string,
business_user_pay_status_business string,
order_cnt string,
user_cnt string,
amount string
) partitioned by (dt int) 
row format delimited fields terminated by "\t";

--营收分布
with t0 as 
(
  select distinct
  substr(pay_time,1,19) pay_time
  ,substr(pay_time,1,10) pay_day
  ,to_date(substr(pay_time,1,7)) pay_month
  ,a.worker_id
  ,worker_name
  ,a.team_id
  ,f.team_name
  ,a.heads_id
  ,e.heads_name
  ,a.regiment_id
  ,d.regiment_name
  ,a.department_id
  ,c.department_name
  ,a.workplace_id
  ,b.workplace_name
  ,case when is_pad = 1 then '平板订单' when is_pad = 0 then '非平板订单' end as is_pad
  ,case when good_id in (
        '62bd6d81b3527f37257e2a71','62bf014940ae6a896f0f293c','635b4f09c817b42e87e5baa6','63a97d7bcc3fc86cd2834d3b','63b67769d8828b81990f26ac','63a97c8043bdb153adfdd60a','63a955b6f4a6f859c3410c9c','6447ad64764d6028e8889a8b','64671cb3c5da9b231733bbd7'
         ,'647851e613fbaa652d59b438','64868c426eda4e724db7f65f','6478511c2e663769e8ac93ec','6497fce57602d515856cab4a','64a10fb8bd9434d0e414677c') then '998体验机'
         when good_id in ('647864af54506ada5a883d53','64671e85933d70362325f8ee','64644a5fc3d8387d4edd01da','64a1112e472a98fafeb764a5') then '1598体验机'
         when good_name REGEXP '押金' and amount in (798,998) then '998体验机'
         when good_name REGEXP '押金' and amount = 1598 then '1598体验机'
         when pad_name REGEXP 'Q10'  then '平板Q10'
         when pad_name REGEXP 'Q20'  then '平板Q20'
         when pad_name REGEXP 'S20'  then '平板S20'
         when pad_name REGEXP 'S30'  then '平板S30'
         when pad_name REGEXP 'P30'  then '平板P30'
         when pad_name REGEXP '实体-Pad-学习平板' then '平板C3'
         else '非平板订单' end pad_name 
  ,case when sync_type = '1' then '自动判单'
        when sync_type = '2' then '申诉'
        when sync_type = '3' then '七陌导入'
        when sync_type = '4' then '专属链接'
        when sync_type = '5' then '端内推送'
        when sync_type = '6' then 'app内购买'
      end  sync_type 
      
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  
  ,case when business_good_kind_name_level_1 = '积木块' then '零售商品' else business_good_kind_name_level_1 end business_good_kind_name_level_1_modify
  
  ,case when course_group_kind = '公域主推品' then '公域主推品' 
        when string(strategy_type) regexp '多孩策略|高中囤课策略|学习机加购策略|历史大会员续购策略' 
        then '续购'
        else business_good_kind_name_level_2 end business_good_kind_name_level_2_modify
        
  ,case when string(strategy_type) regexp '多孩策略' then '多孩策略'
      when string(strategy_type) regexp '高中囤课策略' then '高中屯课策略'
      when string(strategy_type) regexp '历史大会员续购策略' then '历史大会员续购'
      when business_good_kind_name_level_3 = '学习机加购' or string(strategy_type) regexp '学习机加购策略' then '学习机加购策略'
      when business_good_kind_name_level_2 = '其他' then '其他'
      else business_good_kind_name_level_3 end as business_good_kind_name_level_3_modify
  ,course_timing_kind --时长型、到期型
  ,course_group_kind --公域主推品、私域主推品
  ,a.mid_grade
  ,a.mid_stage_name
  ,a.business_user_pay_status_business
  ,a.order_id 
  ,a.user_id
  ,a.amount
  from aws.crm_order_info a
  left join dw.dim_crm_organization as b on a.workplace_id = b.id
  left join dw.dim_crm_organization as c on a.department_id = c.id
  left join dw.dim_crm_organization as d on a.regiment_id = d.id
  left join dw.dim_crm_organization as e on a.heads_id = e.id
  left join dw.dim_crm_organization as f on a.team_id = f.id
  where 
  substr(pay_time,1,10) between '2021-01-01' and  date_sub(current_date,1)
  and a.workplace_id in (4,400,702) --职场归属武汉电销和长沙电销
  and a.regiment_id  not in (303,0,546) --剔除体验营、私域阿拉丁、无团队归属
  and a.worker_id <> 0
  and a.is_test = false
  and a.in_salary =1 
  and a.status = '支付成功'
)

insert overwrite table tmp.tanchen_v2_aws_crm_order_amount partition(dt)
select 
pay_day
,pay_month
,worker_id
,worker_name
,team_id
,team_name
,heads_id
,heads_name
,regiment_id
,regiment_name
,department_id
,department_name
,workplace_id
,workplace_name
,is_pad
,pad_name
,sync_type
,business_good_kind_name_level_1
,business_good_kind_name_level_2
,business_good_kind_name_level_3
,business_good_kind_name_level_1_modify
,business_good_kind_name_level_2_modify
,business_good_kind_name_level_3_modify
,course_timing_kind
,course_group_kind
,mid_grade
,mid_stage_name
,business_user_pay_status_business
,count(distinct order_id) order_cnt
,count(distinct user_id) user_cnt
,sum(amount) amount
,date_format(to_date(pay_time),'yyyyMMdd') dt
from t0
group by 
pay_day
,pay_month
,worker_id
,worker_name
,team_id
,team_name
,heads_id
,heads_name
,regiment_id
,regiment_name
,department_id
,department_name
,workplace_id
,workplace_name
,is_pad
,pad_name
,sync_type
,business_good_kind_name_level_1
,business_good_kind_name_level_2
,business_good_kind_name_level_3
,business_good_kind_name_level_1_modify
,business_good_kind_name_level_2_modify
,business_good_kind_name_level_3_modify
,course_timing_kind
,course_group_kind
,mid_grade
,mid_stage_name
,business_user_pay_status_business
,date_format(to_date(pay_time),'yyyyMMdd')



