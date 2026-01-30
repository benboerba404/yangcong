insert overwrite table tmp.tanchen_dianxiao_predetermined_allocate_leads partition(dt)
select
date_add(last_day(add_months(substr(t1.created_at,1,10),-1)),1) created_month
,case when note regexp '月定向分配公海池数据' then '定向分配公海池数据'
      when note regexp '公海未学习活跃付费用户|定向分配付费|2024珠峰小学付费数据|2024珠峰付费数据|2025冲顶付费数据' then '定向分配付费数据'
      when note regexp '小低年级定向分配优质数据' then '小低年级定向分配优质数据'
      when note regexp '2024珠峰端内定金数据定向分配' then '2024珠峰端内定金数据定向分配'
      when note regexp '2025冲顶端内蓄水数据定向分配' then '2025冲顶端内蓄水数据定向分配'
      when note regexp '2025冲顶端内定金数据定向分配' then '2025冲顶端内定金数据定向分配'
      when note regexp '定向分配高优号段数据' then '定向分配高优号段数据'
      when note regexp '测试' then '测试转化'
      else '其他' end note
,t4.name group_name
,t3.name team_name
,t1.worker_name
,count(distinct t1.user_id) predetermined_cnt
,count(distinct case when t2.order_id is not null then t1.user_id end) paid_cnt
,sum(amount) paid_amount
,cast(regexp_replace(substr(t1.created_at,1,7), '-', '') as int) dt
from
(
  select note,substr(created_at,1,19) created_at,info_uuid,user_id,team_id,regiment_id,worker_name
  from aws.clue_info
  where substr(created_at,1,10) between '2024-01-01' and date_sub(current_date,1)
  and clue_source='mid_school_manual'
  and 
  (
  note regexp '定向分配公海池数据|定向分配付费|公海未学习活跃付费用户|小低年级定向分配优质数据|宝藏悬浮球场景用户转化测试|定向分配高优号段数据'
  or note regexp '社区场景用户转化测试|2024珠峰小学付费数据|2024珠峰付费数据|2024珠峰端内定金数据定向分配|2025冲顶'
  )
) t1
left join 
(
  select 
  user_id
  ,order_id
  ,substr(pay_time,1,19) pay_time
  ,amount
  ,good_name
  from 
  aws.crm_order_info
  where substr(pay_time,1,10)>='2024-01-01' and substr(pay_time,1,10)<=date_sub(current_date,1)
  and workplace_id in (4,400,702) --职场归属武汉电销和长沙电销
  and regiment_id  not in (303,0,546) --剔除体验营、私域阿拉丁、无团队归属
  and worker_id <> 0
  and is_test = false
  and in_salary =1 
) t2
on t1.user_id=t2.user_id and pay_time>=created_at and substr(pay_time,1,7) = substr(created_at,1,7)
left join 
crm.organization t3
on t1.team_id=t3.id
left join 
crm.organization t4
on t1.regiment_id=t4.id
group by created_month,note,group_name,team_name,worker_name,dt
