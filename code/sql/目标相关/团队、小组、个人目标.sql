# 组业绩目标
sql1 = '''
DROP TABLE IF EXISTS tmp.wuhan_crm_heads_goal_mm
'''
sql01 = '''
CREATE TABLE  IF NOT EXISTS tmp.wuhan_crm_heads_goal_mm AS(
with t1  as (
select * from (
select 
  date(concat_ws('-',u_year,u_month)) ym
  ,u_year
  ,u_month
  ,org_id
  ,name
  ,goal
  ,cast(regexp_replace(substr(updated_at,1,10), '-', '') as int) updated_at
  ,row_number()over(partition by u_year,u_month,org_id order by updated_at desc ) rk
from crm.group_goal
where u_year > 2022 and level = 4
and path REGEXP '^,[^,]+,(4|400),[^,]+,(?!303|546|233|234)[^,]+,'
and name not regexp '离职|体验营'
)
where rk = 1 
)


select distinct
  t1.ym
  ,t1.u_year
  ,t1.u_month
  ,t1.org_id heads_id
  ,t1.name heads_name
  ,t1.goal heads_goal
  ,t2.regiment_id
  ,t2.regiment_name
  ,t2.department_name
from t1
left join dw.dim_crm_organization t2 on t2.id = t1.org_id 
where goal  > 0 

)
'''

#  团业绩目标
sql2 = '''
DROP TABLE IF EXISTS tmp.wuhan_crm_regiment_goal_mm
'''
sql02 = '''
CREATE TABLE  IF NOT EXISTS tmp.wuhan_crm_regiment_goal_mm AS(
with t1  as (
select * from (
select 
  date(concat_ws('-',u_year,u_month)) ym
  ,u_year
  ,u_month
  ,org_id
  ,name
  ,goal
  ,cast(regexp_replace(substr(updated_at,1,10), '-', '') as int) updated_at
  ,row_number()over(partition by u_year,u_month,org_id order by updated_at desc ) rk
from crm.group_goal
where u_year > 2022 and level  = 3 
and path REGEXP '^,[^,]+,(4|400)'
and org_id not in (303,546,233,234)
)
where rk = 1  
)

select distinct
  t1.ym
  ,t1.u_year
  ,t1.u_month
  ,t1.org_id regiment_id
  ,t1.name regiment_name
  ,t2.department_id
  ,t2.department_name
  ,t1.goal regiment_goal
from t1
left join dw.dim_crm_organization t2 on t2.id = t1.org_id 
where goal > 0
)

'''


# 员工业绩目标
sql3 = '''
DROP TABLE IF EXISTS tmp.wuhan_crm_worker_goal_mm
'''
sql03 = '''
CREATE TABLE  IF NOT EXISTS tmp.wuhan_crm_worker_goal_mm AS (
select 
  ym,u_year,u_month,worker_id,worker_name,worker_goal
from(
  select distinct
    date(concat_ws('-',u_year,u_month)) ym
    ,u_year
    ,u_month
    ,a.worker_id
    ,name worker_name
    ,b.mail
    ,goal worker_goal
    ,a.org_id
    ,cast(regexp_replace(substr(a.updated_at,1,10), '-', '') as int) updated_at
    ,row_number()over(partition by u_year,u_month,a.worker_id order by a.goal desc,a.updated_at desc ) rk
  from crm.worker_goal a
  left join crm.worker b on  a.worker_id = b.id
  where u_year > 2022
  and name is not null
  and goal > 0 
  ) 
where rk =1
)
'''