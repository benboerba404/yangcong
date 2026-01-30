select a.month
,count(distinct active_u_user) `月活量`
,count(distinct case when yc_user_id is not null then active_u_user end) `企微添加量`
from 
(
  select distinct
  substr(from_unixtime(unix_timestamp(cast(day as string),'yyyyMMdd'),'yyyy-MM-dd'),1,7) month
  ,a.active_u_user
  from aws.crm_active_data_pool_day a
  where (day between 20251001 and 20251020 or day between 20260101 and 20260120)
  and active_user_attribution in ('中学用户','小学用户','c')
) a 
left join 
(
  select distinct
  substr(created_at,1,7) month
  ,yc_user_id
  from crm.contact_log 
  where source = 3 
  and change_type='add_external_contact'
  and (substr(created_at,1,10) between '2025-10-01' and '2025-10-20'
  or substr(created_at,1,10) between '2026-01-01' and '2026-01-20')
) b 
on a.month = b.month and active_u_user = yc_user_id
group by a.month