select  
city_class
,count(distinct user_id) `外呼用户量` --拨打线索数
,count(distinct action_id) `外呼次数`
,count(distinct case when is_valid_connect = 1 then user_id end) `有效接通用户量` --10s以上
from tmp.niyiqiao_crm_clue_call_record
where substr(call_created_at,1,10) between '2025-01-01' and '2025-12-31'
group by 
city_class