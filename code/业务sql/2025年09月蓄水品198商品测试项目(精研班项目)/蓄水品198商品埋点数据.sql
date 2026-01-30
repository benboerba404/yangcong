    select day,u_user,event_key,os,scene,option,task_id,string(operate_ids) operate_ids,page_type,from_unixtime(substr(event_time,1,10),'yyyy-MM-dd HH:mm:ss') event_time
    from events.frontend_event_orc
    where day between  20251210 and 20251211
    and event_type  = 'get'
    and event_key = 'get_PaySceneEntrance'
    and os in ('android','ios')
    and page_type='引流'
    and scene='ad-mytab-notification'
    and product_id = '01'
    and length(u_user)>0
    and length(task_id)>0
    and string(operate_ids) regexp '99640550-d56c-11f0-af56-23f706d96968|89353884-d56c-11f0-a986-af38bb939a0b|b1b6ba1c-d56c-11f0-a987-f372f0d6f0f9'
    -- and string(operate_ids) regexp '583a116e-d63e-11f0-a05b-6bd0bc4fb43c'
    group by day,u_user,event_key,os,scene,option,task_id,string(operate_ids),page_type,from_unixtime(substr(event_time,1,10),'yyyy-MM-dd HH:mm:ss')
    order by event_time
    
    select day,u_user,event_key,os,scene,option,task_id,operate_id,page_type,from_unixtime(substr(event_time,1,10),'yyyy-MM-dd HH:mm:ss') event_time
    from events.frontend_event_orc
    where day between  20251210 and 20251211
    and event_type  = 'click'
    and event_key = 'click_PaySceneEntrance'
    and os in ('android','ios')
    and page_type='引流'
    and scene='ad-mytab-notification'
    and product_id = '01'
    and length(u_user)>0
    and length(task_id)>0
    and operate_id in ('99640550-d56c-11f0-af56-23f706d96968','89353884-d56c-11f0-a986-af38bb939a0b','b1b6ba1c-d56c-11f0-a987-f372f0d6f0f9')
    group by day,u_user,event_key,os,scene,option,task_id,operate_id,page_type,from_unixtime(substr(event_time,1,10),'yyyy-MM-dd HH:mm:ss')
    order by event_time