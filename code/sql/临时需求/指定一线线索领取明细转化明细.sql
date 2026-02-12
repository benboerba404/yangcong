with t1 as (
  select 
    a.user_id
    ,substr(a.created_at,1,19) created_at
    ,substr(a.created_at,1,7) created_ym
    ,user_type_name
    ,clue_stage
    ,clue_grade
    ,b.clue_source_name
    ,b.clue_source_name_level_1
    ,worker_id
    ,worker_name
    ,workplace_id
    ,department_id
    ,regiment_id
    ,team_id
    ,c.phone
  from aws.clue_info a
  left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
  left join 
  (
    select  
    u_user,if(phone is null,phone,if(phone rlike "^\\d+$",phone,cast(unbase64(phone) as string))) AS phone
    from dw.dim_user
    where length(phone)>0
  ) c 
    on a.user_id = c.u_user
  where 
    substr(a.created_at,1,10) between '2025-12-01' and '2026-01-31'
    and user_sk > 0
    and worker_id <> 0
    and a.workplace_id in (4,400,702)
)

, t2 as (
select 
  substr(pay_time,1,19) pay_time 
  ,substr(pay_time,1,7) pay_ym 
  ,worker_id
  ,order_id
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2 
  ,user_id
  ,amount
  ,worker_name
from aws.crm_order_info a
where
  substr(pay_time,1,10)  between '2025-12-01' and '2026-01-31'
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
  and status = '支付成功'
)

select   
substr(t1.created_at,1,19) created_at
,t1.clue_source_name
,t1.worker_name
,d0.workplace_name
,d1.department_name
,d2.regiment_name
,d4.team_name
,t1.user_id
,phone
,substr(pay_time,1,19) pay_time
,order_id
,amount
,t2.worker_name
from t1
left join t2 on t1.user_id = t2.user_id  and t1.created_at < t2.pay_time --and t1.worker_id = t2.worker_id
left join dw.dim_crm_organization d0 on t1.workplace_id = d0.id
left join dw.dim_crm_organization d1 on t1.department_id = d1.id
left join dw.dim_crm_organization d2 on t1.regiment_id = d2.id
left join dw.dim_crm_organization d4 on t1.team_id = d4.id
where t1.worker_name in (
'黄高翔01',
'刘泉泉01',
'万盛02',
'涂志财01',
'李娜05',
'陈升升01',
'黄文君01',
'郭茜01',
'唐小芸01',
'陶灯松01',
'张琳琳01',
'赵耀01',
'徐嘉俊01',
'李博02',
'刘婧02',
'李城沉01',
'徐乐01',
'罗蒙02',
'姜爽01',
'胡乐01',
'王绍杭01',
'任治翔01',
'刘小霞01',
'徐棒01',
'宋发毅01',
'曹骊戈01',
'魏爽01',
'陈永康01',
'周源浩01',
'吴迪03',
'雷帆01',
'张丽芸',
'鲁馨怡01',
'邓红林01',
'邹一帆01',
'李文祥02',
'安航05',
'刘力媛01',
'黄思敏01',
'金晶01',
'朱志豪',
'覃金涛01',
'林云02',
'熊锦龙02',
'成正军01',
'李根01',
'何奇峻01',
'李强04',
'王超',
'杨佳琦01',
'郭勇01',
'熊雯',
'孙周01',
'石鑫01',
'王国升01',
'徐小雨',
'王建锋01',
'陈铸01',
'徐迎01',
'何流星',
'许鹏01',
'史志伟01',
'安航06',
'曾毅01',
'徐文杰02',
'管纬地01',
'陈勇01',
'姜杨杨01',
'王孝涛01',
'张海',
'翟胜浩02',
'童海微01',
'钱邈舜01',
'康梦帆03',
'李汝01',
'彭为01',
'陈诗语01',
'郭欣月01',
'杨志高01',
'刘磊02',
'冯雷',
'姜鹏',
'张斌斌01',
'张琛',
'程灿灿01',
'朱凯',
'谢振01',
'徐慧',
'吕波02',
'周易',
'徐亚洲01',
'张达01',
'刘军03',
'郭静霏01',
'李杰01',
'张志博02',
'刘梦婷',
'高伟01',
'马原',
'卢胜兴01',
'张涛01',
'肖力02',
'赵飞01',
'刘微02',
'张学龙01',
'张伟龙02',
'刘程01',
'詹文龙',
'刘坤01',
'汪力01',
'刘鹏02',
'万昌主',
'卢光明01',
'陈兆功01',
'刘艳01',
'陈健05',
'刘雅倩01',
'田芊01'
)
