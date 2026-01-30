--测试品添加企微漏斗数据
with t0 as -- 测试品售卖量
(
  select
  substr(paid_time,1,10) paid_time
  ,u_user
  ,good_name
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,business_good_kind_name_level_3
  ,business_gmv_attribution
  ,sum(sub_amount) as amount
  from dws.topic_order_detail 
  where 
  paid_time_sk between 20250901 and 20250921
  and business_gmv_attribution in ('商业化','电销')
  and sku_group_good_id in 
  ('dcd05626-7ba5-4507-a1cb-d9f2685b4cdd',
  '2027cbd6-7ba1-438d-9488-8257d92387c5',
  'de1a18b0-6de3-450e-b105-cdbacb855ac2',
  '3462ff20-5b06-4ab2-95d6-110629234209',
  'e00cf31e-4d27-4b26-aa28-7bdfc51b6882',
  '5c75c450-8ca5-4a8a-a66a-56277d61d9f6',
  'c9c1ee72-ad3f-4938-8160-a7596718eb2f',
  'b119de7d-6485-4bc5-84b0-2ba2514baed2',
  'ffeea15c-b5e3-4459-ad87-86ce0b8de702',
  'd149c8a7-3af4-4f83-8f4f-eb07307c8e07',
  'b3f3d62b-5853-452a-8eb1-5aee3d501a46',
  '67660b75-8bab-421e-8b8a-488ed49f377f',
  'f9548515-d264-4e95-a786-919008c736aa',
  '8c0f6d24-09ef-4083-a501-11bdf7f0d674',
  'da1b107c-8b17-4eab-a737-83648144a35f',
  '02b8fc6c-3055-4e86-acfd-e7f52a211109',
  'bc4c1b30-be2f-4b7e-98b6-3255255aa25f',
  'f1dbaded-2866-42e1-a7e6-d8e8695a0323',
  'ae1111e1-4eea-466d-9aa1-902bef63d8d9')
  group by 1,2,3,4,5,6,7
)

,t1 as --后续点击数据
(
  select
  distinct 
  from_unixtime(unix_timestamp(cast(day as string),'yyyyMMdd'),'yyyy-MM-dd') day
  ,scene
  ,task_id
  ,b.channel_name
  ,click_entrance_user
  ,get_wechat_user
  ,add_wechat_user
  ,pull_wechat_user
  ,paid_7d_user
  ,paid_7d_amount
  ,paid_14d_user
  ,paid_14d_amount
  ,paid_current_month_user
  ,paid_current_month_amount
  from aws.user_pay_process_add_wechat_day a
  left join 
  tmp.wuhan_wecom_channel_id b
  on a.task_id=b.id
  where day between 20250901 and 20250921
  and click_entrance_user is not null
  and task_id in (3,20,535,577,576,575)
)

select 
paid_time
,count(distinct u_user) `测试品售卖量`
,count(distinct case when click_entrance_user is not null then u_user end) `资源位点击量`
,count(distinct case when get_wechat_user is not null then u_user end) `坐席二维码曝光量`
,count(distinct case when add_wechat_user is not null then u_user end) `企微添加量`
,count(distinct case when pull_wechat_user is not null then u_user end) `拉取入库量`
from t0 
left join t1 on u_user = click_entrance_user
group by paid_time 
order by paid_time

