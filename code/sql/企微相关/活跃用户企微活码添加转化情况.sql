DROP TABLE IF EXISTS tmp.niyiqiao_active_user_wechat_paid_month；
CREATE TABLE  IF NOT EXISTS tmp.niyiqiao_active_user_wechat_paid_month AS (
select 
    from_unixtime(unix_timestamp(cast(concat(month,01) as string),'yyyyMMdd'),'yyyy-MM-dd') month,
    grade_name_month,
    stage_name_month,
    user_pay_status_business_month, 
    count(distinct  active_u_user ) active_user, --活跃量
    count(distinct case when add_wechat_u_user is not null then  active_u_user end ) wechat_add_user, --企微添加量
    count(distinct case when recieve_u_user is not null then  active_u_user end ) wechat_recieve_user,--企微拉取入库量
    count(distinct case when recieve_paid_u_user is not null then  active_u_user end ) wechat_recieve_paid_user, --拉取入库后转化量
    sum(recieve_paid_amount) wechat_recieve_paid_amount --拉取入库后转化金额
from  aws.crm_active_user_wechat_paid_month
where month > 202305
group by 1,2,3,4