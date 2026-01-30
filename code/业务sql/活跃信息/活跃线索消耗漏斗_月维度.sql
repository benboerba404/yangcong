DROP TABLE IF EXISTS tmp.niyiqiao_active_user_pool_paid_month；
CREATE TABLE  IF NOT EXISTS tmp.niyiqiao_active_user_pool_paid_month AS (
select 
    from_unixtime(unix_timestamp(cast(concat(month,01) as string),'yyyyMMdd'),'yyyy-MM-dd') month,
    grade_name_month,
    stage_name_month,
    user_pay_status_business_month,
    count(distinct  active_u_user ) active_user,--活跃用户量
    count(distinct case when push_u_user is not null then active_u_user end) push_user,--推送到电销量
    count(distinct case when enter_datapool_u_user is not null then active_u_user end) enter_datapool_user,--进入公海池线索量
    count(distinct case when recieve_u_user is not null then active_u_user end) midschool_recieve_user,--被坐席通过电话线索触达线索量
    count(distinct case when recieve_paid_u_user is not null then active_u_user end) midschool_recieve_paid_user,--被坐席通过电话线索触达线索量转化线索量
    sum(recieve_paid_amount) midschool_recieve_paid_amount,--被坐席通过电话线索触达线索量转化金额
    count(distinct case when recieve_u_user_all is not null then active_u_user end) all_recieve_user,--公海池线索触达量，不限制线索来源
    count(distinct case when recieve_all_paid_u_user is not null then active_u_user end) all_recieve_paid_user,--公海池线索触达转化量，不限制线索来源
    sum(recieve_all_paid_amount) all_recieve_paid_amount,--公海池线索触达转化量，不限制线索来源
    count(distinct case when active_recieve_u_user_all is not null then active_u_user end) active_recieve_user,--活跃领取量(活跃触达量)
    count(distinct case when active_paid_u_user_all is not null then active_u_user end) active_paid_user,--活跃领取转化量(活跃触达转化量)
    sum(active_paid_amount_all) active_paid_amount--活跃领取转化金额(活跃触达转化金额)
from aws.crm_active_data_pool_paid_month 
where month > 202305
group  by 1,2,3,4 