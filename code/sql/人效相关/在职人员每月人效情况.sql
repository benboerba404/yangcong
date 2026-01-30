insert overwrite table tmp.wuhan_staff_efficiency partition(dt)
select
t0.month,
t0.workerid,
t0.name,
case when t0.name in ('韩碧成','杨欢','周益微','赵晓方','刘依','陈晨','郑彩铭','饶小琴','韩碧成','李欣慧','王燕','徐倩'
,'杨远翔','舒真','石敬夫','张文君','李新竹','李苏娟','范居丹妮','赵素雅') then 1 else 0 end not_tele,
level5_id,
level3_id,
level2_id,
in_date,
out_date,
status,
month_begin_on_the_job,
month_end_on_the_job,
current_month_entry,
current_month_leave,
current_month_entry_leave,
entry_days,
month_begin_on_the_job_s,
month_end_on_the_job_s,
current_month_entry_s,
current_month_leave_s,
case when entry_days<=30 then '[0,30]'
     when entry_days<=60 then '(30,60]'
     when entry_days<=90 then '(60,90]'
     when entry_days<=120 then '(90,120]'
     when entry_days<=150 then '(120,150]'
     when entry_days<=180 then '(150,180]'
     when entry_days<=360 then '(180,360]'
     else '(360,+inf)' end entry_days_cut,
t2.cnt,
t2.amount,
t2.cnt_pad,
t3.recieve_cnt,
t3.recieve_gonghai_cnt,
t3.recieve_xinzeng_cnt,
t3.recieve_xufei_cnt,
t3.recieve_xinzeng_a_cnt,
t3.recieve_xufei_a_cnt,
t4.call_leads_cnt,
t4.call_valid_leads_cnt,
t4.call_time_length,
t4.call_xinzeng_leads_cnt,
t4.call_xufei_leads_cnt,
t4.call_valid_xinzeng_leads_cnt,
t4.call_valid_xufei_leads_cnt,
t4.call_time_length_xinzeng,
t4.call_time_length_xufei,
t5.recieve_distinct_cnt,
t5.paid_cnt,
t5.paid_pad_cnt,
t5.paid_nopad_cnt,
t5.paid_amount,
t5.paid_pad_amount,
t5.paid_nopad_amount
,cast(regexp_replace(t0.month, '-', '') as int) as dt
from
(
    select a.year_month,substr(a.month_begin_date_id,1,7) month,a.month_begin_date_id,a.month_end_date_id
    ,b.workerid,b.name,b.in_date,b.out_date,b.status
    ,case when b.in_date<=a.month_begin_date_id then 1 else 0 end as month_begin_on_the_job--月初是否在职
    ,case when b.out_date is null or b.out_date>=a.month_end_date_id then 1 else 0 end month_end_on_the_job--月末是否在职
    ,case when b.in_date between a.month_begin_date_id and a.month_end_date_id then 1 else 0 end current_month_entry-- 当月入职
    ,case when b.out_date is not null and b.out_date between a.month_begin_date_id and a.month_end_date_id then 1 else 0 end current_month_leave-- 当月离职
    ,case when b.out_date is not null and b.in_date between a.month_begin_date_id and a.month_end_date_id
                and b.out_date between a.month_begin_date_id and a.month_end_date_id then 1 else 0 end current_month_entry_leave--当月入职且当月离职
    ,case when b.out_date is null and a.month_end_date_id<=date_sub(current_date,1) then datediff(a.month_end_date_id,b.in_date) 
          when b.out_date is null and a.month_end_date_id>date_sub(current_date,1) then datediff(date_sub(current_date,1),b.in_date)
          when b.out_date is not null and b.out_date>=a.month_end_date_id then datediff(a.month_end_date_id,b.in_date)
          else datediff(b.out_date,b.in_date) end entry_days--截止当月入职天数
    ,case when b.in_date<=a.month_begin_date_id then '是' else '否' end as month_begin_on_the_job_s --月初是否在职
    ,case when b.out_date is null or b.out_date>=a.month_end_date_id then '是' else '否' end month_end_on_the_job_s--月末是否在职
    ,case when b.in_date between a.month_begin_date_id and a.month_end_date_id then '是' else '否' end current_month_entry_s -- 当月入职
    ,case when b.out_date is not null and b.out_date between a.month_begin_date_id and a.month_end_date_id then '是' else '否' end current_month_leave_s-- 当月离职
    from
    (
        select  distinct year_month,month_begin_date_id,month_begin_date,month_end_date,month_end_date_id
        from dw.dim_date 
        where day between REGEXP_REPLACE(trunc(date_sub(current_date,1),'MM'), '-', '') and REGEXP_REPLACE(date_sub(current_date,1), '-', '')
    ) a
    left join
    (
        select id as workerid,user_name name,in_date,out_date,status
        from 
        (
            select 
            t0.created_at,t0.user_name,t0.email,t0.in_date,t0.out_date,t0.status,t0.department,t1.id,t1.name,aa
            from 
            (
                select created_at,user_id,email,person_id,employment_no,user_name,substr(start_date,1,10) in_date
                ,if(substr(stop_date,1,10) in ('0001-01-01','0001-01-03'),null,substr(stop_date,1,10)) out_date,status,department
                ,row_number() over (partition by employment_no order by created_at desc) as aa
                from crm.staff_change
            ) t0
            left join 
            crm.worker t1
            on t0.email=t1.mail
            where aa=1 and id is not null
        ) s
    ) b
    on a.month_end_date_id>=b.in_date and (b.out_date is null or b.out_date>=a.month_begin_date_id)   
) t0 --所有员工的入离职时间，每月的在职情况
left join 
(
    select month,worker_id,name,level5_id,level3_id,level2_id,aa 
    from 
    (
        select substr(date,1,7) month,worker_id,name,level5_id,level3_id,level2_id
        ,row_number() over(partition by substr(date,1,7),worker_id,name order by date desc) aa
        from tmp.wuhan_organization_v1 
        where  date>=trunc(date_sub(current_date,1),'MM') and (level5_id<>32 or level5_id is null) 
    ) a 
    where aa=1
) t1 --取每月员工最后所在的组织结构（剔除离职的记录取离职前的组织架构）
on t0.workerid=t1.worker_id and t0.month=t1.month
left join
(
    select substr(pay_time,1,7) month,workerid,sum(cnt) cnt,sum(amount) amount,sum(case when is_pad='平板订单' then cnt end) cnt_pad
    from tmp.wuhan_crm_group_revenue_day
    group by substr(pay_time,1,7),workerid
) t2 -- 每月营收业绩，订单量，平板硬件订单量
on t0.workerid=t2.workerid and t0.month=t2.month
left join 
(
    SELECT
    substr(created_at,1,7) AS month 
    ,worker_id workerid
    ,count(user_id) recieve_cnt --领取线索量
    ,count(case when clue_source='mid_school' then user_id end) recieve_gonghai_cnt --公海池领取线索量
    ,count(case when user_type in (1,3) then user_id end) recieve_xinzeng_cnt --新增线索量
    ,count(case when user_type=2 then user_id end) recieve_xufei_cnt --续费线索量
    ,count(case when user_type in (1,3) and get_json_object(extend,"$.ab") ='A' then user_id end) recieve_xinzeng_a_cnt --新进新增线索量
    ,count(case when user_type=2 and get_json_object(extend,"$.ab") ='A' then user_id end) recieve_xufei_a_cnt --新进续费线索量
    FROM dw.fact_clue_allocate_info
    WHERE SUBSTR(created_at,1,10) BETWEEN trunc(date_sub(current_date,1),'MM') AND date_sub(current_date,1)
    -- and source='mid_school'
    group by substr(created_at,1,7),workerid
) t3 --每月领取线索量 
on t0.workerid=t3.workerid and t0.month=t3.month
left join 
(
    select 
    substr(created_at,1,7) month 
    ,worker_id workerid
    ,count(distinct info_uuid) call_leads_cnt --外呼线索量
    ,count(distinct case when call_time_length>=10 then info_uuid end) call_valid_leads_cnt --有效接通线索量
    ,sum(call_time_length) call_time_length --总通时
    ,count(distinct case when user_type in (1,3) then info_uuid end) call_xinzeng_leads_cnt --外呼新增线索量
    ,count(distinct case when user_type=2 then info_uuid end) call_xufei_leads_cnt --外呼续费线索量
    ,count(distinct case when call_time_length>=10 and user_type in (1,3) then info_uuid end) call_valid_xinzeng_leads_cnt --有效接通新增线索量
    ,count(distinct case when call_time_length>=10 and user_type=2 then info_uuid end) call_valid_xufei_leads_cnt --有效接通续费线索量
    ,sum(case when user_type in (1,3) then call_time_length end) call_time_length_xinzeng --新增线索总通时
    ,sum(case when user_type=2 then call_time_length end) call_time_length_xufei --续费线索总通时
    from dw.fact_call_history 
    where day between CAST(REGEXP_REPLACE(trunc(date_sub(current_date,1),'MM'), '-', '') AS INT) and CAST(REGEXP_REPLACE(DATE_SUB(CURRENT_DATE(), 1), '-', '') AS INT)
    and length(user_id)>0
    group by substr(created_at,1,7),workerid
) t4 --每月通话时长，拨打线索量，接通线索量
on t0.workerid=t4.workerid and t0.month=t4.month
left join
(
    select
    substr(created_at,1,7) month
    ,a.workerid
    ,count(distinct a.userid) recieve_distinct_cnt
    ,count(distinct b.userid) paid_cnt
    ,count(distinct case when c.order_id is not null then b.userid end) paid_pad_cnt
    ,count(distinct case when c.order_id is null then b.userid end) paid_nopad_cnt
    ,sum(b.amount) paid_amount
    ,sum(case when c.order_id is not null then b.amount end) paid_pad_amount
    ,sum(case when c.order_id is null then b.amount end) paid_nopad_amount
    from 
    (
        select 
        substr(created_at,1,7) month
        ,worker_id workerid
        ,user_id userid
        ,min(substr(created_at,1,19)) AS created_at --领取时间
        FROM dw.fact_clue_allocate_info
        WHERE SUBSTR(created_at,1,10) BETWEEN trunc(date_sub(current_date,1),'MM') AND date_sub(current_date,1)
        and length(user_id)>0
        group by substr(created_at,1,7),workerid,userid
    )a 
    left join 
    (
        SELECT 
        user_id userid,
        substr(pay_time,1,19) pay_time,
        worker_id workerid,
        order_id orderid,
        amount
        FROM dw.fact_order_crm
        WHERE 
        substr(pay_time,1,10)>= trunc(date_sub(current_date,1),'MM')
        and not ((order_type in (1,3) AND amount<=198) or (order_type=2 AND amount<=298))
    ) b --转化数据
    on a.userid=b.userid and a.workerid=b.workerid and pay_time>created_at and substr(b.pay_time,1,7)=substr(a.created_at,1,7) 
    left join
    (
        select order_id from
        (
            select
            order_id,
            COLLECT_SET(kind) concat_kind
            from dw.fact_order_detail 
            where 
            SUBSTR(paid_time,1,10)>=trunc(date_sub(current_date,1),'MM') AND SUBSTR(paid_time,1,10)<=date_sub(current_date,1)
            AND business_attribution = '轻课营收' 
            and is_parent_telemarketing=TRUE
            group by order_id
        ) t
        where array_contains(concat_kind,'pad')
    ) c -- 平板订单
    on b.orderid=c.order_id
    group by 
    substr(created_at,1,7)
    ,a.workerid
) t5 --当月领取当月转化线索量
on t0.workerid=t5.workerid and t0.month=t5.month