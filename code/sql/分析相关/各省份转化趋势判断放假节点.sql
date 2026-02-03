with active_user_c as (
    -- 1. 获取 C 端活跃用户及其营收、学段数据
    select 
        day,
        u_user,
        business_user_pay_status_business,--业务分层
        business_user_pay_status_statistics,--统计分层
        user_strategy_tag_day,--策略用户标签（高净值用户区分历史大会员用户、付费组合品用户、付费加购品用户）
        stage_name_month as stage_name, -- 学段
        sum(normal_price_amount) as amount
    from aws.business_active_user_last_14_day
    where day between '20260101' and '20260125'
    group by day, u_user, stage_name_month
),
user_province as (
    -- 2. 获取用户省份信息
    select 
        day,
        u_user,
        province
    from dws.topic_user_active_detail_day
    where day between '20260101' and '20260125'
    group by day, u_user, province
),
active_with_province as (
    -- 3. 将 C 端活跃用户与省份、学段信息关联
    select 
        a.day,
        a.u_user,
        a.amount,
        a.stage_name,
        b.province
    from active_user_c a 
    inner join user_province b on a.u_user = b.u_user and a.day = b.day
)
-- 4. 按省份、学段和日期聚合
select 
    day,
    province,
    stage_name,
    count(distinct u_user) as active_uv,                       -- C端活跃UV
    count(distinct if(amount > 0, u_user, null)) as pay_uv,      -- 付费UV
    sum(coalesce(amount, 0)) as total_amount,                   -- 总营收
    count(distinct if(amount > 0, u_user, null)) / count(distinct u_user) as conversion_rate -- 转化率
from active_with_province
group by day, province, stage_name
order by day, province, stage_name;


--城市线级
select 
    province_code
    ,province
    ,city_code
    ,city
    ,area_code
    ,area
    ,is_has_area
    ,city_class --城市线级
from dw.dim_region