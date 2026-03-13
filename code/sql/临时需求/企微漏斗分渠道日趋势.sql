-- 企微添加量异常分析 Part 2：分渠道类型漏斗日趋势
-- 目的：按渠道类型(type_name)拆解漏斗，定位是哪个渠道导致添加量下降
-- 时间范围：20260201 ~ 20260311

select 
    day as `日期`,
    type_name as `渠道类型`,
    count(distinct get_entrance_user) as `曝光量`,
    count(distinct click_entrance_user) as `点击量`,
    count(distinct get_wechat_user) as `二维码曝光量`,
    count(distinct add_wechat_user) as `添加量`,
    count(distinct pull_wechat_user) as `拉取入库量`,
    round(count(distinct click_entrance_user) * 100.0 / nullif(count(distinct get_entrance_user), 0), 2) as `点击率`,
    round(count(distinct get_wechat_user) * 100.0 / nullif(count(distinct click_entrance_user), 0), 2) as `二维码曝光率`,
    round(count(distinct add_wechat_user) * 100.0 / nullif(count(distinct get_wechat_user), 0), 2) as `添加率`,
    round(count(distinct pull_wechat_user) * 100.0 / nullif(count(distinct add_wechat_user), 0), 2) as `拉取入库率`
from aws.user_pay_process_add_wechat_day
where day between 20260201 and 20260311
group by day, type_name
order by type_name, day
