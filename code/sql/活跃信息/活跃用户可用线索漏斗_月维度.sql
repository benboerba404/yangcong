-- 活跃用户可用线索漏斗分析_月维度
-- 需求：分析每月活跃用户中，可被电销消耗的线索量
-- 指标：活跃量、学习活跃量、已在坐席名下量、已有服务期量、已有非电销服务期量、可消耗线索量
-- 维度：月份、学段
-- 日期：近半年

-- 漏斗逻辑：
-- 活跃用户量 → 学习活跃用户量 → 后续指标均基于学习活跃用户计算

-- 服务期判断逻辑（user_allocation 枚举值：电销/网销、体验营、入校、新媒体视频）：
-- 1. 电销服务期：user_allocation 数组中任一元素包含"电销"
-- 2. 非电销服务期：user_allocation 不为空且不包含"电销"
-- 3. 无服务期：user_allocation 为 NULL 或空数组

-- 在库判断：is_clue_seat 字段（已校验，匹配率99.35%）

-- 可消耗线索定义：
-- 学习活跃用户中，不在坐席名下 且 非"非电销服务期"的用户

with t_active as (
  -- 活跃用户基础数据
  select 
    substr(cast(day as string), 1, 6) as month
    ,mid_stage_name as stage_name  -- 学段
    ,u_user
    ,is_learn_active_user  -- 是否学习活跃用户
    ,is_clue_seat  -- 在库标识（1=在库，0=不在库）
    ,user_allocation
    -- 服务期分类
    ,case 
      when user_allocation is null or size(user_allocation) = 0 then '无服务期'
      when array_join(user_allocation, ',') like '%电销%' then '电销服务期'
      else '非电销服务期'
    end as allocation_type
  from dws.topic_user_active_detail_day
  where day >= 20250801  -- 近半年
    and day < 20260201
)

select 
  concat(substr(month, 1, 4), '-', substr(month, 5, 2)) as `月份`
  ,stage_name as `学段`
  -- 第一层：活跃用户量
  ,count(distinct u_user) as `活跃用户量`
  -- 第二层：学习活跃用户量
  ,count(distinct case when is_learn_active_user = 1 then u_user end) as `学习活跃用户量`
  -- 后续漏斗基于学习活跃用户
  ,count(distinct case when is_learn_active_user = 1 and is_clue_seat = 1 then u_user end) as `已在坐席名下量`
  ,count(distinct case when is_learn_active_user = 1 and allocation_type <> '无服务期' then u_user end) as `已有服务期量`
  ,count(distinct case when is_learn_active_user = 1 and allocation_type = '电销服务期' then u_user end) as `电销服务期量`
  ,count(distinct case when is_learn_active_user = 1 and allocation_type = '非电销服务期' then u_user end) as `非电销服务期量`
  -- 可消耗线索 = 学习活跃 且 不在库 且 非"非电销服务期"
  ,count(distinct case 
    when is_learn_active_user = 1 and is_clue_seat = 0 and allocation_type <> '非电销服务期' 
    then u_user end) as `可消耗线索量`
  -- 占比（基于学习活跃用户）
  ,round(count(distinct case when is_learn_active_user = 1 then u_user end) * 100.0 
    / count(distinct u_user), 2) as `学习活跃占比(%)`
  ,round(count(distinct case when is_learn_active_user = 1 and is_clue_seat = 1 then u_user end) * 100.0 
    / nullif(count(distinct case when is_learn_active_user = 1 then u_user end), 0), 2) as `在库占比(%)`
  ,round(count(distinct case when is_learn_active_user = 1 and allocation_type = '非电销服务期' then u_user end) * 100.0 
    / nullif(count(distinct case when is_learn_active_user = 1 then u_user end), 0), 2) as `非电销服务期占比(%)`
  ,round(count(distinct case 
    when is_learn_active_user = 1 and is_clue_seat = 0 and allocation_type <> '非电销服务期' 
    then u_user end) * 100.0 / nullif(count(distinct case when is_learn_active_user = 1 then u_user end), 0), 2) as `可消耗占比(%)`
from t_active
group by month, stage_name
order by month, stage_name
