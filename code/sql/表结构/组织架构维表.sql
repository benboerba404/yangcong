-- =====================================================
-- 组织架构维表 dw.dim_crm_organization
-- =====================================================
-- 【表说明】
--   电销团队组织架构通用维表。
--   职场、学部、团、小组 共用一张表，通过 id 字段区分层级。
--
-- 【关联方式】⚠️ 重点
--   需要获取团名称、小组名称等，必须关联本表。
--   同一个查询中可 left join 多次（用不同别名），分别获取不同层级的名称：
--
--   left join dw.dim_crm_organization d0 on t.workplace_id  = d0.id  -- 职场名称
--   left join dw.dim_crm_organization d1 on t.department_id = d1.id  -- 学部名称
--   left join dw.dim_crm_organization d2 on t.regiment_id   = d2.id  -- 团名称
--   left join dw.dim_crm_organization d4 on t.team_id       = d4.id  -- 小组名称
--
-- 【字段说明】
--   取哪个层级，对应取哪个 _name 字段：
--   - 职场：d0.workplace_name
--   - 学部：d1.department_name
--   - 团：  d2.regiment_name
--   - 小组：d4.team_name
--
-- ⚠️ 常见错误：
--   - 错误：直接在 aws.clue_info 或 aws.crm_order_info 里取 regiment_name —— 两表没有该字段！
--   - 正确：通过 regiment_id JOIN 本表，取 d2.regiment_name
-- =====================================================

CREATE TABLE `dw`.`dim_crm_organization` (
  `id`               int    COMMENT '组织单元ID（与 workplace_id / department_id / regiment_id / team_id 关联）',
  `workplace_name`   string COMMENT '职场名称（当 id = workplace_id 时使用）',
  `department_name`  string COMMENT '学部名称（当 id = department_id 时使用）',
  `regiment_name`    string COMMENT '团名称（当 id = regiment_id 时使用）',
  `team_name`        string COMMENT '小组名称（当 id = team_id 时使用）',
  `parent_id`        int    COMMENT '父级组织ID',
  `level`            int    COMMENT '组织层级（1=职场、2=学部、3=团、4=小组）',
  `status`           int    COMMENT '状态（1=有效）'
);
