-- =====================================================
-- 渠道活码维度表 crm.qr_code_change_history
-- =====================================================
-- 【表粒度】
--   渠道活码每次变更生成一条记录（变更历史），同一 qr_code_id 可有多条记录
--   无分区字段，全量表
--
-- 【业务定位】
--   企微业务的核心维度表，所有涉及渠道活码ID(qr_code_id)的场景都需要通过本表获取渠道信息
--   渠道信息被修改时会生成一条新记录，以最新记录为准
--
-- 【核心字段】
--   qr_code_id          渠道活码ID，关联其他表的主键
--   created_at           活码创建时间（指 qr_code_created_at）
--   resource_entrance_name 资源位入口名称
--   scene_name           场景名称（渠道活码ID对应的名字）
--   clue_level_name      渠道的等级分类
--   status               1=上线，2=下线
--
-- 【取最新有效记录的标准写法】
--   按 qr_code_id 分组，按 effective_time DESC 排序取第一条，
--   并过滤 deleted_at IS NULL、type_name <> '测试类型'：
--
--   SELECT *
--   FROM (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY qr_code_id ORDER BY effective_time DESC) rn
--     FROM crm.qr_code_change_history
--     WHERE deleted_at IS NULL
--       AND type_name <> '测试类型'
--   ) t
--   WHERE rn = 1
--
-- 【常用关联方式】
--   企微漏斗表：task_id = qr_code_id
--   企微线索表(aws.clue_info)：qr_code_channel_id = qr_code_id

-- =====================================================

CREATE TABLE
  `crm`.`qr_code_change_history` (
    `id` bigint COMMENT '主键ID',
    `created_at` timestamp COMMENT '记录创建时间（本条变更记录的写入时间）',
    `updated_at` timestamp COMMENT '记录更新时间',
    `deleted_at` timestamp COMMENT '软删除时间，NULL表示未删除，查询时需过滤 deleted_at IS NULL',
    `qr_code_id` bigint COMMENT '渠道活码ID，核心关联字段，关联企微漏斗表的 task_id 或线索表的 qr_code_channel_id',
    `resource_entrance_id` bigint COMMENT '资源位入口ID',
    `resource_entrance_name` string COMMENT '资源位入口名称',
    `clue_level_id` bigint COMMENT '线索等级ID',
    `clue_level_name` string COMMENT '线索等级名称（渠道的等级分类，核心筛选/分组字段）',
    `scene_name` string COMMENT '场景名称（渠道活码ID对应的名字，即该活码的业务场景）',
    `status` bigint COMMENT '状态：1=上线，2=下线',
    `type` bigint COMMENT '类型ID',
    `type_name` string COMMENT '类型名称，查询时通常排除"测试类型"',
    `effective_time` timestamp COMMENT '本条记录生效时间，取最新有效记录时按此字段 DESC 排序',
    `invalid_time` timestamp COMMENT '本条记录失效时间，默认 2099-01-01 表示当前仍生效',
    `operator_id` bigint COMMENT '操作人ID',
    `qr_code_created_at` timestamp COMMENT '渠道活码首次创建时间（非本条记录创建时间，是活码本身的创建时间）'
  ) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/crm.db/qr_code_change_history' TBLPROPERTIES (
    'STATS_GENERATED_VIA_STATS_TASK' = 'true',
    'bucketing_version' = '2',
    'creation_platform' = 'coral',
    'is_core' = 'false',
    'is_starred' = 'false',
    'status' = '3',
    'transient_lastDdlTime' = '1769755215'
  )
