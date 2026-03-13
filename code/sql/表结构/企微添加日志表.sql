-- =====================================================
-- 企微添加日志表 crm.contact_log
-- =====================================================
-- 【表粒度】
--   每次企微联系人变更事件生成一条记录（添加/删除等），无分区字段，全量表
--
-- 【业务定位】
--   企微业务的核心事实表，记录用户与坐席之间的企微联系人变更事件
--   常规企微业务指标（企微添加量、拉取入库量）均以本表为基础计算
--   → glossary.md #常规企微业务指标
--
-- 【常用筛选条件】
--   企微添加场景：source = 3（渠道活码）AND change_type = 'add_external_contact'
--   分组织架构时：group_id0 IN (4, 400, 702)（电销职场）
--
-- 【常用关联方式】
--   渠道维度：channel_id = crm.qr_code_change_history.qr_code_id
--   拉取入库：external_user_id = aws.clue_info.we_com_open_id AND worker_id
--   组织架构：group_id1~group_id4 分别关联 dw.dim_crm_organization 获取 department/regiment/heads/team 名称
--   坐席信息：worker_id = crm.worker.id
--
-- 【去重逻辑】
--   企微添加去重（取首次添加）：
--   - 不分组织架构：PARTITION BY (external_user_id, worker_id, channel_id, yc_user_id) ORDER BY created_at
--   - 分组织架构：PARTITION BY (external_user_id, worker_id, channel_id) ORDER BY created_at
--
-- 【source 枚举值】
--   -1: 未知, 0: 海报, 1: 短信, 3: 渠道活码
--
-- 【is_repeated_exposure 说明】
--   2025-07-26 起有值，之前的历史数据无法区分是否重复曝光
-- =====================================================

CREATE TABLE `crm.contact_log`(
  `id` bigint COMMENT '主键',
  `created_at` timestamp COMMENT '事件发生时间，企微添加指标按此字段统计日期',
  `updated_at` timestamp COMMENT '最近一次更新时间',
  `event` string COMMENT '触发事件',
  `change_type` string COMMENT '变更类型，企微添加场景筛选 add_external_contact',
  `userid` string COMMENT '坐席企微userid（企微体系内的ID）',
  `external_user_id` string COMMENT '用户企微userid，企微添加量的去重主键，关联 aws.clue_info.we_com_open_id',
  `worker_id` bigint COMMENT '坐席ID（CRM系统ID），关联 crm.worker.id',
  `channel_id` bigint COMMENT '渠道活码ID，关联 crm.qr_code_change_history.qr_code_id',
  `source` bigint COMMENT '来源：-1=未知, 0=海报, 1=短信, 3=渠道活码',
  `topic_id` bigint,
  `rule_template_id` bigint COMMENT '渠道活码规则ID',
  `account_id` bigint COMMENT '企业微信账号id',
  `yc_user_id` string COMMENT '洋葱用户id，不分组织架构时参与去重(四维去重)，筛选 length=24 且排除全0+1',
  `add_way` bigint COMMENT '客户来源',
  `group_id1` bigint COMMENT '组织架构-学部ID，关联 dw.dim_crm_organization 获取 department_name',
  `group_id2` bigint COMMENT '组织架构-团ID，关联 dw.dim_crm_organization 获取 regiment_name',
  `group_id3` bigint COMMENT '组织架构-主管组ID，关联 dw.dim_crm_organization 获取 heads_name',
  `group_id4` bigint COMMENT '组织架构-小组ID，关联 dw.dim_crm_organization 获取 team_name',
  `group_id0` bigint COMMENT '职场ID，分组织架构时筛选 IN (4,400,702) 限定电销职场',
  `is_repeated_exposure` boolean COMMENT '是否多次曝光，true=是 false=否，2025-07-26起有值，之前历史数据无法区分')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='',
  'serialization.format'='')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'tos://yc-data-platform/user/hive/warehouse/crm.db/contact_log'
TBLPROPERTIES (
  'STATS_GENERATED_VIA_STATS_TASK'='true',
  'bucketing_version'='2',
  'transient_lastDdlTime'='1739204879')
