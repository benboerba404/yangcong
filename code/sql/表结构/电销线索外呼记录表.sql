-- =====================================================
-- 电销外呼记录表 dw.fact_call_history
-- =====================================================
-- 【表粒度】
--   一个线索(info_uuid) + 一次外呼(action_id) = 一条记录
--   同一线索可能被多次外呼
--
-- 【统计口径】
--   外呼次数 = count(action_id)
--   外呼线索量 = count(distinct user_id)  ← 所有线索量统计都用 user_id 去重
--   接通次数 = count(case when is_connect=1 then action_id end)
--   有效接通次数 = count(case when is_valid_connect=1 then action_id end)
--
-- 【关联关系】
--   通过 info_uuid 关联线索表 aws.clue_info
--   通过 user_id 关联用户相关表
--
-- 【接通判断】
--   - is_connect=1：接通
--   - is_valid_connect=1：有效接通（通话时长>=10秒）
--
-- 【呼叫状态】(call_status)
--   - 外呼异常
--   - 坐席放弃
--   - 用户挂断
--   - 未接通
--   - 已接听
--
-- 【外呼结果状态】(call_state)
--   - dealing: 已接通
--   - notDeal: 未接通
--   - abnormal: 异常
--   - blackList: 黑名单
--   - leak: IVR放弃
--
-- 【外呼渠道】(channel_id)
--   - 1: 七陌
--   - 2: 天眼
--   - 34: 蘑谷云
--   - 38: 百川
--   - 39: 百悟
--   - 101: 智鱼
--   - 102: 天田
--
-- 【振铃时长计算口径】(deal_times)
--   - 外呼异常时：不计算
--   - 已接听时：call_start_time - created_at
--   - 其他状态：call_time_length - created_at
--
-- 【组织架构层级】
--   workplace_id(职场) → department_id(学部) → regiment_id(团) → heads_id(主管组) → team_id(小组) → worker_id(坐席)
--
-- 【分区字段】
--   day：日期分区（如 20260115）
-- =====================================================

CREATE EXTERNAL TABLE `dw`.`fact_call_history` (
  `info_uuid` string COMMENT '线索id（关联 aws.clue_info.info_uuid）',
  `action_id` string COMMENT '外呼记录唯一ID（主键）',
  `call_sheet_id` string COMMENT '三方系统唯一id',
  `channel_id` string COMMENT '呼叫渠道ID（1七陌/2天眼/34蘑谷云/101智鱼/38百川/39百悟/102天田）',
  `user_id` string COMMENT '用户id',
  `user_sk` int COMMENT '用户sk',
  `call_state` string COMMENT '外呼结果状态（dealing已接通/notDeal未接通/abnormal异常/blackList黑名单/leak IVR放弃）',
  `call_phone` string COMMENT '外呼号码',
  `called_phone` string COMMENT '被叫号码',
  `called_province` string COMMENT '被叫号码省份',
  `called_district` string COMMENT '被叫号码区县',
  `called_district_code` string COMMENT '被叫号码区县编码',
  `call_start_time` timestamp COMMENT '外呼开始时间（即接通时间）',
  `call_end_time` timestamp COMMENT '外呼结束时间（即挂断时间）',
  `call_time_length` int COMMENT '呼叫时长/通话时长（秒，>=10秒为有效接通）',
  `record_file_url` string COMMENT '通话录音文件地址',
  `record_file_ip` string COMMENT '通话录音文件服务器ip',
  `exten` string COMMENT '销售工号',
  `worker_id` string COMMENT '销售id',
  `agent_name` string COMMENT '销售名称',
  `user_type` string COMMENT '用户类型',
  `user_type_name` string COMMENT '用户类型',
  `clue_stage` string COMMENT '线索学段',
  `hang_up_part` string COMMENT '挂断方',
  `is_connect` smallint COMMENT '是否接通（1=接通，0=未接通）',
  `is_valid_connect` smallint COMMENT '是否有效接通（1=有效，通话时长>=10秒）',
  `department_id` string COMMENT '学部id',
  `regiment_id` string COMMENT '团id',
  `heads_id` string COMMENT '主管组id',
  `team_id` string COMMENT '小组id',
  `created_at` timestamp COMMENT '外呼创建时间（即系统拨号时间）',
  `updated_at` timestamp COMMENT '更新时间',
  `day` int,
  `clue_created_at` timestamp COMMENT '线索领取时间（冗余字段，来自线索表 created_at）',
  `workplace_id` int COMMENT '销售职场id',
  `call_status` string COMMENT '呼叫状态（外呼异常/坐席放弃/用户挂断/未接通/已接听）',
  `deal_times` int COMMENT '振铃时长（秒）；外呼异常时不计算，已接听时=call_start_time-created_at，其他状态=call_time_length-created_at'
) COMMENT '一个线索id一个action_id一条记录' ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/dw.db/fact_call_history' TBLPROPERTIES (
  'alias' = '坐席呼叫表',
  'bucketing_version' = '2',
  'last_modified_by' = 'liuguanxiong',
  'last_modified_time' = '1727248622',
  'spark.sql.create.version' = '2.2 or prior',
  'spark.sql.sources.schema' = '{"type":"struct","fields":[{"name":"info_uuid","type":"string","nullable":true,"metadata":{"comment":"线索id"}},{"name":"action_id","type":"string","nullable":true,"metadata":{"comment":"唯一ID"}},{"name":"call_sheet_id","type":"string","nullable":true,"metadata":{"comment":"三方系统唯一id"}},{"name":"channel_id","type":"string","nullable":true,"metadata":{"comment":"呼叫渠道ID"}},{"name":"user_id","type":"string","nullable":true,"metadata":{"comment":"用户id"}},{"name":"user_sk","type":"integer","nullable":true,"metadata":{"comment":"用户sk"}},{"name":"call_state","type":"string","nullable":true,"metadata":{"comment":"外呼结果状态"}},{"name":"call_phone","type":"string","nullable":true,"metadata":{"comment":"外呼号码"}},{"name":"called_phone","type":"string","nullable":true,"metadata":{"comment":"被叫号码"}},{"name":"called_province","type":"string","nullable":true,"metadata":{"comment":"被叫号码省份"}},{"name":"called_district","type":"string","nullable":true,"metadata":{"comment":"被叫号码区县"}},{"name":"called_district_code","type":"string","nullable":true,"metadata":{"comment":"被叫号码区县编码"}},{"name":"call_start_time","type":"timestamp","nullable":true,"metadata":{"comment":"外呼开始时间"}},{"name":"call_end_time","type":"timestamp","nullable":true,"metadata":{"comment":"外呼结束时间"}},{"name":"call_time_length","type":"integer","nullable":true,"metadata":{"comment":"呼叫时长"}},{"name":"record_file_url","type":"string","nullable":true,"metadata":{"comment":"通话录音文件地址"}},{"name":"record_file_ip","type":"string","nullable":true,"metadata":{"comment":"通话录音文件服务器ip"}},{"name":"exten","type":"string","nullable":true,"metadata":{"comment":"销售工号"}},{"name":"worker_id","type":"string","nullable":true,"metadata":{"comment":"销售id"}},{"name":"agent_name","type":"string","nullable":true,"metadata":{"comment":"销售名称"}},{"name":"user_type","type":"string","nullable":true,"metadata":{"comment":"用户类型"}},{"name":"user_type_name","type":"string","nullable":true,"metadata":{"comment":"用户类型"}},{"name":"clue_stage","type":"string","nullable":true,"metadata":{"comment":"线索学段"}},{"name":"hang_up_part","type":"string","nullable":true,"metadata":{"comment":"挂断方"}},{"name":"is_connect","type":"short","nullable":true,"metadata":{"comment":"是否接通"}},{"name":"is_valid_connect","type":"short","nullable":true,"metadata":{"comment":"是否有效接通"}},{"name":"department_id","type":"string","nullable":true,"metadata":{"comment":"学部id"}},{"name":"regiment_id","type":"string","nullable":true,"metadata":{"comment":"团id"}},{"name":"heads_id","type":"string","nullable":true,"metadata":{"comment":"主管组id"}},{"name":"team_id","type":"string","nullable":true,"metadata":{"comment":"小组id"}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{"comment":"创建时间"}},{"name":"updated_at","type":"timestamp","nullable":true,"metadata":{"comment":"更新时间"}},{"name":"day","type":"integer","nullable":true,"metadata":{}},{"name":"clue_created_at","type":"timestamp","nullable":true,"metadata":{"comment":"线索领取时间"}}]}',
  'transient_lastDdlTime' = '1770055983'
)