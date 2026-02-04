-- =====================================================
-- 电销订单表 aws.crm_order_info
-- =====================================================
-- 【表粒度】
--   一个订单 = 一条记录（order_id 唯一）
--
-- 【与全公司订单宽表的关系】⚠️ 重要
--   - 本表：仅电销业务订单
--   - 全公司订单宽表(dws.topic_order_detail)：全公司所有业务订单
--   
--   营收归属差异：
--   - 用户可能同时存在多个服务期（如电销+新媒体）
--   - 全公司表的 business_gmv_attribution 按优先级归属，双服务期可能归新媒体
--   - 因此：本表营收 ≠ 全公司表筛选电销后的营收
--
-- 【使用场景约定】
--   - 电销营收分析 → 使用本表
--   - 活跃转化分析 → 使用全公司订单宽表（dws.topic_order_detail）
--
-- 【统计口径】
--   订单量 = count(order_id)
--   营收金额 = sum(amount)
--   转化用户量 = count(distinct user_id)
--
-- 【关联线索表】
--   - recent_info_uuid：成交前该坐席最近一次领取的线索id
--     · 关联 aws.clue_info.info_uuid 可追溯线索来源
--   - modify_recent_info_uuid：矫正后的线索关联
--     · 成交前该坐席最近一次【非人工录入】来源领取的线索id
--     · 若该坐席对此用户只有人工录入记录，则取人工录入来源
--   - first_clue_source：成单用户首次被领取时的线索来源
--
-- 【常用筛选条件】
--   - status = '支付成功'  -- 支付成功订单
--   - is_test = false  -- 排除测试订单
--   - in_salary = 1  -- 计入工资的订单
--   - workplace_id in (4, 400, 702)  -- 电销职场
--   - regiment_id not in (0, 303, 546)  -- 排除特定团组
--   - worker_id <> 0  -- 有坐席
--
-- 【金额字段】
--   - amount：订单金额（★ 营收统计使用此字段）
--   - real_amount：实付金额
--   - original_amount：订单原价
--
-- 【商品分类体系】
--   - good_kind_name_level_1/2/3：商品类目（一/二/三级）
--   - business_good_kind_name_level_1/2/3：策略组修正后的商品类目
--   - good_type：已弃用，推荐使用 good_kind_name_level_2
--
-- 【组织架构层级】
--   workplace_id(职场) → department_id(学部) → regiment_id(团) → heads_id(主管组) → team_id(小组) → worker_id(坐席)
-- =====================================================

CREATE TABLE
  `aws`.`crm_order_info` (
    `order_id` string COMMENT '订单id（主键）',
    `user_id` string COMMENT '用户id（转化用户量用此字段去重）',
    `user_sk` string COMMENT '用户sk',
    `worker_id` int COMMENT '坐席id',
    `order_created_time` timestamp COMMENT '订单创建时间',
    `pay_time` timestamp COMMENT '支付时间',
    `order_type` smallint COMMENT '订单类型 1新增 2付费',
    `amount` double COMMENT '订单金额',
    `sync_type` smallint COMMENT '同步方式：1-自动判单>，2-申诉, 3-七陌导入, 4-专属链接',
    `sync_status` smallint COMMENT '同步状态：1-正常，2->异常',
    `status` string COMMENT '订单状态（常用筛选：支付成功）',
    `stage` string COMMENT '下单年级',
    `department_id` int COMMENT '学部id',
    `regiment_id` int COMMENT '团id（常用筛选：排除0、303、546，关联dw.dim_crm_organization获取团名称）',
    `heads_id` int COMMENT '主管组id',
    `team_id` int COMMENT '小组id',
    `sell_from` string COMMENT '商品售卖来源',
    `created_at` timestamp COMMENT '创建日期',
    `updated_at` timestamp COMMENT '更新日期',
    `is_pad` int COMMENT '是否包含pad的订单',
    `pad_name` string COMMENT '订单包含pad的名字',
    `business_attribution` string COMMENT '业务群归属：b 端营收、小学网课营收、轻课营收',
    `worker_name` string COMMENT '坐席名称',
    `good_id` string COMMENT '商品id',
    `good_name` string COMMENT '商品名称',
    `is_pad_price_difference_order` smallint COMMENT '是否体验机补差价订单',
    `first_clue_source` string COMMENT '成单用户首次被领取时线索来源',
    `recent_info_uuid` string COMMENT '成交前该坐席最近一次领取的线索id（关联 aws.clue_info.info_uuid）',
    `workplace_id` int COMMENT '销售职场id（常用筛选：4、400、702为电销职场）',
    `business_gmv_attribution` string COMMENT '业务GMV归属划分',
    `real_amount` double COMMENT '订单实付金额',
    `good_sell_kind` string COMMENT '商品售卖类型',
    `good_year` string COMMENT '商品时长',
    `model_type_array` array < string > COMMENT '订单平板型号',
    `original_amount` double COMMENT '订单原价',
    `xugou_order_kind` string COMMENT '续购订单类型',
    `xugou_pre_order_id` string COMMENT '续购前序订单id',
    `dynamic_diff_price_type` string COMMENT '补差价类型',
    `good_original_amount` double COMMENT '商品原价',
    `modify_recent_info_uuid` string COMMENT '矫正线索id：成交前该坐席最近一次非人工录入来源领取的记录，若只有人工录入则取人工录入',
    `team_ids` array < string > COMMENT '全域业绩归属',
    `team_names` array < string > COMMENT '全域业绩归属',
    `good_category` string COMMENT '商品类别',
    `sku_group_good_id` string COMMENT 'sku商品组id',
    `group` array < string > COMMENT '商品标签',
    `in_salary` smallint COMMENT '是否计入工资（常用筛选：1=计入）',
    `salary_threshold` string COMMENT '订单计入工资的金额阈值',
    `good_type` string COMMENT '商品类型(已弃用,推荐使用dw.fact_order_detail:good_kind_name_level_2)',
    `pad_type` string COMMENT '平板类型',
    `kind_array` array < string > COMMENT '子商品的类型-数组',
    `good_biz_type` string COMMENT '商品业务类型',
    `mid_grade` string COMMENT '中学修正年级',
    `mid_stage_name` string COMMENT '中学修正年级',
    `gender` string COMMENT '用户性别',
    `regist_time` timestamp COMMENT '注册时间',
    `province` string COMMENT '省',
    `city` string COMMENT '市',
    `city_class` string COMMENT '城市分线',
    `interest_subsidy_method` string COMMENT '贴息方式',
    `hire_purchase_commission` double COMMENT '分期手续费',
    `user_pay_status_statistics` string COMMENT '付费标签：统计维度口径',
    `user_pay_status_business` string COMMENT '付费标签：业务维度口径',
    `business_user_pay_status_statistics` string COMMENT '商业化付费会员拆分为大会员付费、非大会员付费',
    `business_user_pay_status_business` string COMMENT '付费分层-业务维度',
    `good_kind_name_level_1` string COMMENT '商品类目-一级',
    `good_kind_name_level_2` string COMMENT '商品类目-二级',
    `good_kind_name_level_3` string COMMENT '商品类目-三级',
    `good_kind_id_level_1` string COMMENT '商品类目-一级id',
    `good_kind_id_level_2` string COMMENT '商品类目-二级id',
    `good_kind_id_level_3` string COMMENT '商品类目-三级id',
    `fix_good_kind_id_level_2` string COMMENT '修正-商品类目-二级id(积木块抵扣「升单商品」专用)',
    `fix_good_kind_name_level_2` string COMMENT '修正-商品类目-二级(积木块抵扣「升单商品」专用)',
    `is_clue_seat` smallint COMMENT '线索是否在坐席名下',
    `business_good_kind_name_level_1` string COMMENT '策略组修正-商品类目-一级',
    `business_good_kind_name_level_2` string COMMENT '策略组修正-商品类目-二级',
    `business_good_kind_name_level_3` string COMMENT '策略组修正-商品类目-三级',
    `fix_deductible_price` double COMMENT '修正-补差价总金额',
    `is_test` boolean COMMENT '是否是测试订单（常用筛选：false=正式订单）',
    `fix_good_year` string COMMENT '修正的商品时长',
    `course_timing_kind` string COMMENT '商品分类标签',
    `course_group_kind` string COMMENT '商品分组标签',
    `strategy_type` string COMMENT '策略类型:20260101上线以后为业务数据，之前按规则清洗',
    `strategy_detail` string COMMENT '策略明细：策略及对应的金额明细'
  ) COMMENT '一个订单一条记录' ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/aws.db/crm_order_info' TBLPROPERTIES (
    'alias' = '电销订单表',
    'bucketing_version' = '2',
    'last_modified_by' = 'finebi',
    'last_modified_time' = '1749699283',
    'spark.sql.create.version' = '2.2 or prior',
    'spark.sql.sources.schema.numParts' = '3',
    'spark.sql.sources.schema.part.0' = '{"type":"struct","fields":[{"name":"order_id","type":"string","nullable":true,"metadata":{"comment":"订单id"}},{"name":"user_id","type":"string","nullable":true,"metadata":{"comment":"用户id"}},{"name":"user_sk","type":"string","nullable":true,"metadata":{"comment":"用户sk"}},{"name":"worker_id","type":"integer","nullable":true,"metadata":{"comment":"坐席id"}},{"name":"order_created_time","type":"timestamp","nullable":true,"metadata":{"comment":"订单创建时间"}},{"name":"pay_time","type":"timestamp","nullable":true,"metadata":{"comment":"支付时间"}},{"name":"order_type","type":"short","nullable":true,"metadata":{"comment":"订单类型 1新增 2付费"}},{"name":"amount","type":"double","nullable":true,"metadata":{"comment":"订单金额"}},{"name":"sync_type","type":"short","nullable":true,"metadata":{"comment":"同步方式：1-自动判单>，2-申诉, 3-七陌导入, 4-专属链接"}},{"name":"sync_status","type":"short","nullable":true,"metadata":{"comment":"同步状态：1-正常，2->异常"}},{"name":"status","type":"string","nullable":true,"metadata":{"comment":"订单状态"}},{"name":"stage","type":"string","nullable":true,"metadata":{"comment":"下单年级"}},{"name":"department_id","type":"integer","nullable":true,"metadata":{"comment":"学部id"}},{"name":"regiment_id","type":"integer","nullable":true,"metadata":{"comment":"团id"}},{"name":"heads_id","type":"integer","nullable":true,"metadata":{"comment":"主管组id"}},{"name":"team_id","type":"integer","nullable":true,"metadata":{"comment":"小组id"}},{"name":"sell_from","type":"string","nullable":true,"metadata":{"comment":"商品售卖来源"}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{"comment":"创建日期"}},{"name":"updated_at","type":"timestamp","nullable":true,"metadata":{"comment":"更新日期"}},{"name":"is_pad","type":"integer","nullable":true,"metadata":{"comment":"是否包含pad的订单"}},{"name":"pad_name","type":"string","nullable":true,"metadata":{"comment":"订单包含pad的名字"}},{"name":"business_attribution","type":"string","nullable":true,"metadata":{"comment":"业务群归属：b 端营收、小学网课营收、轻课营收"}},{"name":"worker_name","type":"string","nullable":true,"metadata":{"comment":"坐席名称"}},{"name":"good_id","type":"string","nullable":true,"metadata":{"comment":"商品id"}},{"name":"good_name","type":"string","nullable":true,"metadata":{"comment":"商品名称"}},{"name":"is_pad_price_difference_order","type":"short","nullable":true,"metadata":{"comment":"是否体验机补差价订单"}},{"name":"first_clue_source","type":"string","nullable":true,"metadata":{"comment":"成单用户首次被领取时线索来源"}},{"name":"recent_info_uuid","type":"string","nullable":true,"metadata":{"comment":"成单用户最近一次被领取的info_uuid"}},{"name":"workplace_id","type":"integer","nullable":true,"metadata":{"comment":"销售职场id"}},{"name":"business_gmv_attribution","type":"string","nullable":true,"metadata":{"comment":"业务GMV归属划分"}},{"name":"real_amount","type":"double","nullable":true,"metadata":{"comment":"订单实付金额"}},{"name":"good_sell_kind","type":"string","nullable":true,"metadata":{"comment":"商品售卖类型"}},{"name":"good_year","type":"string","nullable":true,"metadata":{"comment":"商品时长"}},{"name":"model_type_array","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"订单平板型号"}},{"name":"original_amount","type":"double","nullable":true,"metadata":{"comment":"订单原价"}},{"name":"xugou_order_kind","type":"string","nullable":true,"metadata":{"comment":"续购订单类型"}},{"name":"xugou_pre_order_id","type":"string","nullable":true,"metadata":{"comment":"续购前序订单id"}},{"name":"dynamic_diff_price_type","type":"string","nullable":true,"metadata":{"comment":"补差价类型"}},{"name":"good_original_amount","type":"double","nullable":true,"metadata":{"comment":"商品原价"}},{"name":"modify_recent_info_uuid","type":"string","nullable":true,"metadata":{"comment":"矫正成单用户最近一次被领取的info_uuid"}},{"name":"team_ids","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"全域业绩归属"}},{"name":"team_names","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"全域业绩归属"}},{"name":"good_category",',
    'spark.sql.sources.schema.part.1' = '"type":"string","nullable":true,"metadata":{"comment":"商品类别"}},{"name":"sku_group_good_id","type":"string","nullable":true,"metadata":{"comment":"sku商品组id"}},{"name":"group","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"商品标签"}},{"name":"in_salary","type":"short","nullable":true,"metadata":{"comment":"是否计入工资"}},{"name":"salary_threshold","type":"string","nullable":true,"metadata":{"comment":"订单计入工资的金额阈值"}},{"name":"good_type","type":"string","nullable":true,"metadata":{"comment":"商品类型(已弃用,推荐使用dw.fact_order_detail:good_kind_name_level_2)"}},{"name":"pad_type","type":"string","nullable":true,"metadata":{"comment":"平板类型"}},{"name":"kind_array","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"子商品的类型-数组"}},{"name":"good_biz_type","type":"string","nullable":true,"metadata":{"comment":"商品业务类型"}},{"name":"mid_grade","type":"string","nullable":true,"metadata":{"comment":"中学修正年级"}},{"name":"mid_stage_name","type":"string","nullable":true,"metadata":{"comment":"中学修正年级"}},{"name":"gender","type":"string","nullable":true,"metadata":{"comment":"用户性别"}},{"name":"regist_time","type":"timestamp","nullable":true,"metadata":{"comment":"注册时间"}},{"name":"province","type":"string","nullable":true,"metadata":{"comment":"省"}},{"name":"city","type":"string","nullable":true,"metadata":{"comment":"市"}},{"name":"city_class","type":"string","nullable":true,"metadata":{"comment":"城市分线"}},{"name":"interest_subsidy_method","type":"string","nullable":true,"metadata":{"comment":"贴息方式"}},{"name":"hire_purchase_commission","type":"double","nullable":true,"metadata":{"comment":"分期手续费"}},{"name":"user_pay_status_statistics","type":"string","nullable":true,"metadata":{"comment":"付费标签：统计维度口径"}},{"name":"user_pay_status_business","type":"string","nullable":true,"metadata":{"comment":"付费标签：业务维度口径"}},{"name":"business_user_pay_status_statistics","type":"string","nullable":true,"metadata":{"comment":"商业化付费会员拆分为大会员付费、非大会员付费"}},{"name":"business_user_pay_status_business","type":"string","nullable":true,"metadata":{"comment":"付费分层-业务维度"}},{"name":"good_kind_name_level_1","type":"string","nullable":true,"metadata":{"comment":"商品类目-一级"}},{"name":"good_kind_name_level_2","type":"string","nullable":true,"metadata":{"comment":"商品类目-二级"}},{"name":"good_kind_name_level_3","type":"string","nullable":true,"metadata":{"comment":"商品类目-三级"}},{"name":"good_kind_id_level_1","type":"string","nullable":true,"metadata":{"comment":"商品类目-一级id"}},{"name":"good_kind_id_level_2","type":"string","nullable":true,"metadata":{"comment":"商品类目-二级id"}},{"name":"good_kind_id_level_3","type":"string","nullable":true,"metadata":{"comment":"商品类目-三级id"}},{"name":"fix_good_kind_id_level_2","type":"string","nullable":true,"metadata":{"comment":"修正-商品类目-二级id(积木块抵扣「升单商品」专用)"}},{"name":"fix_good_kind_name_level_2","type":"string","nullable":true,"metadata":{"comment":"修正-商品类目-二级(积木块抵扣「升单商品」专用)"}},{"name":"is_clue_seat","type":"short","nullable":true,"metadata":{"comment":"线索是否在坐席名下"}},{"name":"business_good_kind_name_level_1","type":"string","nullable":true,"metadata":{"comment":"策略组修正-商品类目-一级"}},{"name":"business_good_kind_name_level_2","type":"string","nullable":true,"metadata":{"comment":"策略组修正-商品类目-二级"}},{"name":"business_good_kind_name_level_3","type":"string","nullable":true,"metadata":{"comment":"策略组修正-商品类目-三级"}},{"name":"fix_deductible_price","type":"double","nullable":true,"metadata":{"comment":"修正-补差价总金额"}},{"name":"is_test","type":"boolean","nullable":true,"metadata":{"comment":"是否是测试订单"}},{"name":"fix_good_year","type":"string","nullable":true,"metadata":{"comment":"修正的商品时长"}},{"name":"course_timing_kind","type":"string","nullable":true,"metadata":{"comment":"商品分类标签"}},{"name":"course_group_kind","type":"string","nullable":true,"metadata":{"comment":"商品分组标签"}},{"name":"strategy_type","type":"string","nullable":true,"metadata":{"comment":"策略类型:20260101上线以后为业务数据，之前按规则清洗"}},{"name":"strat',
    'spark.sql.sources.schema.part.2' = 'egy_detail","type":"string","nullable":true,"metadata":{"comment":"策略明细：策略及对应的金额明细"}}]}',
    'transient_lastDdlTime' = '1770065714'
  )