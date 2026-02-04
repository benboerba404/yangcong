-- =====================================================
-- 全公司订单宽表 dws.topic_order_detail
-- =====================================================
-- 【表粒度】
--   一笔子订单 = 一条记录（order_id + sub_good_sk 唯一）
--
-- 【与电销订单表的关系】⚠️ 重要
--   - 本表：全公司所有业务的订单（电销、新媒体、入校、体验营等）
--   - 电销订单表(aws.crm_order_info)：仅电销业务订单
--   
--   营收归属差异：
--   - 用户可能同时存在多个服务期（如电销+新媒体）
--   - business_gmv_attribution 按优先级归属到某一业务
--   - 双服务期用户的营收可能归属新媒体，不算电销
--   - 因此：本表电销营收 ≠ 电销订单表营收
--
-- 【使用场景约定】
--   - 活跃转化分析 → 使用本表（全量订单）
--   - 电销营收分析 → 使用电销订单表
--   - 判断用户是否购买过某种商品 → 使用本表（全量订单）⚠️ 强制
--     · 原因：用户可能通过多渠道购买，仅查单一业务表会遗漏购买记录
--     · 常用字段：good_kind_name_level_2（商品二级类目）、good_name、good_id
--
-- 【统计口径】
--   订单量 = count(distinct order_id)  -- 注意去重，因为有子订单拆分
--   营收金额（两种方式）：
--     · 订单去重后：sum(order_amount)
--     · 子订单粒度：sum(sub_amount)
--   转化用户量 = count(distinct u_user)
--
-- 【业务归属字段】
--   - business_gmv_attribution：业务GMV归属划分（★ 统一使用此字段）
--   - business_attribution：业务群归属（b端营收、小学网课营收、轻课营收）
--   - attribution：B/C订单归属
--   - user_attribution：用户活跃时归属
--
-- 【金额字段】
--   - order_amount：订单实收金额（订单去重后用此字段）
--   - sub_amount：子商品实收金额（子订单粒度用此字段）
--   - original_amount：订单原价
--   - arrival_amount：到账金额
--
-- 【电销相关字段】✓ 已验证（与电销订单表一致）
--   - workplace_id/regiment_id/worker_id：组织架构字段（100%匹配）
--   - is_clue_seat：线索是否在坐席名下（99.92%匹配）
--   - is_telemarketing_user：是否电销触达用户
--
-- 【常用筛选条件】
--   - status = '支付成功'
--   - is_test_user = 0  -- 排除测试用户
--   - order_amount >= 39  -- 正价订单（常用，is_normal_price 不常用）
-- =====================================================

CREATE EXTERNAL TABLE `dws`.`topic_order_detail` (
  `order_id` string COMMENT '订单业务id',
  `create_time` timestamp COMMENT '源系统创建条目的时间',
  `create_time_sk` string,
  `paid_time` timestamp COMMENT '支付时间',
  `paid_time_sk` int COMMENT '支付时间sk',
  `is_normal_price` smallint COMMENT '是否正价订单',
  `is_group_buy` smallint COMMENT '是否团购订单',
  `is_by_manual` smallint COMMENT '是否是手工订单',
  `business_group` string COMMENT '业务群',
  `add_time_ms` bigint COMMENT '增加的服务时长',
  `add_time_day` int COMMENT '增加的服务时长（天）',
  `product_id` string COMMENT '产品id',
  `payment_channel` string COMMENT '支付渠道，微信支付、支付宝支付、银联',
  `app_channel` string COMMENT '创建订单的App的下载渠道，苹果是appstore',
  `payment_platform` string COMMENT '支付平台[ping++',
  `client_os` string COMMENT '设备类型',
  `is_recalled` smallint COMMENT '退款后是否回收权益',
  `shop_name` string COMMENT '推广来源',
  `shop_id` string COMMENT '推广来源id',
  `shop_detail_id` string COMMENT '推广来源明细id',
  `shop_detail_name` string COMMENT '推广来源明细名称',
  `discount_amount` double COMMENT '优惠金额',
  `service_amount` double COMMENT '服务费',
  `procedures_amount` double COMMENT '手续费',
  `arrival_amount` double COMMENT '到账金额',
  `register_pay_duration` int COMMENT '付费周期 （支付日期-注册日期）',
  `order_sn` int COMMENT '用户在当前下单属于第几次购买',
  `normal_price_order_sn` int COMMENT '用户在当前下单属于第几次正价购买',
  `channel_normal_price_order_sn` int COMMENT '用户在当前下单渠道属于第几次正价购买',
  `good_sk` int COMMENT '商品代理键',
  `good_name` string COMMENT '商品名',
  `good_stage_subject` string COMMENT '商品【学科-学段-类型】数组',
  `sub_good_cnt` int COMMENT '子商品的个数',
  `good_subject_cnt` int COMMENT '学科数',
  `sub_good_sk` int COMMENT '子商品代理键',
  `kind` string COMMENT '子商品的类型，英文[vip',
  `stage_id` int COMMENT '学段',
  `stage_name` string COMMENT '学段名',
  `subject_id` int COMMENT '学科',
  `subject_name` string COMMENT '学科名',
  `semester_id` int COMMENT '学期',
  `semester_name` string COMMENT '学期名',
  `good_original_amount` double COMMENT '商品原价',
  `sub_amount` double COMMENT '子商品实收金额',
  `ss_order_sn` int COMMENT '用户当前学段学科在当前下单属于第几次购买',
  `ss_normal_price_order_sn` int COMMENT '用户当前学段学科在当前下单属于第几次正价购买',
  `before_order_last_end_time` timestamp COMMENT '购买订单前权限到期日期',
  `end_pay_duration` int COMMENT '用户当前学段学科订单支付日期和权益截止日期差值（天数）',
  `stage_order_sn` int COMMENT '用户当前学段在当前下单属于第几次购买',
  `stage_normal_price_order_sn` int COMMENT '用户当前学段在当前下单属于第几次正价购买',
  `user_sk` int COMMENT '用户代理键',
  `u_user` string COMMENT '用户id',
  `role` string COMMENT '用户角色',
  `grade` string COMMENT '用户填写年级',
  `mid_stage_name` string COMMENT '中学修正学段',
  `gender` string COMMENT '用户性别',
  `user_attribution` string COMMENT '用户活跃时归属',
  `attribution` string COMMENT 'B/C订单归属',
  `city_class` string COMMENT '城市分线',
  `province` string COMMENT '省',
  `province_code` string COMMENT '省code',
  `city` string COMMENT '市',
  `city_code` string COMMENT '市code',
  `area` string COMMENT '区',
  `area_code` string COMMENT '区code',
  `school_id` string COMMENT '学校id',
  `school_sk` int COMMENT '学校sk',
  `school_id1` string COMMENT '学校id1',
  `school_sk1` int COMMENT '学校sk1',
  `is_test_user` smallint COMMENT '是否测试用户',
  `is_teach_user` smallint COMMENT '是否教学班用户',
  `is_room_user` smallint COMMENT '是否有班用户',
  `is_new_user` smallint COMMENT '是否新用户',
  `is_telemarketing_user` smallint COMMENT '是否电销触达用户（1=是）',
  `regist_time` timestamp COMMENT '用户注册时间',
  `is_parent_telemarketing` smallint COMMENT '是否属于家长电销订单',
  `group` array < string > COMMENT '标签',
  `good_sell_kind` string COMMENT '商品售卖类型',
  `sell_from` string COMMENT '商品售卖来源',
  `is_pad_price_difference_order` smallint COMMENT '是否体验机补差价订单',
  `light_class_before_sum_amount` int COMMENT '轻课营收之前的金额',
  `light_class_sum_amount` int COMMENT '轻课营收加上本次的金额',
  `before_sum_amount` int COMMENT '订单之前的金额',
  `sum_amount` int COMMENT '订单加上这次的金额',
  `user_pay_status_statistics` string COMMENT '付费标签：统计维度口径 ',
  `user_pay_status_business` string COMMENT '付费标签：业务维度口径',
  `business_attribution` string COMMENT '业务群归属：b 端营收、小学网课营收、轻课营收',
  `original_amount` double COMMENT '订单原价',
  `mid_grade` string COMMENT '中学年级',
  `status` string COMMENT '当前订单状态',
  `good_id` string COMMENT '商品id',
  `dynamic_diff_price_type` string COMMENT '补差价类型',
  `good_year` string COMMENT '商品时长',
  `good_content` string COMMENT '内容标识',
  `xugou_order_kind` string COMMENT '续购订单类型',
  `xugou_pre_order_id` string COMMENT '续购前序订单id',
  `discount_id` string COMMENT '优惠券id',
  `discount_note` string COMMENT '优惠券note',
  `discount_price` double COMMENT '优惠券金额（元）',
  `special_course_type` string COMMENT '课程包类型',
  `business_gmv_attribution` string COMMENT '业务GMV归属划分（按服务期优先级判断，双服务期用户可能归属非电销）',
  `sync_type` smallint COMMENT '同步方式：1-自动判单>，2-申诉, 3-七陌导入, 4-专属链接',
  `sync_status` smallint COMMENT '同步状态：1->正常，2->异常',
  `model_type` string COMMENT '平板型号',
  `order_amount` double COMMENT '订单实收金额（★ 营收统计使用此字段）',
  `order_first_refund_time` timestamp COMMENT '订单首次退费时间',
  `sku_amount` double COMMENT 'sku 价格',
  `sku_name` string COMMENT 'sku名字',
  `procedures_rate` double COMMENT '手续费率',
  `workplace_id` int COMMENT '销售职场id',
  `department_id` int COMMENT '学部id',
  `regiment_id` int COMMENT '团id',
  `heads_id` int COMMENT '主管组id',
  `team_id` int COMMENT '小组id',
  `worker_id` int COMMENT '坐席id',
  `worker_name` string COMMENT '坐席名称',
  `insurance_category` string COMMENT '保险类别',
  `sn` string COMMENT 'pad sn',
  `yc_from` string COMMENT '机构名称',
  `total_refund_amt` double COMMENT '总退款金额',
  `team_ids` array < string > COMMENT '全域业绩归属',
  `team_names` array < string > COMMENT '全域业绩归属',
  `good_category` string COMMENT '商品类别',
  `platform_id` string COMMENT '平台ID',
  `business_id` string COMMENT '商户ID',
  `tiktok_author_id` string COMMENT '抖音订单直播主播id',
  `tiktok_author_name` string COMMENT '抖音订单直播主播名称',
  `tiktok_first_author_name` string COMMENT '抖音订单直播主播首次名称',
  `good_type` string COMMENT '商品类型(已弃用,推荐使用good_kind_name_level_2)',
  `phone` string COMMENT '手机号',
  `hire_purchase_num` int COMMENT '分期数',
  `interest_subsidy_method` string COMMENT '贴息方式',
  `hire_purchase_commission` double COMMENT '分期手续费',
  `real_deductible_price` double COMMENT '补差总金额',
  `deduct_category` string COMMENT '补差类型',
  `business_user_pay_status_statistics` string COMMENT '商业化付费会员拆分为大会员付费、非大会员付费',
  `regist_user_allocation` array < string > COMMENT '用户注册当天服务期归属',
  `business_user_pay_status_business` string COMMENT '付费分层-业务维度',
  `correct_team_names` array < string > COMMENT '修正后业绩归属',
  `pad_type` string COMMENT '平板类型',
  `live_platform_tag` string COMMENT '直播平台标签',
  `actual_deduct_amount` double COMMENT '用户实际的抵扣金额，单位：元',
  `max_deduct_amount` double COMMENT '该策略的最高抵扣金额，单位：元',
  `grade_stage_name_day` string COMMENT '付费当天年级学段',
  `grade_name_month` string COMMENT '付费当月年级',
  `stage_name_month` string COMMENT '付费当月学段',
  `grade_stage_name_month` string COMMENT '付费当月年级学段',
  `user_pay_status_statistics_month` string COMMENT '付费当月首次标签新增(统计日期当天注册的)、付费(统计日期之前买过正价课)、老未(统计日期之前注册的)',
  `user_pay_status_business_month` string COMMENT '付费当月首次付费标签 付费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)',
  `business_user_pay_status_statistics_month` string COMMENT '付费当月首次付费标签新增(统计日期当天注册的)、大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、老未(统计日期之前注册的)',
  `business_user_pay_status_business_month` string COMMENT '付费当月首次付费标签大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)',
  `grade_name_year` string COMMENT '付费当年年级',
  `stage_name_year` string COMMENT '付费当年学段',
  `grade_stage_name_year` string COMMENT '付费当年年级学段',
  `user_pay_status_statistics_year` string COMMENT '付费当年首次付费标签：新增(统计日期当天注册的)、付费(统计日期之前买过正价课)、老未(统计日期之前注册的)',
  `user_pay_status_business_year` string COMMENT '付费当年首次付费标签：付费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)',
  `business_user_pay_status_statistics_year` string COMMENT '付费当年首次付费标签： 新增(统计日期当天注册的)、大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、老未(统计日期之前注册的)',
  `business_user_pay_status_business_year` string COMMENT '付费当年首次付费标签：大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)',
  `good_stage_subject_cnt` int COMMENT '学段学科个数',
  `sku_group_good_id` string COMMENT 'sku商品组id',
  `good_kind_name_level_1` string COMMENT '商品类目-一级',
  `good_kind_name_level_2` string COMMENT '商品类目-二级',
  `good_kind_name_level_3` string COMMENT '商品类目-三级',
  `good_kind_id_level_1` string COMMENT '商品类目-一级id',
  `good_kind_id_level_2` string COMMENT '商品类目-二级id',
  `good_kind_id_level_3` string COMMENT '商品类目-三级id',
  `fix_good_kind_id_level_2` string COMMENT '修正-商品类目-二级id(积木块抵扣「升单商品」专用)',
  `fix_good_kind_name_level_2` string COMMENT '修正-商品类目-二级(积木块抵扣「升单商品」专用)',
  `is_clue_seat` smallint COMMENT '线索是否在坐席名下',
  `fix_good_year` string COMMENT '修正的商品时长',
  `business_good_kind_name_level_1` string COMMENT '策略组修正-商品类目-一级',
  `business_good_kind_name_level_2` string COMMENT '策略组修正-商品类目-二级',
  `business_good_kind_name_level_3` string COMMENT '策略组修正-商品类目-三级',
  `fix_deductible_price` double COMMENT '修正-补差价总金额',
  `course_timing_kind` string COMMENT '商品分类标签',
  `course_group_kind` string COMMENT '商品分组标签',
  `strategy_type` string COMMENT '策略类型:20260101上线以后为业务数据，之前按规则清洗',
  `strategy_detail` string COMMENT '策略明细：策略及对应的金额明细',
  `userauth_exchange_time` timestamp COMMENT '授权转换时间',
  `delay_vip_activation_time` timestamp COMMENT '囤课品激活时间',
  `multi_child_refund_time` timestamp COMMENT '多孩策略退差价时间'
) COMMENT '一笔子订单一条记录' ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/dws.db/topic_order_detail' TBLPROPERTIES (
  'alias' = '订单宽表',
  'bucketing_version' = '2',
  'discover.partitions' = 'true',
  'is_core' = 'true',
  'last_modified_by' = 'finebi',
  'last_modified_time' = '1749699296',
  'spark.sql.create.version' = '3.2.1',
  'spark.sql.sources.schema.numParts' = '5',
  'spark.sql.sources.schema.part.0' = '{"type":"struct","fields":[{"name":"order_id","type":"string","nullable":true,"metadata":{"comment":"订单业务id"}},{"name":"create_time","type":"timestamp","nullable":true,"metadata":{"comment":"源系统创建条目的时间"}},{"name":"create_time_sk","type":"string","nullable":true,"metadata":{}},{"name":"paid_time","type":"timestamp","nullable":true,"metadata":{"comment":"支付时间"}},{"name":"paid_time_sk","type":"integer","nullable":true,"metadata":{"comment":"支付时间sk"}},{"name":"is_normal_price","type":"short","nullable":true,"metadata":{"comment":"是否正价订单"}},{"name":"is_group_buy","type":"short","nullable":true,"metadata":{"comment":"是否团购订单"}},{"name":"is_by_manual","type":"short","nullable":true,"metadata":{"comment":"是否是手工订单"}},{"name":"business_group","type":"string","nullable":true,"metadata":{"comment":"业务群"}},{"name":"add_time_ms","type":"long","nullable":true,"metadata":{"comment":"增加的服务时长"}},{"name":"add_time_day","type":"integer","nullable":true,"metadata":{"comment":"增加的服务时长（天）"}},{"name":"product_id","type":"string","nullable":true,"metadata":{"comment":"产品id"}},{"name":"payment_channel","type":"string","nullable":true,"metadata":{"comment":"支付渠道，微信支付、支付宝支付、银联"}},{"name":"app_channel","type":"string","nullable":true,"metadata":{"comment":"创建订单的App的下载渠道，苹果是appstore"}},{"name":"payment_platform","type":"string","nullable":true,"metadata":{"comment":"支付平台[ping++"}},{"name":"client_os","type":"string","nullable":true,"metadata":{"comment":"设备类型"}},{"name":"is_recalled","type":"short","nullable":true,"metadata":{"comment":"退款后是否回收权益"}},{"name":"shop_name","type":"string","nullable":true,"metadata":{"comment":"推广来源"}},{"name":"shop_id","type":"string","nullable":true,"metadata":{"comment":"推广来源id"}},{"name":"shop_detail_id","type":"string","nullable":true,"metadata":{"comment":"推广来源明细id"}},{"name":"shop_detail_name","type":"string","nullable":true,"metadata":{"comment":"推广来源明细名称"}},{"name":"discount_amount","type":"double","nullable":true,"metadata":{"comment":"优惠金额"}},{"name":"service_amount","type":"double","nullable":true,"metadata":{"comment":"服务费"}},{"name":"procedures_amount","type":"double","nullable":true,"metadata":{"comment":"手续费"}},{"name":"arrival_amount","type":"double","nullable":true,"metadata":{"comment":"到账金额"}},{"name":"register_pay_duration","type":"integer","nullable":true,"metadata":{"comment":"付费周期 （支付日期-注册日期）"}},{"name":"order_sn","type":"integer","nullable":true,"metadata":{"comment":"用户在当前下单属于第几次购买"}},{"name":"normal_price_order_sn","type":"integer","nullable":true,"metadata":{"comment":"用户在当前下单属于第几次正价购买"}},{"name":"channel_normal_price_order_sn","type":"integer","nullable":true,"metadata":{"comment":"用户在当前下单渠道属于第几次正价购买"}},{"name":"good_sk","type":"integer","nullable":true,"metadata":{"comment":"商品代理键"}},{"name":"good_name","type":"string","nullable":true,"metadata":{"comment":"商品名"}},{"name":"good_stage_subject","type":"string","nullable":true,"metadata":{"comment":"商品【学科-学段-类型】数组"}},{"name":"sub_good_cnt","type":"integer","nullable":true,"metadata":{"comment":"子商品的个数"}},{"name":"good_subject_cnt","type":"integer","nullable":true,"metadata":{"comment":"学科数"}},{"name":"sub_good_sk","type":"integer","nullable":true,"metadata":{"comment":"子商品代理键"}},{"name":"kind","type":"string","nullable":true,"metadata":{"comment":"子商品的类型，英文[vip"}},{"name":"stage_id","type":"integer","nullable":true,"metadata":{"comment":"学段"}},{"name":"stage_name","type":"string","nullable":true,"metadata":{"comment":"学段名"}},{"name":"subject_id","type":"integer","nullable":true,"metadata":{"comment":"学科"}},{"name":"subject_name","type":"string","nullable":true,"metadata":{"comment":"学科名"}},{"name":"semester_id","type":"integer","nullable":true,"metadata":{"comment":"学期"}},{"name":"semester_name","type":"string","nullable":true,"metadata":{"comment":"学期名"}},{"name":"good_original_amount","type":"double","nullable":true,"metadata":{"comment":"商品原价"}},{"name":"sub_amount","type":"double","nullable":true,"metadata":{"comment":"子商品实收金额"}},{"name":"ss_order_',
  'spark.sql.sources.schema.part.1' = 'sn","type":"integer","nullable":true,"metadata":{"comment":"用户当前学段学科在当前下单属于第几次购买"}},{"name":"ss_normal_price_order_sn","type":"integer","nullable":true,"metadata":{"comment":"用户当前学段学科在当前下单属于第几次正价购买"}},{"name":"before_order_last_end_time","type":"timestamp","nullable":true,"metadata":{"comment":"购买订单前权限到期日期"}},{"name":"end_pay_duration","type":"integer","nullable":true,"metadata":{"comment":"用户当前学段学科订单支付日期和权益截止日期差值（天数）"}},{"name":"stage_order_sn","type":"integer","nullable":true,"metadata":{"comment":"用户当前学段在当前下单属于第几次购买"}},{"name":"stage_normal_price_order_sn","type":"integer","nullable":true,"metadata":{"comment":"用户当前学段在当前下单属于第几次正价购买"}},{"name":"user_sk","type":"integer","nullable":true,"metadata":{"comment":"用户代理键"}},{"name":"u_user","type":"string","nullable":true,"metadata":{"comment":"用户id"}},{"name":"role","type":"string","nullable":true,"metadata":{"comment":"用户角色"}},{"name":"grade","type":"string","nullable":true,"metadata":{"comment":"用户填写年级"}},{"name":"mid_stage_name","type":"string","nullable":true,"metadata":{"comment":"中学修正学段"}},{"name":"gender","type":"string","nullable":true,"metadata":{"comment":"用户性别"}},{"name":"user_attribution","type":"string","nullable":true,"metadata":{"comment":"用户活跃时归属"}},{"name":"attribution","type":"string","nullable":true,"metadata":{"comment":"B/C订单归属"}},{"name":"city_class","type":"string","nullable":true,"metadata":{"comment":"城市分线"}},{"name":"province","type":"string","nullable":true,"metadata":{"comment":"省"}},{"name":"province_code","type":"string","nullable":true,"metadata":{"comment":"省code"}},{"name":"city","type":"string","nullable":true,"metadata":{"comment":"市"}},{"name":"city_code","type":"string","nullable":true,"metadata":{"comment":"市code"}},{"name":"area","type":"string","nullable":true,"metadata":{"comment":"区"}},{"name":"area_code","type":"string","nullable":true,"metadata":{"comment":"区code"}},{"name":"school_id","type":"string","nullable":true,"metadata":{"comment":"学校id"}},{"name":"school_sk","type":"integer","nullable":true,"metadata":{"comment":"学校sk"}},{"name":"school_id1","type":"string","nullable":true,"metadata":{"comment":"学校id1"}},{"name":"school_sk1","type":"integer","nullable":true,"metadata":{"comment":"学校sk1"}},{"name":"is_test_user","type":"short","nullable":true,"metadata":{"comment":"是否测试用户"}},{"name":"is_teach_user","type":"short","nullable":true,"metadata":{"comment":"是否教学班用户"}},{"name":"is_room_user","type":"short","nullable":true,"metadata":{"comment":"是否有班用户"}},{"name":"is_new_user","type":"short","nullable":true,"metadata":{"comment":"是否新用户"}},{"name":"is_telemarketing_user","type":"short","nullable":true,"metadata":{"comment":"是否是电销触达"}},{"name":"regist_time","type":"timestamp","nullable":true,"metadata":{"comment":"用户注册时间"}},{"name":"is_parent_telemarketing","type":"short","nullable":true,"metadata":{"comment":"是否属于家长电销订单"}},{"name":"group","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"标签"}},{"name":"good_sell_kind","type":"string","nullable":true,"metadata":{"comment":"商品售卖类型"}},{"name":"sell_from","type":"string","nullable":true,"metadata":{"comment":"商品售卖来源"}},{"name":"is_pad_price_difference_order","type":"short","nullable":true,"metadata":{"comment":"是否体验机补差价订单"}},{"name":"light_class_before_sum_amount","type":"integer","nullable":true,"metadata":{"comment":"轻课营收之前的金额"}},{"name":"light_class_sum_amount","type":"integer","nullable":true,"metadata":{"comment":"轻课营收加上本次的金额"}},{"name":"before_sum_amount","type":"integer","nullable":true,"metadata":{"comment":"订单之前的金额"}},{"name":"sum_amount","type":"integer","nullable":true,"metadata":{"comment":"订单加上这次的金额"}},{"name":"user_pay_status_statistics","type":"string","nullable":true,"metadata":{"comment":"付费标签：统计维度口径 "}},{"name":"user_pay_status_business","type":"string","nullable":true,"metadata":{"comment":"付费标签：业务维度口径"}},{"name":"business_attribution","type":"string","nullable":true,"metadata":{"comment":"业务群归属：b 端营收、小学网课营收、轻课营收"}},{"',
  'spark.sql.sources.schema.part.2' = 'name":"original_amount","type":"double","nullable":true,"metadata":{"comment":"订单原价"}},{"name":"mid_grade","type":"string","nullable":true,"metadata":{"comment":"中学年级"}},{"name":"status","type":"string","nullable":true,"metadata":{"comment":"当前订单状态"}},{"name":"good_id","type":"string","nullable":true,"metadata":{"comment":"商品id"}},{"name":"dynamic_diff_price_type","type":"string","nullable":true,"metadata":{"comment":"补差价类型"}},{"name":"good_year","type":"string","nullable":true,"metadata":{"comment":"商品时长"}},{"name":"good_content","type":"string","nullable":true,"metadata":{"comment":"内容标识"}},{"name":"xugou_order_kind","type":"string","nullable":true,"metadata":{"comment":"续购订单类型"}},{"name":"xugou_pre_order_id","type":"string","nullable":true,"metadata":{"comment":"续购前序订单id"}},{"name":"discount_id","type":"string","nullable":true,"metadata":{"comment":"优惠券id"}},{"name":"discount_note","type":"string","nullable":true,"metadata":{"comment":"优惠券note"}},{"name":"discount_price","type":"double","nullable":true,"metadata":{"comment":"优惠券金额（元）"}},{"name":"special_course_type","type":"string","nullable":true,"metadata":{"comment":"课程包类型"}},{"name":"business_gmv_attribution","type":"string","nullable":true,"metadata":{"comment":"业务GMV归属划分"}},{"name":"sync_type","type":"short","nullable":true,"metadata":{"comment":"同步方式：1-自动判单>，2-申诉, 3-七陌导入, 4-专属链接"}},{"name":"sync_status","type":"short","nullable":true,"metadata":{"comment":"同步状态：1->正常，2->异常"}},{"name":"model_type","type":"string","nullable":true,"metadata":{"comment":"平板型号"}},{"name":"order_amount","type":"double","nullable":true,"metadata":{"comment":"订单实收金额"}},{"name":"order_first_refund_time","type":"timestamp","nullable":true,"metadata":{"comment":"订单首次退费时间"}},{"name":"sku_amount","type":"double","nullable":true,"metadata":{"comment":"sku 价格"}},{"name":"sku_name","type":"string","nullable":true,"metadata":{"comment":"sku名字"}},{"name":"procedures_rate","type":"double","nullable":true,"metadata":{"comment":"手续费率"}},{"name":"workplace_id","type":"integer","nullable":true,"metadata":{"comment":"销售职场id"}},{"name":"department_id","type":"integer","nullable":true,"metadata":{"comment":"学部id"}},{"name":"regiment_id","type":"integer","nullable":true,"metadata":{"comment":"团id"}},{"name":"heads_id","type":"integer","nullable":true,"metadata":{"comment":"主管组id"}},{"name":"team_id","type":"integer","nullable":true,"metadata":{"comment":"小组id"}},{"name":"worker_id","type":"integer","nullable":true,"metadata":{"comment":"坐席id"}},{"name":"worker_name","type":"string","nullable":true,"metadata":{"comment":"坐席名称"}},{"name":"insurance_category","type":"string","nullable":true,"metadata":{"comment":"保险类别"}},{"name":"sn","type":"string","nullable":true,"metadata":{"comment":"pad sn"}},{"name":"yc_from","type":"string","nullable":true,"metadata":{"comment":"机构名称"}},{"name":"total_refund_amt","type":"double","nullable":true,"metadata":{"comment":"总退款金额"}},{"name":"team_ids","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"全域业绩归属"}},{"name":"team_names","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"全域业绩归属"}},{"name":"good_category","type":"string","nullable":true,"metadata":{"comment":"商品类别"}},{"name":"platform_id","type":"string","nullable":true,"metadata":{"comment":"平台ID"}},{"name":"business_id","type":"string","nullable":true,"metadata":{"comment":"商户ID"}},{"name":"tiktok_author_id","type":"string","nullable":true,"metadata":{"comment":"抖音订单直播主播id"}},{"name":"tiktok_author_name","type":"string","nullable":true,"metadata":{"comment":"抖音订单直播主播名称"}},{"name":"tiktok_first_author_name","type":"string","nullable":true,"metadata":{"comment":"抖音订单直播主播首次名称"}},{"name":"good_type","type":"string","nullable":true,"metadata":{"comment":"商品类型(已弃用,推荐使用good_kind_name_level_2)"}},{"name":"phone","type":"string","nullable":true,"metadata":{"comment":"手机号"}},{"name":"hire_purchase_num","type":"integer",',
  'spark.sql.sources.schema.part.3' = '"nullable":true,"metadata":{"comment":"分期数"}},{"name":"interest_subsidy_method","type":"string","nullable":true,"metadata":{"comment":"贴息方式"}},{"name":"hire_purchase_commission","type":"double","nullable":true,"metadata":{"comment":"分期手续费"}},{"name":"real_deductible_price","type":"double","nullable":true,"metadata":{"comment":"补差总金额"}},{"name":"deduct_category","type":"string","nullable":true,"metadata":{"comment":"补差类型"}},{"name":"business_user_pay_status_statistics","type":"string","nullable":true,"metadata":{"comment":"商业化付费会员拆分为大会员付费、非大会员付费"}},{"name":"regist_user_allocation","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"用户注册当天服务期归属"}},{"name":"business_user_pay_status_business","type":"string","nullable":true,"metadata":{"comment":"付费分层-业务维度"}},{"name":"correct_team_names","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"修正后业绩归属"}},{"name":"pad_type","type":"string","nullable":true,"metadata":{"comment":"平板类型"}},{"name":"live_platform_tag","type":"string","nullable":true,"metadata":{"comment":"直播平台标签"}},{"name":"actual_deduct_amount","type":"double","nullable":true,"metadata":{"comment":"用户实际的抵扣金额，单位：元"}},{"name":"max_deduct_amount","type":"double","nullable":true,"metadata":{"comment":"该策略的最高抵扣金额，单位：元"}},{"name":"grade_stage_name_day","type":"string","nullable":true,"metadata":{"comment":"付费当天年级学段"}},{"name":"grade_name_month","type":"string","nullable":true,"metadata":{"comment":"付费当月年级"}},{"name":"stage_name_month","type":"string","nullable":true,"metadata":{"comment":"付费当月学段"}},{"name":"grade_stage_name_month","type":"string","nullable":true,"metadata":{"comment":"付费当月年级学段"}},{"name":"user_pay_status_statistics_month","type":"string","nullable":true,"metadata":{"comment":"付费当月首次标签新增(统计日期当天注册的)、付费(统计日期之前买过正价课)、老未(统计日期之前注册的)"}},{"name":"user_pay_status_business_month","type":"string","nullable":true,"metadata":{"comment":"付费当月首次付费标签 付费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)"}},{"name":"business_user_pay_status_statistics_month","type":"string","nullable":true,"metadata":{"comment":"付费当月首次付费标签新增(统计日期当天注册的)、大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、老未(统计日期之前注册的)"}},{"name":"business_user_pay_status_business_month","type":"string","nullable":true,"metadata":{"comment":"付费当月首次付费标签大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)"}},{"name":"grade_name_year","type":"string","nullable":true,"metadata":{"comment":"付费当年年级"}},{"name":"stage_name_year","type":"string","nullable":true,"metadata":{"comment":"付费当年学段"}},{"name":"grade_stage_name_year","type":"string","nullable":true,"metadata":{"comment":"付费当年年级学段"}},{"name":"user_pay_status_statistics_year","type":"string","nullable":true,"metadata":{"comment":"付费当年首次付费标签：新增(统计日期当天注册的)、付费(统计日期之前买过正价课)、老未(统计日期之前注册的)"}},{"name":"user_pay_status_business_year","type":"string","nullable":true,"metadata":{"comment":"付费当年首次付费标签：付费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)"}},{"name":"business_user_pay_status_statistics_year","type":"string","nullable":true,"metadata":{"comment":"付费当年首次付费标签： 新增(统计日期当天注册的)、大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、老未(统计日期之前注册的)"}},{"name":"business_user_pay_status_business_year","type":"string","nullable":true,"metadata":{"comment":"付费当年首次付费标签：大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)"}},{"name":"good_stage_subject_cnt","type":"integer","nullable":true,"metadata":{"comment":"学段学科个数"}},{"name":"sku_group_good_id","type":"string","nullable":true,"metadata":{"comment":"sku商品组id"}},{"name":"good_kind_name_level_1","type":"string","nullable":true,"metadata":{"comment":"商品类目-一级"}},{"name":"good_kind_name_level_2","type":"string","nullable":true,"metadata":{"comment":"商品类目-二级"}},{"name":"good_kind_name_level_3","type":"string","nullable":true,"metadata":{"comment":"商品类目-三级"}},{"name":"good_kind_id_level_1","type":"string","nullable":true,"metadata":{"comment":"商品类目-一级id"}},{"na',
  'spark.sql.sources.schema.part.4' = 'me":"good_kind_id_level_2","type":"string","nullable":true,"metadata":{"comment":"商品类目-二级id"}},{"name":"good_kind_id_level_3","type":"string","nullable":true,"metadata":{"comment":"商品类目-三级id"}},{"name":"fix_good_kind_id_level_2","type":"string","nullable":true,"metadata":{"comment":"修正-商品类目-二级id(积木块抵扣「升单商品」专用)"}},{"name":"fix_good_kind_name_level_2","type":"string","nullable":true,"metadata":{"comment":"修正-商品类目-二级(积木块抵扣「升单商品」专用)"}},{"name":"is_clue_seat","type":"short","nullable":true,"metadata":{"comment":"线索是否在坐席名下"}},{"name":"fix_good_year","type":"string","nullable":true,"metadata":{"comment":"修正的商品时长"}},{"name":"business_good_kind_name_level_1","type":"string","nullable":true,"metadata":{"comment":"策略组修正-商品类目-一级"}},{"name":"business_good_kind_name_level_2","type":"string","nullable":true,"metadata":{"comment":"策略组修正-商品类目-二级"}},{"name":"business_good_kind_name_level_3","type":"string","nullable":true,"metadata":{"comment":"策略组修正-商品类目-三级"}},{"name":"fix_deductible_price","type":"double","nullable":true,"metadata":{"comment":"修正-补差价总金额"}},{"name":"course_timing_kind","type":"string","nullable":true,"metadata":{"comment":"商品分类标签"}},{"name":"course_group_kind","type":"string","nullable":true,"metadata":{"comment":"商品分组标签"}},{"name":"strategy_type","type":"string","nullable":true,"metadata":{"comment":"策略类型:20260101上线以后为业务数据，之前按规则清洗"}},{"name":"strategy_detail","type":"string","nullable":true,"metadata":{"comment":"策略明细：策略及对应的金额明细"}},{"name":"userauth_exchange_time","type":"timestamp","nullable":true,"metadata":{"comment":"授权转换时间"}},{"name":"delay_vip_activation_time","type":"timestamp","nullable":true,"metadata":{"comment":"囤课品激活时间"}},{"name":"multi_child_refund_time","type":"timestamp","nullable":true,"metadata":{"comment":"多孩策略退差价时间"}}]}',
  'transient_lastDdlTime' = '1770064955'
)