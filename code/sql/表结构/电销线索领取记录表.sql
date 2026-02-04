-- =====================================================
-- 电销线索领取记录表 aws.clue_info
-- =====================================================
-- 【表粒度】
--   一个用户被领取一次 = 一条记录（info_uuid 唯一标识）
--   同一用户(user_id)可被同一或不同坐席(worker_id)领取多次
--
-- 【统计口径】
--   领取次数 = count(info_uuid)
--   领取线索量/消耗线索量 = count(distinct user_id)
--
-- 【在库 vs 电销服务期】（两个不同概念）
--   - 在库（在坐席名下）：当前日期在 created_at ~ clue_expire_time 之间
--     · 判断方式：本表时间范围 或 活跃表 is_clue_seat=1
--   - 电销服务期：被电销触达（领取）后进入的服务期，时长与线索来源有关
--     · 判断方式：活跃表 user_allocation 包含"电销"
--   - 关系：在库 → 一定在电销服务期；在电销服务期 → 不一定在库（线索可能已过期但服务期未结束）
--
-- 【线索生命周期】
--   领取(created_at) → 在库期间 → 过期(clue_expire_time)
--   - 在库期间：线索在当前坐席名下
--   - 过期后：线索不在坐席名下，但用户可能仍在电销服务期，可被其他坐席重新领取
--
-- 【线索来源】(clue_source) - 关联维表 tmp.wuhan_clue_soure_name
--   - mid_school: 公海池线索（用户活跃→推送电销→进入公海池→坐席领取）
--     · 下钻字段：tag（电话线索分类标签，用户画像分层）
--   - WeCom: 企微线索（用户活跃→资源位曝光→点击→添加坐席→拉取入库）
--     · 下钻字段：wecom_clue_level_id（渠道活码线索等级）→ qr_code_channel_id（渠道活码渠道id）
--   - mid_school_manual: 中学人工录入
--   - manual: 人工录入
--   - 其他来源详见维表
--   注意：tag 仅在 mid_school 来源时有效，wecom_clue_level_id/qr_code_channel_id 仅在 WeCom 来源时有效
--
-- 【组织架构层级】
--   workplace_id(职场) → department_id(学部) → regiment_id(团) → heads_id(主管组) → team_id(小组) → worker_id(坐席)
--
-- 【常用筛选条件】
--   - workplace_id in (4, 400, 702)  -- 电销职场
--   - regiment_id not in (0, 303, 546)  -- 排除特定团组
--   - user_sk > 0  -- 有效用户
--   - worker_id <> 0  -- 有坐席
--
-- 【转化指标】⚠️ 不建议使用
--   - 本表转化指标（paid_cnt, paid_amount 等）口径不明确
--   - 转化需求请直接关联订单表 aws.crm_order_info 计算
--   - 订单表通过 recent_info_uuid 关联本表 info_uuid 可追溯线索来源
--
-- 【用户行为指标】
--   - 领取前行为（用于评估线索质量）：
--     · 字段注释含"领取前"：launch_order_3d_cnt, pay_block_dialog_3d_cnt, enter_payment_page_3d_cnt 等
--     · 字段注释含"过去X天"：open_count_1, open_count_3, open_count_7, open_count_14（打开学情报告次数）
--   - 领取后行为（用于评估转化效果）：
--     · 字段注释含"领取X天内"：exercise_cnt_3days, watch_video_cnt_7days 等
-- =====================================================

CREATE TABLE
  `aws`.`clue_info` (
    `info_uuid` string COMMENT '线索领取记录id（主键，一次领取一条记录）',
    `user_id` string COMMENT '用户id（与活跃表u_user关联，统计线索量时对此字段去重）',
    `user_sk` int COMMENT '用户sk（筛选条件：user_sk > 0 表示有效用户）',
    `worker_id` string COMMENT '坐席ID（筛选条件：worker_id <> 0 表示有坐席）',
    `created_at` timestamp COMMENT '线索领取时间（在库起始时间）',
    `created_sk` string COMMENT '线索创建时间sk',
    `clue_expire_time` timestamp COMMENT '线索过期时间（在库结束时间。过期后线索不在坐席名下，但用户可能仍在电销服务期）',
    `clue_source` string COMMENT '线索来源（关联维表tmp.wuhan_clue_soure_name，枚举：mid_school公海池、WeCom企微、mid_school_manual中学人工、manual人工录入等）',
    `user_type_name` string COMMENT '用户类型（新增、续费、老未等）',
    `clue_stage` string COMMENT '学段',
    `clue_grade` string COMMENT '年级',
    `phone` string COMMENT '手机号',
    `is_first_receive` smallint COMMENT '是否首次领取',
    `gender` string COMMENT '性别',
    `province` string COMMENT '省',
    `city` string COMMENT '市',
    `regist_type` string COMMENT '注册方式(枚举值)',
    `user_attribution` string COMMENT '数仓计算用户当天归属',
    `city_class` string COMMENT '客户所在城市线级',
    `call_phone_cnt` int COMMENT '统计日期当天外呼次数',
    `call_through_cnt` int COMMENT '统计日期当天接通次数',
    `valid_call_cnt` int COMMENT '统计日期当天有效接通次数（通话时长>=10秒为有效接通）',
    `call_time_total` int COMMENT '统计日期当天总通话时长',
    `valid_call_time_total` int COMMENT '统计日期当天有效通话时长',
    `paid_cnt` int COMMENT '统计日期当天转化订单量',
    `paid_amount` double COMMENT '统计日期当天转化订单金额',
    `valid_call_paid_cnt` int COMMENT '统计日期当天有效接通线索转化订单量',
    `valid_call_paid_amount` double COMMENT '统计日期当天有效接通线索转化订单金额',
    `call_phone_cnt_7d` int COMMENT '统计日期起7天内外呼次数',
    `call_through_cnt_7d` bigint COMMENT '统计日期起7天内接通次数',
    `valid_call_cnt_7d` int COMMENT '统计日期起7天内有效接通次数',
    `call_time_total_7d` int COMMENT '统计日期起7天内总通话时长',
    `valid_call_time_total_7d` int COMMENT '统计日期起7天内有效通话时长',
    `paid_cnt_7d` int COMMENT '统计日期起7天内转化订单量',
    `paid_amount_7d` double COMMENT '统计日期起7天内转化订单金额',
    `valid_call_paid_cnt_7d` int COMMENT '统计日期起7天内效接通线索转化订单量',
    `valid_call_paid_amount_7d` double COMMENT '统计日期起7天内效接通线索转化订单金额',
    `call_phone_cnt_14d` int COMMENT '统计日期起14天内外呼次数',
    `call_through_cnt_14d` int COMMENT '统计日期起14天内接通次数',
    `valid_call_cnt_14d` int COMMENT '统计日期起14天内有效接通次数',
    `call_time_total_14d` int COMMENT '统计日期起14天内总通话时长',
    `valid_call_time_total_14d` int COMMENT '统计日期起14天内有效通话时长',
    `paid_cnt_14d` int COMMENT '统计日期起14天内转化订单量',
    `paid_amount_14d` double COMMENT '统计日期起14天内转化订单金额',
    `valid_call_paid_cnt_14d` int COMMENT '统计日期起14天内效接通线索转化订单量',
    `valid_call_paid_amount_14d` double COMMENT '统计日期起14天内效接通线索转化订单金额',
    `call_phone_cnt_30d` int COMMENT '统计日期起30天内外呼次数',
    `call_through_cnt_30d` int COMMENT '统计日期起30天内接通次数',
    `valid_call_cnt_30d` int COMMENT '统计日期起30天内有效接通次数',
    `call_time_total_30d` int COMMENT '统计日期起30天内总通话时长',
    `valid_call_time_total_30d` int COMMENT '统计日期起30天内有效通话时长',
    `paid_cnt_30d` int COMMENT '统计日期起30天内转化订单量',
    `paid_amount_30d` double COMMENT '统计日期起30天内转化订单金额',
    `valid_call_paid_cnt_30d` int COMMENT '统计日期起30天内效接通线索转化订单量',
    `valid_call_paid_amount_30d` double COMMENT '统计日期起30天内效接通线索转化订单金额',
    `regist_time` string COMMENT '用户注册时间',
    `regist_create_clue_duration_day` int COMMENT '注册到创建时间间隔',
    `user_clue_cnt` int COMMENT '该用户历史被领取次数（同一user_id在表中有多条记录时，此字段记录累计次数）',
    `user_vaild_clue_cnt` int COMMENT '线索有效领取次数',
    `exercise_cnt_3days` int COMMENT '领取3天内练习次数',
    `finsh_exercise_cnt_3days` int COMMENT '领取3天内完成练习次数',
    `watch_video_cnt_3days` int COMMENT '领取三天内观看视频次数',
    `finsh_watch_video_cnt_3days` int COMMENT '领取三天内完成视频观看次数',
    `exercise_cnt_7days` int COMMENT '领取7天内练习次数',
    `finsh_exercise_cnt_7days` int COMMENT '领取7天内完成练习次数',
    `watch_video_cnt_7days` int COMMENT '领取7天内观看视频次数',
    `finsh_watch_video_cnt_7days` int COMMENT '领取7天内完成视频观看次数',
    `exercise_cnt_before_3days` int COMMENT '领取线索之前(不包括领取当天)3天内练习次数',
    `finsh_exercise_cnt_before_3days` int COMMENT '领取线索之前(不包括领取当天)3天内完成练习次数',
    `watch_video_cnt_before_3days` int COMMENT '领取线索之前(不包括领取当天)三天内观看视频次数',
    `finsh_watch_video_cnt_before_3days` int COMMENT '领取线索之前(不包括领取当天)三天内完成视频观看次数',
    `exercise_cnt_before_7days` int COMMENT '领取线索之前(不包括领取当天)7天内练习次数',
    `finsh_exercise_cnt_before_7days` int COMMENT '领取线索之前(不包括领取当天)7天内完成练习次数',
    `watch_video_cnt_before_7days` int COMMENT '领取线索之前(不包括领取当天)7天内观看视频次数',
    `finsh_watch_video_cnt_before_7days` int COMMENT '领取线索之前(不包括领取当天)7天内完成视频观看次数',
    `department_id` string COMMENT '学部id',
    `regiment_id` string COMMENT '团id（常用筛选：排除0、303、546，关联dw.dim_crm_organization获取团名称）',
    `heads_id` string COMMENT '主管组id',
    `team_id` string COMMENT '小组id',
    `worker_name` string COMMENT '坐席名称',
    `we_com_open_id` string COMMENT '线索企微openid',
    `first_clue_source` string COMMENT '首次领取时线索来源（用于分析用户首次触达渠道）',
    `wecom_channel_id` int COMMENT '通过企微来的线索的渠道id',
    `first_wecom_channel_id` int COMMENT '首次领取时线索企微渠道',
    `last_enter_produce_time` timestamp COMMENT '线索被领取前最近一次进入公海池的时间',
    `u_from` string COMMENT '系统平台',
    `type` string COMMENT '注册方式(枚举值)',
    `real_identity` string COMMENT '用户的真实身份',
    `before_create_clue_last_paid_amount` double COMMENT '领取该条线索前，用户最近一次购买的订单实际支付金额',
    `before_create_clue_last_original_paid_amount` double COMMENT '领取该条线索前，用户最近一次购买的订单原始金额',
    `before_create_clue_last_paid_time` timestamp COMMENT '领取该条线索前，用户最近一次购买的订单的时间',
    `workplace_id` int COMMENT '销售职场id（常用筛选：4、400、702为电销职场）',
    `source_detail` string COMMENT '企微线索二级来源（lotteryDraw 抽奖小程序普通用户,lotteryDrawFission  抽奖小程序裂变用户）',
    `launch_order_3d_cnt` int COMMENT '领取前3天发起待支付订单个数（不含支付成功订单）',
    `pay_block_dialog_3d_cnt` int COMMENT '领取前3天播放器付费视频阻断次数',
    `total_review_pay_block_dialog_3d_cnt` int COMMENT '领取前3天总复习付费视频阻断次数',
    `click_study_upgrade_course_3d_cnt` int COMMENT '领取前3天教材同步章节列表页点击升级课程按钮次数',
    `click_total_review_upgrade_course_3d_cnt` int COMMENT '领取前3天总复习章节列表页点击升级课程按钮次数',
    `click_thinking_expanded_upgrade_course_3d_cnt` int COMMENT '领取前3天教材同步思维拓展章节列表页点击升级课程按钮次数',
    `click_service_button_3d_cnt` int COMMENT '领取前3天点击客服按钮次数',
    `enter_payment_page_3d_cnt` int COMMENT '领取前3天进入订单支付详情页次数',
    `popup_payment_custservice_3d_cnt` int COMMENT '领取前3天订单支付详情页点击人工咨询后弹出客服弹窗次数',
    `click_payment_helpdoc_3d_cnt` int COMMENT '领取前3天订单支付详情页点击支付帮助次数',
    `click_payment_confirm_3d_cnt` int COMMENT '领取前3天订单支付详情页点击确认支付次数',
    `click_payment_exit_stay_button_3d_cnt` int COMMENT '领取前3天退出支付阻断弹窗内点击我再想想次数',
    `note` string COMMENT '备注',
    `enter_community_3d_cnt` int COMMENT '领取前3天进入社区场景次数',
    `community_content_comment_3d_cnt` int COMMENT '领取前3天社区内评论次数',
    `community_content_success_public_3d_cnt` int COMMENT '领取前3天社区内成功发布内容次数',
    `enter_onion_shop_3d_cnt` int COMMENT '领取前3天进入洋葱商店次数',
    `enter_onion_shop_exchange_dialog_3d_cnt` int COMMENT '领取前3天洋葱商店兑换次数',
    `enter_primp_3d_cnt` int COMMENT '领取前3天进入换装次数',
    `enter_draw_3d_cnt` int COMMENT '领取前3天抽取服装次数',
    `add_wechat_cnt` int COMMENT '历史添加过企微的次数',
    `mid_active_type` string COMMENT '活跃类型（新增：首次活跃；回流：30天+未活跃后再活跃；持续：连续活跃）',
    `phone_range` string COMMENT '用户号段(手机号前3位)',
    `level` int COMMENT '领取当天用户星阶等级',
    `click_help_center_home_menu_3d_cnt` int COMMENT '领取前3天在帮助中心查询优惠卷次数',
    `click_nuannuan_message_button_3d_cnt` int COMMENT '领取前3天在洋葱树洞收听音频/视频的次数',
    `click_total_reviewnew_upgrade_3d_cnt` int COMMENT '领取前3天点击新中考总复习升级按钮次数',
    `click_exam_store_download_3d_cnt` int COMMENT '领取前3天试卷下载阻断次数',
    `login_device_num` int COMMENT '领取前用户历史登录设备数',
    `device_regist_uv` int COMMENT '领取前用户当前设备注册用户数',
    `mid_active_type_2d` string COMMENT '领取线索时最近2天的活跃类型',
    `paid_cnt_current_month` int COMMENT '统计日期起截止当月底内转化订单量',
    `paid_amount_current_month` int COMMENT '统计日期起截止当月底转化订单金额',
    `valid_call_paid_cnt_current_month` int COMMENT '统计日期起截止当月底接通线索转化订单量',
    `valid_call_paid_amount_current_month` int COMMENT '统计日期起截止当月底效接通线索转化订单金额',
    `before_sum_amount` double COMMENT '截止领取前用户历史累计成功付费金额',
    `user_vip_tag` string COMMENT '领取线索前用户会员身份标签',
    `is_bind_parent` smallint COMMENT '领取当天是否绑定家长用户',
    `auth_type` array < string > COMMENT '领取线索当天用户授权类型',
    `worker_join_at` timestamp COMMENT '销售入职日期',
    `business_user_pay_status_statistics` string COMMENT '新增(统计日期当天注册的)、大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、老未(统计日期之前注册的)',
    `business_user_pay_status_business` string COMMENT '大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)',
    `new_exam` smallint COMMENT '是否新中考区域',
    `open_count_1` int COMMENT '过去一天打开学情报告次数',
    `open_count_3` int COMMENT '过去三天打开学情报告次数',
    `open_count_7` int COMMENT '过去七天打开学情报告次数',
    `open_count_14` int COMMENT '过去十四天打开学情报告次数',
    `risk_score` smallint COMMENT '风险分数 分数范围为【0-9】，分数越大，手机号风险值越高',
    `risk_tag` smallint COMMENT '风险标签',
    `site_id` int COMMENT '线索所属站点id',
    `last_active_time` timestamp COMMENT '最后一次看视频时间',
    `have_call` int COMMENT '是否发起外呼',
    `call_count` int COMMENT '外呼次数',
    `call_duration` int COMMENT '外呼时长',
    `call_state` int COMMENT '外呼状态',
    `last_paid_time` timestamp COMMENT '最后支付时间',
    `initial_phone` string COMMENT '初始手机号',
    `last_dealing_time` timestamp COMMENT '最后一次拨通时间戳',
    `qr_code_channel_id` int COMMENT '渠道活码渠道id（仅clue_source=WeCom时有效，wecom_clue_level_id的下钻维度）',
    `province_code` string COMMENT '省份行政编码',
    `city_code` string COMMENT '城市行政编码',
    `district_code` string COMMENT '区县行政编码',
    `tag` string COMMENT '电话线索分类标签（仅clue_source=mid_school时有效，用户画像分层）',
    `feature_tags` string COMMENT '特性标签',
    `wecom_clue_level_id` bigint COMMENT '渠道活码线索等级（仅clue_source=WeCom时有效，企微线索的分类标签）'
  ) COMMENT '一个用户一个线索一条记录 https://guanghe.feishu.cn/sheets/Q2UNscQ6JhZVZ5tWpRtcqkFEnFf?sheet=9ff9f5' ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/aws.db/clue_info' TBLPROPERTIES (
    'alias' = '应用层线索表',
    'bucketing_version' = '2',
    'is_core' = 'true',
    'last_modified_by' = 'finebi',
    'last_modified_time' = '1766385378',
    'spark.sql.create.version' = '3.2.1',
    'spark.sql.sources.schema.numParts' = '5',
    'spark.sql.sources.schema.part.0' = '{"type":"struct","fields":[{"name":"info_uuid","type":"string","nullable":true,"metadata":{"comment":"线索领取记录id"}},{"name":"user_id","type":"string","nullable":true,"metadata":{"comment":"用户id"}},{"name":"user_sk","type":"integer","nullable":true,"metadata":{"comment":"用户sk"}},{"name":"worker_id","type":"string","nullable":true,"metadata":{"comment":"坐席ID"}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{"comment":"线索领取时间"}},{"name":"created_sk","type":"string","nullable":true,"metadata":{"comment":"线索创建时间sk"}},{"name":"clue_expire_time","type":"timestamp","nullable":true,"metadata":{"comment":"线索过期时间"}},{"name":"clue_source","type":"string","nullable":true,"metadata":{"comment":"线索来源"}},{"name":"user_type_name","type":"string","nullable":true,"metadata":{"comment":"用户类型"}},{"name":"clue_stage","type":"string","nullable":true,"metadata":{"comment":"学段"}},{"name":"clue_grade","type":"string","nullable":true,"metadata":{"comment":"年级"}},{"name":"phone","type":"string","nullable":true,"metadata":{"comment":"手机号"}},{"name":"is_first_receive","type":"short","nullable":true,"metadata":{"comment":"是否首次领取"}},{"name":"gender","type":"string","nullable":true,"metadata":{"comment":"性别"}},{"name":"province","type":"string","nullable":true,"metadata":{"comment":"省"}},{"name":"city","type":"string","nullable":true,"metadata":{"comment":"市"}},{"name":"regist_type","type":"string","nullable":true,"metadata":{"comment":"注册方式(枚举值)"}},{"name":"user_attribution","type":"string","nullable":true,"metadata":{"comment":"数仓计算用户当天归属"}},{"name":"city_class","type":"string","nullable":true,"metadata":{"comment":"客户所在城市线级"}},{"name":"call_phone_cnt","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天外呼次数"}},{"name":"call_through_cnt","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天接通次数"}},{"name":"valid_call_cnt","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天有效接通次数"}},{"name":"call_time_total","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天总通话时长"}},{"name":"valid_call_time_total","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天有效通话时长"}},{"name":"paid_cnt","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天转化订单量"}},{"name":"paid_amount","type":"double","nullable":true,"metadata":{"comment":"统计日期当天转化订单金额"}},{"name":"valid_call_paid_cnt","type":"integer","nullable":true,"metadata":{"comment":"统计日期当天有效接通线索转化订单量"}},{"name":"valid_call_paid_amount","type":"double","nullable":true,"metadata":{"comment":"统计日期当天有效接通线索转化订单金额"}},{"name":"call_phone_cnt_7d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起7天内外呼次数"}},{"name":"call_through_cnt_7d","type":"long","nullable":true,"metadata":{"comment":"统计日期起7天内接通次数"}},{"name":"valid_call_cnt_7d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起7天内有效接通次数"}},{"name":"call_time_total_7d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起7天内总通话时长"}},{"name":"valid_call_time_total_7d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起7天内有效通话时长"}},{"name":"paid_cnt_7d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起7天内转化订单量"}},{"name":"paid_amount_7d","type":"double","nullable":true,"metadata":{"comment":"统计日期起7天内转化订单金额"}},{"name":"valid_call_paid_cnt_7d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起7天内效接通线索转化订单量"}},{"name":"valid_call_paid_amount_7d","type":"double","nullable":true,"metadata":{"comment":"统计日期起7天内效接通线索转化订单金额"}},{"name":"call_phone_cnt_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内外呼次数"}},{"name":"call_through_cnt_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内接通次数"}},{"name":"valid_call_cnt_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内有效接通次数"}},{"name":"call_time_total_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内总通话时长"}},{"name":"valid_call_time_total_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内有效通话时长"}},{"name":"pa',
    'spark.sql.sources.schema.part.1' = 'id_cnt_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内转化订单量"}},{"name":"paid_amount_14d","type":"double","nullable":true,"metadata":{"comment":"统计日期起14天内转化订单金额"}},{"name":"valid_call_paid_cnt_14d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起14天内效接通线索转化订单量"}},{"name":"valid_call_paid_amount_14d","type":"double","nullable":true,"metadata":{"comment":"统计日期起14天内效接通线索转化订单金额"}},{"name":"call_phone_cnt_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内外呼次数"}},{"name":"call_through_cnt_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内接通次数"}},{"name":"valid_call_cnt_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内有效接通次数"}},{"name":"call_time_total_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内总通话时长"}},{"name":"valid_call_time_total_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内有效通话时长"}},{"name":"paid_cnt_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内转化订单量"}},{"name":"paid_amount_30d","type":"double","nullable":true,"metadata":{"comment":"统计日期起30天内转化订单金额"}},{"name":"valid_call_paid_cnt_30d","type":"integer","nullable":true,"metadata":{"comment":"统计日期起30天内效接通线索转化订单量"}},{"name":"valid_call_paid_amount_30d","type":"double","nullable":true,"metadata":{"comment":"统计日期起30天内效接通线索转化订单金额"}},{"name":"regist_time","type":"string","nullable":true,"metadata":{"comment":"用户注册时间"}},{"name":"regist_create_clue_duration_day","type":"integer","nullable":true,"metadata":{"comment":"注册到创建时间间隔"}},{"name":"user_clue_cnt","type":"integer","nullable":true,"metadata":{"comment":"线索被领取次数"}},{"name":"user_vaild_clue_cnt","type":"integer","nullable":true,"metadata":{"comment":"线索有效领取次数"}},{"name":"exercise_cnt_3days","type":"integer","nullable":true,"metadata":{"comment":"领取3天内练习次数"}},{"name":"finsh_exercise_cnt_3days","type":"integer","nullable":true,"metadata":{"comment":"领取3天内完成练习次数"}},{"name":"watch_video_cnt_3days","type":"integer","nullable":true,"metadata":{"comment":"领取三天内观看视频次数"}},{"name":"finsh_watch_video_cnt_3days","type":"integer","nullable":true,"metadata":{"comment":"领取三天内完成视频观看次数"}},{"name":"exercise_cnt_7days","type":"integer","nullable":true,"metadata":{"comment":"领取7天内练习次数"}},{"name":"finsh_exercise_cnt_7days","type":"integer","nullable":true,"metadata":{"comment":"领取7天内完成练习次数"}},{"name":"watch_video_cnt_7days","type":"integer","nullable":true,"metadata":{"comment":"领取7天内观看视频次数"}},{"name":"finsh_watch_video_cnt_7days","type":"integer","nullable":true,"metadata":{"comment":"领取7天内完成视频观看次数"}},{"name":"exercise_cnt_before_3days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)3天内练习次数"}},{"name":"finsh_exercise_cnt_before_3days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)3天内完成练习次数"}},{"name":"watch_video_cnt_before_3days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)三天内观看视频次数"}},{"name":"finsh_watch_video_cnt_before_3days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)三天内完成视频观看次数"}},{"name":"exercise_cnt_before_7days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)7天内练习次数"}},{"name":"finsh_exercise_cnt_before_7days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)7天内完成练习次数"}},{"name":"watch_video_cnt_before_7days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)7天内观看视频次数"}},{"name":"finsh_watch_video_cnt_before_7days","type":"integer","nullable":true,"metadata":{"comment":"领取线索之前(不包括领取当天)7天内完成视频观看次数"}},{"name":"department_id","type":"string","nullable":true,"metadata":{"comment":"学部id"}},{"name":"regiment_id","type":"string","nullable":true,"metadata":{"comment":"团id"}},{"name":"heads_id","type":"string","nullable":true,"metadata":{"comment":"主管组id"}},{"name":"team_id","type":"string","nullable":true,"metadata":{"comment":"小组id"}},{"name":"worker_name","type":"string","nullable":true,"metadata":{"comment":"坐席名',
    'spark.sql.sources.schema.part.2' = '称"}},{"name":"we_com_open_id","type":"string","nullable":true,"metadata":{"comment":"线索企微openid"}},{"name":"first_clue_source","type":"string","nullable":true,"metadata":{"comment":"首次领取时线索来源"}},{"name":"wecom_channel_id","type":"integer","nullable":true,"metadata":{"comment":"通过企微来的线索的渠道id"}},{"name":"first_wecom_channel_id","type":"integer","nullable":true,"metadata":{"comment":"首次领取时线索企微渠道"}},{"name":"last_enter_produce_time","type":"timestamp","nullable":true,"metadata":{"comment":"线索被领取前最近一次进入公海池的时间"}},{"name":"u_from","type":"string","nullable":true,"metadata":{"comment":"系统平台"}},{"name":"type","type":"string","nullable":true,"metadata":{"comment":"注册方式(枚举值)"}},{"name":"real_identity","type":"string","nullable":true,"metadata":{"comment":"用户的真实身份"}},{"name":"before_create_clue_last_paid_amount","type":"double","nullable":true,"metadata":{"comment":"领取该条线索前，用户最近一次购买的订单实际支付金额"}},{"name":"before_create_clue_last_original_paid_amount","type":"double","nullable":true,"metadata":{"comment":"领取该条线索前，用户最近一次购买的订单原始金额"}},{"name":"before_create_clue_last_paid_time","type":"timestamp","nullable":true,"metadata":{"comment":"领取该条线索前，用户最近一次购买的订单的时间"}},{"name":"workplace_id","type":"integer","nullable":true,"metadata":{"comment":"销售职场id"}},{"name":"source_detail","type":"string","nullable":true,"metadata":{"comment":"企微线索二级来源（lotteryDraw 抽奖小程序普通用户,lotteryDrawFission  抽奖小程序裂变用户）"}},{"name":"launch_order_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天发起待支付订单个数（不含支付成功订单）"}},{"name":"pay_block_dialog_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天播放器付费视频阻断次数"}},{"name":"total_review_pay_block_dialog_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天总复习付费视频阻断次数"}},{"name":"click_study_upgrade_course_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天教材同步章节列表页点击升级课程按钮次数"}},{"name":"click_total_review_upgrade_course_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天总复习章节列表页点击升级课程按钮次数"}},{"name":"click_thinking_expanded_upgrade_course_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天教材同步思维拓展章节列表页点击升级课程按钮次数"}},{"name":"click_service_button_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天点击客服按钮次数"}},{"name":"enter_payment_page_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天进入订单支付详情页次数"}},{"name":"popup_payment_custservice_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天订单支付详情页点击人工咨询后弹出客服弹窗次数"}},{"name":"click_payment_helpdoc_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天订单支付详情页点击支付帮助次数"}},{"name":"click_payment_confirm_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天订单支付详情页点击确认支付次数"}},{"name":"click_payment_exit_stay_button_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天退出支付阻断弹窗内点击我再想想次数"}},{"name":"note","type":"string","nullable":true,"metadata":{"comment":"备注"}},{"name":"enter_community_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天进入社区场景次数"}},{"name":"community_content_comment_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天社区内评论次数"}},{"name":"community_content_success_public_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天社区内成功发布内容次数"}},{"name":"enter_onion_shop_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天进入洋葱商店次数"}},{"name":"enter_onion_shop_exchange_dialog_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天洋葱商店兑换次数"}},{"name":"enter_primp_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天进入换装次数"}},{"name":"enter_draw_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天抽取服装次数"}},{"name":"add_wechat_cnt","type":"integer","nullable":true,"metadata":{"comment":"历史添加过企微的次数"}},{"name":"mid_active_type","type":"string","nullable":true,"metadata":{"comment":"活跃类型(新增、回流、持续)"}},{"name":"phone_range","type":"string","nullable":true,"metadata":{"comment":"用户号段(手机号前',
    'spark.sql.sources.schema.part.3' = '3位)"}},{"name":"level","type":"integer","nullable":true,"metadata":{"comment":"领取当天用户星阶等级"}},{"name":"click_help_center_home_menu_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天在帮助中心查询优惠卷次数"}},{"name":"click_nuannuan_message_button_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天在洋葱树洞收听音频/视频的次数"}},{"name":"click_total_reviewnew_upgrade_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天点击新中考总复习升级按钮次数"}},{"name":"click_exam_store_download_3d_cnt","type":"integer","nullable":true,"metadata":{"comment":"领取前3天试卷下载阻断次数"}},{"name":"login_device_num","type":"integer","nullable":true,"metadata":{"comment":"领取前用户历史登录设备数"}},{"name":"device_regist_uv","type":"integer","nullable":true,"metadata":{"comment":"领取前用户当前设备注册用户数"}},{"name":"mid_active_type_2d","type":"string","nullable":true,"metadata":{"comment":"领取线索时最近2天的活跃类型"}},{"name":"paid_cnt_current_month","type":"integer","nullable":true,"metadata":{"comment":"统计日期起截止当月底内转化订单量"}},{"name":"paid_amount_current_month","type":"integer","nullable":true,"metadata":{"comment":"统计日期起截止当月底转化订单金额"}},{"name":"valid_call_paid_cnt_current_month","type":"integer","nullable":true,"metadata":{"comment":"统计日期起截止当月底接通线索转化订单量"}},{"name":"valid_call_paid_amount_current_month","type":"integer","nullable":true,"metadata":{"comment":"统计日期起截止当月底效接通线索转化订单金额"}},{"name":"before_sum_amount","type":"double","nullable":true,"metadata":{"comment":"截止领取前用户历史累计成功付费金额"}},{"name":"user_vip_tag","type":"string","nullable":true,"metadata":{"comment":"领取线索前用户会员身份标签"}},{"name":"is_bind_parent","type":"short","nullable":true,"metadata":{"comment":"领取当天是否绑定家长用户"}},{"name":"auth_type","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"领取线索当天用户授权类型"}},{"name":"worker_join_at","type":"timestamp","nullable":true,"metadata":{"comment":"销售入职日期"}},{"name":"business_user_pay_status_statistics","type":"string","nullable":true,"metadata":{"comment":"新增(统计日期当天注册的)、大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、老未(统计日期之前注册的)"}},{"name":"business_user_pay_status_business","type":"string","nullable":true,"metadata":{"comment":"大会员付费用户(统计日期之前买过大会员商品)、续费用户(统计日期之前买过正价课)、新用户(统计日期30天内注册的)、老用户(统计日期30以前注册的)"}},{"name":"new_exam","type":"short","nullable":true,"metadata":{"comment":"是否新中考区域"}},{"name":"open_count_1","type":"integer","nullable":true,"metadata":{"comment":"过去一天打开学情报告次数"}},{"name":"open_count_3","type":"integer","nullable":true,"metadata":{"comment":"过去三天打开学情报告次数"}},{"name":"open_count_7","type":"integer","nullable":true,"metadata":{"comment":"过去七天打开学情报告次数"}},{"name":"open_count_14","type":"integer","nullable":true,"metadata":{"comment":"过去十四天打开学情报告次数"}},{"name":"risk_score","type":"short","nullable":true,"metadata":{"comment":"风险分数 分数范围为【0-9】，分数越大，手机号风险值越高"}},{"name":"risk_tag","type":"short","nullable":true,"metadata":{"comment":"风险标签"}},{"name":"site_id","type":"integer","nullable":true,"metadata":{"comment":"线索所属站点id"}},{"name":"last_active_time","type":"timestamp","nullable":true,"metadata":{"comment":"最后一次看视频时间"}},{"name":"have_call","type":"integer","nullable":true,"metadata":{"comment":"是否发起外呼"}},{"name":"call_count","type":"integer","nullable":true,"metadata":{"comment":"外呼次数"}},{"name":"call_duration","type":"integer","nullable":true,"metadata":{"comment":"外呼时长"}},{"name":"call_state","type":"integer","nullable":true,"metadata":{"comment":"外呼状态"}},{"name":"last_paid_time","type":"timestamp","nullable":true,"metadata":{"comment":"最后支付时间"}},{"name":"initial_phone","type":"string","nullable":true,"metadata":{"comment":"初始手机号"}},{"name":"last_dealing_time","type":"timestamp","nullable":true,"metadata":{"comment":"最后一次拨通时间戳"}},{"name":"qr_code_channel_id","type":"integer","nullable":true,"metadata":{"comment":"渠道活码渠道id"}},{"name":"province_code","type":"string","nullable":true,"metadata":{"comment":"省份行政编码"}},{"name":"city_code","type":"string","nullable":true,"metadata":{"comment":"城市行政编码"}},{"name":"district_code","type',
    'spark.sql.sources.schema.part.4' = '":"string","nullable":true,"metadata":{"comment":"区县行政编码"}},{"name":"tag","type":"string","nullable":true,"metadata":{"comment":"电话线索分类标签"}},{"name":"feature_tags","type":"string","nullable":true,"metadata":{"comment":"特性标签"}}]}',
    'transient_lastDdlTime' = '1770060563'
  )