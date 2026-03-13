-- =====================================================
-- 企微资源位曝光到线索到付费转化月表 aws.user_pay_process_add_wechat_month
-- =====================================================
-- 【表粒度】
--   一个资源位(operate_id) × 一个场景(scene) × 一个渠道(task_id) × 一个曝光用户(get_entrance_user) 一条记录
--   按 month(int, 如 202603) 分区
--
-- 【业务定位】
--   企微线索的月度漏斗 + 转化表，追踪以下五层转化：
--   资源位曝光 → 点击入口 → 曝光坐席二维码 → 添加坐席微信 → 成功拉取入库(=企微线索)
--   在此基础上追踪拉取入库用户的当月付费转化
--   业务流程 → business-context.md #1.2 企微线索
--
-- 【与日表(user_pay_process_add_wechat_day)的区别】
--   1. 转化窗口：日表有 7天/14天/当月 三个窗口，月表只有当月窗口
--   2. 用户属性：日表取当天值，月表取月末最后一天的值（按 DAY DESC 排序取第一条）
--   3. 添加/拉取时间窗口：日表=当天到次日凌晨1:00，月表=月初到月末次日凌晨1:00
--   4. 月表独立跑数（不是日表聚合），数据来源相同但范围为整月
--
-- 【数据来源】
--   events.frontend_event_orc         —— 埋点数据：曝光 + 点击（整月范围）
--   crm.new_user                      —— 企微添加记录（channel=3），时间窗口：月初到月末次日凌晨1:00
--   aws.clue_info                     —— 线索领取记录（clue_source IN ('WeCom','building_blocks_goods_wecom')）
--   dws.topic_user_active_detail_day  —— 用户属性（月末值，按 DAY DESC 取第一条）
--   dw.dim_grade                      —— 年级→学段映射维表
--   aws.crm_order_info                —— 电销订单（is_test=false, in_salary=1, worker_id<>0，当月范围）
--   user_allocation.user_allocation   —— 用户服务期归属
--   crm.qr_code_change_history        —— 渠道活码历史变更记录
--
-- 【用户属性取值时点】
--   取当月最后一天活跃表数据（按 DAY DESC 排序取第一条）
--
-- 【常用筛选条件】
--   无特殊必加条件，按 month 分区过滤即可
-- =====================================================

CREATE TABLE
  `aws`.`user_pay_process_add_wechat_month` (

    -- ========== 资源位信息 ==========
    `scene` string COMMENT '资源位对应的场景标识',
    `option` string COMMENT '点击场景后跳转的页面类型',
    `operate_id` string COMMENT '资源位ID',
    `page_type` string COMMENT '页面类型，固定值"引流"',
    `task_id` string COMMENT '资源位对应的渠道活码ID（qr_code_id），关联 crm.qr_code_change_history',

    -- ========== 企微漏斗字段（5层） ==========
    `get_entrance_user` string COMMENT '【漏斗第1层-曝光量】资源位曝光的用户ID',
    `click_entrance_user` string COMMENT '【漏斗第2层-点击量】点击资源位的用户ID，非NULL表示该用户点击了入口',
    `get_wechat_user` string COMMENT '【漏斗第3层-二维码曝光量】曝光了坐席二维码的用户ID',
    `add_wechat_user` string COMMENT '【漏斗第4层-添加量】添加了坐席微信的用户ID（时间窗口：月初到月末次日凌晨1:00），来源 crm.new_user',
    `pull_wechat_user` string COMMENT '【漏斗第5层-拉取入库量】被坐席成功拉取入库的用户ID = 企微线索（时间窗口：月初到月末次日凌晨1:00），来源 aws.clue_info（clue_source IN WeCom/building_blocks_goods_wecom）',
    `info_uuid` string COMMENT '拉取入库对应的线索领取记录ID（aws.clue_info.info_uuid），仅 pull_wechat_user 非NULL时有值',

    -- ========== 用户属性（月末值，取当月最后一天活跃表数据） ==========
    `grade` string COMMENT '【月末值】年级',
    `gender` string COMMENT '【月末值】性别',
    `regist_time` timestamp COMMENT '注册时间',
    `user_attribution` string COMMENT '用户注册当天归属',
    `active_user_attribution` string COMMENT '【月末值】用户活跃时归属',
    `city_class` string COMMENT '【月末值】用户城市分线',
    `province` string COMMENT '【月末值】省名称',
    `city` string COMMENT '【月末值】市名称',
    `user_pay_status_statistics` string COMMENT '【月末值】统计维度付费状态：新增、老未、付费',
    `user_pay_status_business` string COMMENT '【月末值】业务维度付费状态：新用户、老用户、付费用户',

    -- ========== 转化字段（仅当月窗口，基于 pull_wechat_user 关联订单） ==========
    `paid_current_month_user` string COMMENT '【当月转化】当月拉取入库用户中截止当月月底有付费的用户ID，无付费则为NULL',
    `paid_current_month_order_cnt` bigint COMMENT '【当月转化】订单量',
    `paid_current_month_amount` double COMMENT '【当月转化】付费金额',

    -- ========== 其他信息 ==========
    `event_time` bigint COMMENT '曝光事件时间戳（毫秒级）',
    `before_get_entrance_team_name` string COMMENT '曝光前用户所属服务期团队名称，无服务期则为"无服务期"',

    -- ========== 渠道活码信息（来源 crm.qr_code_change_history） ==========
    `scene_name` string COMMENT '渠道活码场景名称',
    `clue_level_name` string COMMENT '渠道活码等级名称',
    `resource_entrance_name` string COMMENT '渠道活码入口名称',
    `type_name` string COMMENT '渠道活码类型名称（排除"测试类型"）',
    `stage_name` string COMMENT '【月末值】学段（通过 dw.dim_grade 由年级映射而来）'
  ) COMMENT '一个资源位id一个资源位的场景一个资源位的渠道id一个资源位曝光用户一条记录' PARTITIONED BY (`month` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/aws.db/user_pay_process_add_wechat_month' TBLPROPERTIES (
    'bucketing_version' = '2',
    'last_modified_by' = 'huaxiong',
    'last_modified_time' = '1763620256',
    'spark.sql.create.version' = '2.2 or prior',
    'transient_lastDdlTime' = '1770629431'
  )
