-- =====================================================
-- 用户信息表 dw.dim_user
-- =====================================================
-- 【表粒度】
--   一个用户 = 一条记录（user_sk / u_user 唯一）
--
-- 【数据时效】
--   T-1 快照（存储昨日的用户信息）
--
-- 【用户标识】
--   - user_sk：数仓代理键（整型，用于join优化）
--   - u_user：用户id（= user_id，各表通用）
--   - onion_id：用户在APP内的洋葱id
--   - system_id：系统id
--
-- 【服务期归属】(user_allocation)
--   - 电销服务期：数组包含"电销"（如["电销/网销"]）
--   - 非电销服务期：不含"电销"（如["体验营"]、["入校"]、["新媒体视频"]）
--   - 无服务期：NULL或空数组
--   - regist_user_allocation：注册当天的服务期归属
--
-- 【用户归属】
--   - attribution：用户归属（原始）
--   - user_attribution：数仓计算的当天归属
--   - regist_user_attribution：数仓计算的注册时归属
--
-- 【用户分层】
--   - user_lifecycle_stage：生命周期（引入期<=13天、成长期14-30天、成熟期>=31天）
--   - user_vip_tag：会员身份标签
--   - user_identity：用户身份（研究员/高级研究员/首席研究员）
--
-- 【常用筛选条件】
--   - is_test_user = false  -- 排除测试用户
--   - role = 'student'  -- 学生用户
-- =====================================================

CREATE EXTERNAL TABLE `dw`.`dim_user` (
  `user_sk` int COMMENT '数仓用户sk（主键，整型便于join）',
  `u_user` string COMMENT '用户id（= user_id，各表通用）',
  `system_id` string COMMENT '系统id',
  `onion_id` string COMMENT '用户在APP内的洋葱id',
  `name` string COMMENT '姓名',
  `nickname` string COMMENT '昵称',
  `gender` string COMMENT '性别',
  `role` string COMMENT '身份',
  `school_id` string COMMENT '学校id',
  `school_sk` int COMMENT '学校sk',
  `school_sk1` int COMMENT '学校sk1',
  `channel` string COMMENT '注册渠道',
  `is_put_channel` boolean COMMENT '是否投放渠道',
  `is_room` boolean COMMENT '是否有班',
  `u_from` string COMMENT '系统平台',
  `grade` string COMMENT '年级',
  `type` string COMMENT '注册方式(枚举值)',
  `province` string COMMENT '省',
  `province_code` string COMMENT '省代码',
  `city` string COMMENT '市',
  `city_code` string COMMENT '市代码',
  `area` string COMMENT '地区',
  `area_code` string COMMENT '区',
  `region_source` string COMMENT '区域数据来源',
  `learning_time` int,
  `teaching_type` string COMMENT '教学类型',
  `teacher_organization` string COMMENT '教师用户的单位属性',
  `regist_time` timestamp COMMENT '注册时间',
  `activate_date` timestamp COMMENT '激活时间',
  `regist_time_sk` int COMMENT '注册date_sk',
  `activate_date_sk` int COMMENT '激活date_sk',
  `level` int COMMENT '等级',
  `coins` double COMMENT '洋葱币数量',
  `points` double COMMENT '经验值数',
  `scores` double COMMENT '23个技能总分',
  `verified_by_phone` boolean COMMENT '手机号是否经过验证',
  `is_parents` boolean COMMENT '是否家长',
  `tenant` string COMMENT '校园版id',
  `trial_type` int COMMENT '是否为新商品实验组',
  `is_test_user` boolean COMMENT '是否为测试用户（常用筛选：false=正式用户）',
  `is_agent_user` boolean COMMENT '是否归属代理商',
  `realname` string COMMENT '用户真实姓名',
  `auth_type` array < string > COMMENT '用户授权类型',
  `stage_id` int COMMENT '学段',
  `subject_id` int COMMENT '老师的学科',
  `is_admin_room` boolean COMMENT '是否为行政班用户',
  `regist_app_version` string COMMENT '注册时的app版本号',
  `school_tag` int COMMENT '学校标签：0:非维护学校，1普通维护学校，2、重点维护学校',
  `is_room_user` boolean COMMENT '是否是有班用户',
  `is_teach_user` boolean COMMENT '是否是有教学班用户',
  `regist_entrance_id` string COMMENT '注册入口',
  `os` string COMMENT '操作系统',
  `is_bind_parent` boolean COMMENT '是否绑定家长用户',
  `ladder_info_list` array < string > COMMENT '天梯试炼场数据',
  `skills` array < int > COMMENT '用户能力值',
  `attribution` string COMMENT '用户归属',
  `user_attribution` string COMMENT '数仓计算用户当天归属',
  `regist_user_attribution` string COMMENT '数仓计算用户注册时归属',
  `study_book_info` string COMMENT '学生当前的教学版本和学期信息',
  `study_book_info_array` array < string > COMMENT '学生当前的教学版本和学期信息(数组)',
  `phone` string COMMENT '手机号',
  `email` string COMMENT '邮箱',
  `qq_no` string COMMENT 'qq号',
  `user_allocation` array < string > COMMENT '用户全域服务期（T-1快照）。包含"电销"为电销服务期，其他为非电销服务期，NULL为无服务期',
  `user_vip_tag` string COMMENT '会员身份标签',
  `regist_user_allocation` array < string > COMMENT '用户注册当天服务期归属',
  `real_identity` string COMMENT '用户真实身份',
  `user_risk` string COMMENT '用户风险',
  `user_identity` string COMMENT '用户身份：研究员：common，高级研究员：advanced，首席研究员：lead，体验版首席研究员：expLead',
  `core_first_activated_date` timestamp COMMENT '首次激活洋葱学园app产品时间',
  `user_lifecycle_stage` string COMMENT '引入期：注册天数 <= 13 （注册当天是0） 成长期：14 <= 注册天数 <= 30  成熟期：注册天数31天及以上',
  `is_provost_teacher` int COMMENT '是否确认老师',
  `is_ai_room_user` int COMMENT '是否ai班级用户'
) COMMENT '一个用户一条记录' ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'tos://yc-data-platform/user/hive/warehouse/dw.db/dim_user' TBLPROPERTIES (
  'alias' = '用户基础信息表',
  'bucketing_version' = '2',
  'is_core' = 'true',
  'last_modified_by' = 'huaxiong',
  'last_modified_time' = '1768981465',
  'primary_key' = 'user_sk',
  'spark.sql.create.version' = '2.2 or prior',
  'spark.sql.sources.schema.numParts' = '2',
  'spark.sql.sources.schema.part.0' = '{"type":"struct","fields":[{"name":"user_sk","type":"integer","nullable":true,"metadata":{"comment":"数仓用户sk"}},{"name":"u_user","type":"string","nullable":true,"metadata":{"comment":"用户id"}},{"name":"system_id","type":"string","nullable":true,"metadata":{"comment":"系统id"}},{"name":"onion_id","type":"string","nullable":true,"metadata":{"comment":"洋葱id"}},{"name":"name","type":"string","nullable":true,"metadata":{"comment":"姓名"}},{"name":"nickname","type":"string","nullable":true,"metadata":{"comment":"昵称"}},{"name":"gender","type":"string","nullable":true,"metadata":{"comment":"性别"}},{"name":"role","type":"string","nullable":true,"metadata":{"comment":"身份"}},{"name":"school_id","type":"string","nullable":true,"metadata":{"comment":"学校id"}},{"name":"school_sk","type":"integer","nullable":true,"metadata":{"comment":"学校sk"}},{"name":"school_sk1","type":"integer","nullable":true,"metadata":{"comment":"学校sk1"}},{"name":"channel","type":"string","nullable":true,"metadata":{"comment":"注册渠道"}},{"name":"is_put_channel","type":"boolean","nullable":true,"metadata":{"comment":"是否投放渠道"}},{"name":"is_room","type":"boolean","nullable":true,"metadata":{"comment":"是否有班"}},{"name":"u_from","type":"string","nullable":true,"metadata":{"comment":"系统平台"}},{"name":"grade","type":"string","nullable":true,"metadata":{"comment":"年级"}},{"name":"type","type":"string","nullable":true,"metadata":{"comment":"注册方式(枚举值)"}},{"name":"province","type":"string","nullable":true,"metadata":{"comment":"省"}},{"name":"province_code","type":"string","nullable":true,"metadata":{"comment":"省代码"}},{"name":"city","type":"string","nullable":true,"metadata":{"comment":"市"}},{"name":"city_code","type":"string","nullable":true,"metadata":{"comment":"市代码"}},{"name":"area","type":"string","nullable":true,"metadata":{"comment":"地区"}},{"name":"area_code","type":"string","nullable":true,"metadata":{"comment":"区"}},{"name":"region_source","type":"string","nullable":true,"metadata":{"comment":"区域数据来源"}},{"name":"learning_time","type":"integer","nullable":true,"metadata":{}},{"name":"teaching_type","type":"string","nullable":true,"metadata":{"comment":"教学类型"}},{"name":"teacher_organization","type":"string","nullable":true,"metadata":{"comment":"教师用户的单位属性"}},{"name":"regist_time","type":"timestamp","nullable":true,"metadata":{"comment":"注册时间"}},{"name":"activate_date","type":"timestamp","nullable":true,"metadata":{"comment":"激活时间"}},{"name":"regist_time_sk","type":"integer","nullable":true,"metadata":{"comment":"注册date_sk"}},{"name":"activate_date_sk","type":"integer","nullable":true,"metadata":{"comment":"激活date_sk"}},{"name":"level","type":"integer","nullable":true,"metadata":{"comment":"等级"}},{"name":"coins","type":"double","nullable":true,"metadata":{"comment":"洋葱币数量"}},{"name":"points","type":"double","nullable":true,"metadata":{"comment":"经验值数"}},{"name":"scores","type":"double","nullable":true,"metadata":{"comment":"23个技能总分"}},{"name":"verified_by_phone","type":"boolean","nullable":true,"metadata":{"comment":"手机号是否经过验证"}},{"name":"is_parents","type":"boolean","nullable":true,"metadata":{"comment":"是否家长"}},{"name":"tenant","type":"string","nullable":true,"metadata":{"comment":"校园版id"}},{"name":"trial_type","type":"integer","nullable":true,"metadata":{"comment":"是否为新商品实验组"}},{"name":"is_test_user","type":"boolean","nullable":true,"metadata":{"comment":"是否为测试用户"}},{"name":"is_agent_user","type":"boolean","nullable":true,"metadata":{"comment":"是否归属代理商"}},{"name":"realname","type":"string","nullable":true,"metadata":{"comment":"用户真实姓名"}},{"name":"auth_type","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"用户授权类型"}},{"name":"stage_id","type":"integer","nullable":true,"metadata":{"comment":"学段"}},{"name":"subject_id","type":"integer","nullable":true,"metadata":{"comment":"老师的学科"}},{"name":"is_admin_room","type":"boolean","nullable":true,"metadata":{"comment":"是否为行政班用户"}},{"name":"regist_app_version","type":"string","nullable":true',
  'spark.sql.sources.schema.part.1' = ',"metadata":{"comment":"注册时的app版本号"}},{"name":"school_tag","type":"integer","nullable":true,"metadata":{"comment":"学校标签：0:非维护学校，1普通维护学校，2、重点维护学校"}},{"name":"is_room_user","type":"boolean","nullable":true,"metadata":{"comment":"是否是有班用户"}},{"name":"is_teach_user","type":"boolean","nullable":true,"metadata":{"comment":"是否是有教学班用户"}},{"name":"regist_entrance_id","type":"string","nullable":true,"metadata":{"comment":"注册入口"}},{"name":"os","type":"string","nullable":true,"metadata":{"comment":"操作系统"}},{"name":"is_bind_parent","type":"boolean","nullable":true,"metadata":{"comment":"是否绑定家长用户"}},{"name":"ladder_info_list","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"天梯试炼场数据"}},{"name":"skills","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":true,"metadata":{"comment":"用户能力值"}},{"name":"attribution","type":"string","nullable":true,"metadata":{"comment":"用户归属"}},{"name":"user_attribution","type":"string","nullable":true,"metadata":{"comment":"数仓计算用户当天归属"}},{"name":"regist_user_attribution","type":"string","nullable":true,"metadata":{"comment":"数仓计算用户注册时归属"}},{"name":"study_book_info","type":"string","nullable":true,"metadata":{"comment":"学生当前的教学版本和学期信息"}},{"name":"study_book_info_array","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"学生当前的教学版本和学期信息(数组)"}},{"name":"phone","type":"string","nullable":true,"metadata":{"comment":"手机号"}},{"name":"email","type":"string","nullable":true,"metadata":{"comment":"邮箱"}},{"name":"qq_no","type":"string","nullable":true,"metadata":{"comment":"qq号"}},{"name":"user_allocation","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"用户全域服务期"}},{"name":"user_vip_tag","type":"string","nullable":true,"metadata":{"comment":"会员身份标签"}},{"name":"regist_user_allocation","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{"comment":"用户注册当天服务期归属"}},{"name":"real_identity","type":"string","nullable":true,"metadata":{"comment":"用户真实身份"}},{"name":"user_risk","type":"string","nullable":true,"metadata":{"comment":"用户风险"}},{"name":"user_indetity","type":"string","nullable":true,"metadata":{"comment":"用户身份：研究员：common，高级研究员：advanced，首席研究员：lead，体验版首席研究员：expLead"}}]}',
  'transient_lastDdlTime' = '1770054049'
)