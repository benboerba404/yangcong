-- =====================================================
-- 线索来源名称维表 tmp.wuhan_clue_soure_name
-- =====================================================
-- 【表说明】
--   线索来源编码 → 中文名称的映射维表。
--   aws.clue_info 中 clue_source 字段存的是英文编码（如 mid_school、WeCom），
--   需要 JOIN 本表才能得到中文名称。
--
-- 【关联方式】
--   left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
--
-- 【常用字段】
--   - clue_source            原始来源编码（JOIN key）
--   - clue_source_name       线索来源名称（中文，如"公海池"、"企微"）
--   - clue_source_name_level_1  线索来源一级分类（更粗粒度的分组）
--
-- ⚠️ clue_source_name 和 clue_source_name_level_1 不是 aws.clue_info 的字段！
--    必须通过 JOIN 本表获取，不可直接从线索表选取。
-- =====================================================

CREATE TABLE `tmp`.`wuhan_clue_soure_name` (
  `clue_source`              string COMMENT '线索来源编码（关联 aws.clue_info.clue_source）',
  `clue_source_name`         string COMMENT '线索来源名称（中文展示名）',
  `clue_source_name_level_1` string COMMENT '线索来源一级分类'
);
