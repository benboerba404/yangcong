package com.onion.etl.aws

import com.onion.etl.util.Constant.getSparkSession
import com.onion.etl.util.DateUitil.{getBeforeXDay, intToStrDate}
import org.apache.spark.sql.SparkSession
import org.joda.time.{Days, DateTime}

/**
  * Created by haodong on 2023/6/13 10:52
  */
object UserPayProcessAddWechatDay {

  def main(args: Array[String]): Unit = {

    //传入昨天日期
    val yesterday = args(0).toInt

    //转换为date格式
    val yesterdate = intToStrDate(yesterday)

    //取当前日期所在月的第一天
    val month_start = yesterdate.substring(0, 8) + "01"

    //取当前日期的前14天
    val before14 = getBeforeXDay(yesterday, 14)

    //转换为date格式
    val before14date = intToStrDate(before14)

    //前14天与月初取最早的作为数据开始时间
    val min_date = if (month_start < before14date) month_start else before14date

    val startDate = new DateTime(min_date)
    val endDate = new DateTime(yesterdate)

    //获取截止到当前的所有时间间隔
    val days = Days.daysBetween(startDate, endDate).getDays()

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.autoBroadcastJoinThreshold", "83886080") //设置自动广播变量的大小阈值 80mb
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")
      .config("spark.dynamicAllocation.enabled", "true")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    //重跑最多30天数据，添加微信算当天和第二天多一个小时的添加，转化数据按成功拉取入库去卡时间
    for (i <- 0 to days) {
      val currentDate = startDate.plusDays(i)

      val date = currentDate.toString("yyyy-MM-dd")

      doMain(sparkSession, date)
    }

    sparkSession.stop()
  }

  def doMain(sparkSession: SparkSession, date: String) = {

    val yesterday = date.replace("-", "").toInt
    //    获取埋点数据
    getFrontendEvent(sparkSession,yesterday)

    getEventData(sparkSession, yesterday)

    getWechatData(sparkSession, date)

    getUserInfo(sparkSession, yesterday)

    getOrderInfo(sparkSession, date)

    result(sparkSession, date,yesterday)

    sparkSession.sql(
      s"""
         |INSERT OVERWRITE TABLE aws.user_pay_process_add_wechat_day PARTITION(day = "${yesterday}")
         |SELECT * FROM user_pay_process_add_wechat_tmp
      """.stripMargin)
  }

  def result(sparkSession: SparkSession, yesterdate: String,yesterday:Integer) = {

    val sql =
      s"""
        |SELECT t1.scene,
        |       t1.option,
        |       t1.get_operate_id operate_id,
        |       "引流" page_type,
        |       t1.task_id,
        |       t1.u_user get_entrance_user,
        |       t2.u_user click_entrance_user,
        |       t3.get_wechat_user,
        |       t3.add_wechat_user,
        |       t3.pull_wechat_user,
        |       t3.info_uuid,
        |       t4.grade,
        |       t4.gender,
        |       t4.regist_time,
        |       t4.user_attribution,
        |       t4.active_user_attribution,
        |       t4.city_class,
        |       t4.province,
        |       t4.city,
        |       t4.user_pay_status_statistics,
        |       t4.user_pay_status_business,
        |       t5.user_id paid_7d_user,
        |       t5.paid_7d_order_cnt,
        |       t5.paid_7d_amount,
        |       t6.user_id paid_14d_user,
        |       t6.paid_14d_order_cnt,
        |       t6.paid_14d_amount,
        |       t7.user_id paid_current_month_user,
        |       t7.paid_current_month_order_cnt,
        |       t7.paid_current_month_amount,
        |       t1.event_time,
        |       t1.team_name before_get_entrance_team_name,
        |       t8.scene_name,
        |       t8.clue_level_name,
        |       t8.resource_entrance_name,
        |       t8.type_name,
        |       t4.stage_name
        |FROM enter_data_tmp t1
        |LEFT JOIN click_data_tmp t2 ON t1.u_user = t2.u_user
        |AND t1.scene = t2.scene
        |AND t1.task_id = t2.task_id
        |AND t1.get_operate_id = t2.operate_id
        |LEFT JOIN wechat_tmp t3 ON t2.u_user = t3.get_wechat_user
        |AND t2.task_id = t3.channel_id
        |LEFT JOIN user_info_tmp t4 ON t1.u_user = t4.u_user
        |LEFT JOIN
        |  (SELECT user_id,
        |          count(1) paid_7d_order_cnt,
        |          sum(amount) paid_7d_amount
        |   FROM order_info_tmp
        |   WHERE to_date(pay_time) BETWEEN "$yesterdate" AND paid_7d
        |   GROUP BY user_id) t5 ON t3.pull_wechat_user = t5.user_id
        |LEFT JOIN
        |  (SELECT user_id,
        |          count(1) paid_14d_order_cnt,
        |          sum(amount) paid_14d_amount
        |   FROM order_info_tmp
        |   WHERE to_date(pay_time) BETWEEN "$yesterdate" AND paid_14d
        |   GROUP BY user_id) t6 ON t3.pull_wechat_user = t6.user_id
        |LEFT JOIN
        |  (SELECT user_id,
        |          count(1) paid_current_month_order_cnt,
        |          sum(amount) paid_current_month_amount
        |   FROM order_info_tmp
        |   WHERE to_date(pay_time) BETWEEN "$yesterdate" AND month_last_day
        |   GROUP BY user_id) t7 ON t3.pull_wechat_user = t7.user_id
        |left join (
        | select
        |    qr_code_created_at --渠道活码创建时间
        |    ,qr_code_id --渠道id
        |    ,scene_name --场景名称
        |    ,clue_level_name --渠道等级
        |    ,resource_entrance_name --渠道入口名称
        |    ,type_name -- 类型名称
        |    ,effective_time --生效时间
        |    ,invalid_time --失效时间
        |    ,row_number() OVER ( PARTITION BY qr_code_id  ORDER BY effective_time desc ) rn
        | from crm.qr_code_change_history --渠道活码历史变更记录表
        | where type_name <> '测试类型'
        | and deleted_at is null
        | and regexp_replace(substr(effective_time,1,10),'-','')<=${yesterday}
        | and regexp_replace(substr(invalid_time,1,10),'-','')>=${yesterday}
        | having rn=1  -- 取周期内有效的最后一个渠道的信息
        |) t8 on t1.task_id=t8.qr_code_id
        |
        |""".stripMargin

    sparkSession.sql(sql).createOrReplaceTempView("user_pay_process_add_wechat_tmp")
  }

  def getOrderInfo(sparkSession: SparkSession, yesterday: String) = {

    //订单属性信息
    val order_info_sql =
      s"""
         |SELECT user_id,
         |       order_id,
         |       pay_time,
         |       double(amount) amount,
         |       date_add("$yesterday", 7) paid_7d,
         |       date_add("$yesterday", 14) paid_14d,
         |       last_day("$yesterday") month_last_day
         |FROM aws.crm_order_info
         |WHERE is_test = false
         |  and in_salary = 1
         |  and worker_id <> 0
      """.stripMargin

    sparkSession.sql(order_info_sql).cache().createOrReplaceTempView("order_info_tmp")
  }

  def getUserInfo(sparkSession: SparkSession, yesterday: Int) = {

    //用户属性信息
    val user_info_sql =
      s"""
         |SELECT a.u_user,
         |       max(a.grade) grade,
         |       max(a.gender) gender,
         |       max(a.regist_time) regist_time,
         |       max(a.user_attribution) user_attribution,
         |       max(a.active_user_attribution) active_user_attribution,
         |       max(a.city_class) city_class,
         |       max(a.province) province,
         |       max(a.city) city,
         |       max(a.user_pay_status_statistics) user_pay_status_statistics,
         |       max(a.user_pay_status_business) user_pay_status_business,
         |       max(b.stage_name) stage_name
         |FROM dws.topic_user_active_detail_day a
         |left join dw.dim_grade b
         |on a.grade=b.grade_name
         |WHERE DAY = ${yesterday}
         |GROUP BY u_user
      """.stripMargin

    sparkSession.sql(user_info_sql).createOrReplaceTempView("user_info_tmp")
  }

  def getWechatData(sparkSession: SparkSession, yesterdate: String) = {

    //坐席信息
    val crm_user_sql =
      s"""
        |SELECT user_id,
        |       channel_id,
        |       external_user_id,
        |       worker_id,
        |       created_at,
        |       to_date("${yesterdate}") start_time,
        |       concat(date_add(to_date("$yesterdate"), 1), " 01:00:00") end_time
        |FROM crm.new_user
        |WHERE user_id != ""
        |  AND channel = 3
        |  AND created_at between to_date("${yesterdate}") and concat(date_add(to_date("${yesterdate}"), 1), " 01:00:00")
        |""".stripMargin

    sparkSession.sql(crm_user_sql).createOrReplaceTempView("crm_user_tmp")

    val wechat_sql =
      s"""
        |SELECT t1.user_id get_wechat_user,
        |       t1.channel_id,
        |       t2.user_id add_wechat_user,
        |       t3.user_id pull_wechat_user,
        |       t3.info_uuid
        |FROM
        |  (SELECT user_id,
        |          channel_id
        |   FROM crm_user_tmp
        |   WHERE to_date(created_at) = start_time
        |   GROUP BY user_id,
        |            channel_id) t1
        |LEFT JOIN
        |  (SELECT user_id,
        |          worker_id,
        |          external_user_id,
        |          channel_id
        |   FROM crm_user_tmp
        |   WHERE length(external_user_id) > 0
        |   GROUP BY user_id,
        |            worker_id,
        |            external_user_id,
        |            channel_id) t2 ON t1.user_id = t2.user_id
        |AND t1.channel_id = t2.channel_id
        |LEFT JOIN
        |  (SELECT user_id,
        |          worker_id,
        |          we_com_open_id,
        |          info_uuid
        |   FROM aws.clue_info
        |   WHERE created_at BETWEEN to_date("$yesterdate") AND concat(date_add(to_date("$yesterdate"), 1), " 01:00:00")
        |     AND length(we_com_open_id) > 0) t3 ON t2.user_id = t3.user_id
        |AND t2.external_user_id = t3.we_com_open_id
        |AND t2.worker_id = t3.worker_id
        |""".stripMargin

    sparkSession.sql(wechat_sql).createOrReplaceTempView("wechat_tmp")
  }

  def getEventData(sparkSession: SparkSession, yesterday: Int) = {

    //曝光
    val enter_data_sql =
      s"""
         |SELECT u_user,
         |       event_time,
         |       timestamp(event_time/1000 - 1) event_timestamp,
         |       task_id,
         |       scene,
         |       OPTION,
         |       event_key,
         |       get_operate_id
         |FROM
         |  (SELECT u_user,
         |          event_time,
         |          task_id,
         |          OPTION,
         |          scene,
         |          event_key,
         |          get_operate_id,
         |          task_id,
         |          row_number() OVER(PARTITION BY u_user, get_operate_id, scene, task_id
         |                            ORDER BY event_time) rk
         |   FROM
         |     (SELECT u_user,
         |             event_time,
         |             OPTION,
         |             task_id,
         |             scene,
         |             event_key,
         |             trim(get_operate_id) get_operate_id
         |      FROM
         |        (SELECT u_user,
         |                event_time,
         |                OPTION,
         |                task_id,
         |                scene,
         |                event_key,
         |                explode(split(coalesce(operate_ids[0], ""), ",")) get_operate_id
         |         FROM frontend_event
         |         WHERE DAY = ${yesterday}
         |           AND page_type IN ("引流",
         |                             "DRAINAGE")
         |           AND event_key = "get_PaySceneEntrance"
         |           AND event_type = "get"
         |           AND product_id in ( "01","127")
         |           AND os IN ("android",'harmony',
         |                      "ios","weapp")
         |           AND u_user IS NOT NULL
         |           AND u_user != ""
         |           AND task_id IS NOT NULL
         |           AND task_id != ""
         |           AND task_id != 'undefined') t
         |      UNION SELECT u_user,
         |                   event_time,
         |                   OPTION,
         |                   CASE
         |                       WHEN operate_id = '434754f3-c808-4c4c-b671-f0789db1d5d1' THEN 317
         |                       WHEN operate_id = 'a975d5a8-fd34-461b-9cfb-6720d5655c09' THEN 317
         |                       WHEN operate_id = '7f53a8d4-7c46-4d23-926f-709eaa24d6bb' THEN 316
         |                       WHEN operate_id = 'e908068c-7f74-4fca-9aa6-a7b8c995dbef' THEN 316
         |                       WHEN operate_id = '92ce6aa4-9932-4edf-a8cf-cbe4854b3ada' THEN 315
         |                       WHEN operate_id = '4ae78fed-9a98-4b7f-ac20-b10c8894cc38' THEN 315
         |                   END task_id,
         |                   scene,
         |                   event_key,
         |                   operate_id get_operate_id
         |      FROM
         |        (SELECT u_user,
         |                event_time,
         |                scene,
         |                OPTION,
         |                event_key,
         |                explode(split(operate_ids[0], ",")) operate_id,
         |                '引流' page_type
         |         FROM frontend_event
         |         WHERE DAY = ${yesterday}
         |           AND event_type = 'get'
         |           AND event_key = 'get_PaySceneEntrance'
         |           AND os IN ('android','harmony',
         |                      'ios','weapp')
         |           AND scene = 'shop-baozang-hotEnter'
         |           AND product_id in ( "01","127")
         |           AND length(u_user) > 0) t
         |      WHERE operate_id IN ('434754f3-c808-4c4c-b671-f0789db1d5d1',
         |                           'a975d5a8-fd34-461b-9cfb-6720d5655c09',
         |                           '7f53a8d4-7c46-4d23-926f-709eaa24d6bb',
         |                           'e908068c-7f74-4fca-9aa6-a7b8c995dbef',
         |                           '4ae78fed-9a98-4b7f-ac20-b10c8894cc38',
         |                           '92ce6aa4-9932-4edf-a8cf-cbe4854b3ada') ) tt
         |   WHERE get_operate_id != "") ttt
         |WHERE rk = 1
      """.stripMargin

    sparkSession.sql(enter_data_sql).createOrReplaceTempView("enter_data_distinct_tmp")

    //转换为date格式
    val yesterdate = intToStrDate(yesterday)

    //曝光前服务期
    sparkSession.sql(
      s"""
        |SELECT u_user,
        |       event_time,
        |       task_id,
        |       scene,
        |       OPTION,
        |       event_key,
        |       get_operate_id,
        |       team_name,
        |       row_number() OVER(PARTITION BY u_user,
        |                                      get_operate_id,
        |                                      scene,
        |                                      task_id
        |                         ORDER BY start_time DESC) rk
        |FROM
        |  (SELECT t1.u_user,
        |          t1.event_time,
        |          t1.event_timestamp,
        |          t1.task_id,
        |          t1.scene,
        |          t1.OPTION,
        |          t1.event_key,
        |          t1.get_operate_id,
        |          coalesce(t2.team_name, '无服务期') team_name,
        |          coalesce(t2.start_time, '9999-12-31') start_time
        |   FROM enter_data_distinct_tmp t1
        |   LEFT JOIN
        |     (SELECT t1.user_id,
        |             t2.name team_name,
        |             t1.start_time,
        |             t1.end_time
        |      FROM
        |        (SELECT user_id,
        |                team_id,
        |                start_time,
        |                end_time
        |         FROM user_allocation.user_allocation
        |         WHERE '$yesterdate' BETWEEN to_date(start_time) AND to_date(end_time)) t1
        |      INNER JOIN user_allocation.team t2 ON t1.team_id = t2.id
        |      UNION SELECT user_id,
        |                   '入校' AS team_name,
        |                   start_time,
        |                   end_time
        |      FROM usercore.attribution_log
        |      WHERE '$yesterdate' BETWEEN to_date(start_time) AND to_date(end_time)
        |        AND attribution = 'b') t2 ON t1.u_user = t2.user_id
        |   AND t1.event_timestamp BETWEEN t2.start_time AND t2.end_time) t
        |HAVING rk = 1
        |""".stripMargin).createOrReplaceTempView("enter_data_tmp")

    //点击
    val click_data_sql =
      s"""
         |SELECT u_user,
         |       event_time,
         |       scene,
         |       event_key,
         |       task_id,
         |       operate_id
         |FROM
         |  (SELECT u_user,
         |          event_time,
         |          scene,
         |          event_key,
         |          task_id,
         |          operate_id,
         |          row_number() OVER(PARTITION BY u_user, operate_id, scene, task_id
         |                            ORDER BY event_time) rk
         |   FROM
         |     (SELECT u_user,
         |             event_time,
         |             scene,
         |             event_key,
         |             task_id,
         |             trim(operate_id) operate_id
         |      FROM frontend_event
         |      WHERE DAY = ${yesterday}
         |        AND page_type IN ("引流",
         |                          "DRAINAGE")
         |        AND event_key = "click_PaySceneEntrance"
         |        AND event_type = "click"
         |        AND product_id in ( "01","127")
         |        AND os IN ("android",'harmony',
         |                   "ios","weapp")
         |        AND u_user IS NOT NULL
         |        AND u_user != ""
         |        AND task_id IS NOT NULL
         |        AND task_id != ""
         |      UNION SELECT u_user,
         |                   event_time,
         |                   scene,
         |                   event_key,
         |                   CASE
         |                       WHEN operate_id = '434754f3-c808-4c4c-b671-f0789db1d5d1' THEN 317
         |                       WHEN operate_id = 'a975d5a8-fd34-461b-9cfb-6720d5655c09' THEN 317
         |                       WHEN operate_id = '7f53a8d4-7c46-4d23-926f-709eaa24d6bb' THEN 316
         |                       WHEN operate_id = 'e908068c-7f74-4fca-9aa6-a7b8c995dbef' THEN 316
         |                       WHEN operate_id = '92ce6aa4-9932-4edf-a8cf-cbe4854b3ada' THEN 315
         |                       WHEN operate_id = '4ae78fed-9a98-4b7f-ac20-b10c8894cc38' THEN 315
         |                       ELSE ''
         |                   END task_id,
         |                   operate_id
         |      FROM frontend_event
         |      WHERE DAY = ${yesterday}
         |        AND event_type = 'click'
         |        AND event_key = 'click_PaySceneEntrance'
         |        AND os IN ('android','harmony',
         |                   'ios','weapp')
         |        AND scene = 'shop-baozang-hotEnter'
         |        AND product_id in ( "01","127")
         |        AND length(u_user) > 0
         |        AND operate_id IN ('434754f3-c808-4c4c-b671-f0789db1d5d1',
         |                           'a975d5a8-fd34-461b-9cfb-6720d5655c09',
         |                           '7f53a8d4-7c46-4d23-926f-709eaa24d6bb',
         |                           'e908068c-7f74-4fca-9aa6-a7b8c995dbef',
         |                           '4ae78fed-9a98-4b7f-ac20-b10c8894cc38',
         |                           '92ce6aa4-9932-4edf-a8cf-cbe4854b3ada') ) t) tt
         |WHERE rk = 1
      """.stripMargin

    sparkSession.sql(click_data_sql).createOrReplaceTempView("click_data_tmp")
  }


  def getFrontendEvent(sparkSession: SparkSession, yesterday: Int): Unit = {
    val sql =
      s"""
         |select
         |  u_user
         |  ,event_time
         |  ,scene
         |  ,OPTION
         |  ,event_type
         |  ,event_key
         |  ,operate_ids
         |  ,operate_id
         |  ,task_id
         |  ,os
         |  ,page_type
         |  ,product_id
         |  ,day
         |from events.frontend_event_orc
         |where day = ${yesterday}
         |      and event_type in ('click','get')
         |      and event_key in ('click_PaySceneEntrance','get_PaySceneEntrance')
         |""".stripMargin

    sparkSession.sql(sql).createOrReplaceTempView("frontend_event")
  }
}
