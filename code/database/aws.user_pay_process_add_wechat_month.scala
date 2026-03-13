package com.onion.etl.aws

import com.onion.etl.util.DateUitil
import com.onion.etl.util.DateUitil.{getBeforeXDay, intToStrDate}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days}

/**
  * Created by 李华雄 on 2025/11/19 12:19
  */
object UserPayProcessAddWechatMonth {

  def main(args: Array[String]): Unit = {

    //传入昨天日期
    val yesterday = args(0).toInt

    //转换为date格式
    val yesterdate = intToStrDate(yesterday)

    //取当前日期所在月的第一天
    val month_start = yesterdate.substring(0, 8) + "01"

    val month_str = args(0).substring(0, 6)
    val month = month_str.toInt
    val dateStr = DateUitil.getMonthDate(month_str)
    val start = Integer.parseInt(dateStr(0)) //月开始日期，比如20181201
    val end = Integer.parseInt(dateStr(1)) //月结束日期，比如20181231





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
    doMain(sparkSession, start,end,month)


    sparkSession.stop()
  }

  def doMain(sparkSession: SparkSession, start:Integer, end:Integer,month:Integer) = {

    //    获取埋点数据
    getFrontendEvent(sparkSession,start,end)

    getEventData(sparkSession,start,end)

    getWechatData(sparkSession,start,end)

    getUserInfo(sparkSession,start,end)

    getOrderInfo(sparkSession,start,end)

    result(sparkSession,start,end)

    sparkSession.sql(
      s"""
         |INSERT OVERWRITE TABLE aws.user_pay_process_add_wechat_month PARTITION(month = ${month})
         |SELECT * FROM user_pay_process_add_wechat_tmp
      """.stripMargin)
  }

  def result(sparkSession: SparkSession, start:Integer, end:Integer) = {

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
        |       t5.user_id paid_current_month_user,
        |       t5.paid_order_cnt paid_current_month_order_cnt,
        |       t5.paid_amount paid_current_month_amount,
        |       t1.event_time,
        |       t1.team_name as before_get_entrance_team_name,
        |       t6.scene_name,
        |       t6.clue_level_name,
        |       t6.resource_entrance_name,
        |       t6.type_name,
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
        |          count(1) paid_order_cnt,
        |          sum(amount) paid_amount
        |   FROM order_info_tmp
        |   GROUP BY user_id) t5 ON t3.pull_wechat_user = t5.user_id
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
        |    ,row_number() OVER ( PARTITION BY qr_code_id  ORDER BY effective_time desc) rn
        | from crm.qr_code_change_history --渠道活码历史变更记录表
        | where type_name <> '测试类型'
        | and deleted_at is null
        | and regexp_replace(substr(effective_time,1,10),'-','')<=${end}
        | and regexp_replace(substr(invalid_time,1,10),'-','')>=${start}
        | having rn=1  -- 取周期内有效的最后一个渠道的信息
        |) t6 on t1.task_id=t6.qr_code_id
        |""".stripMargin

    sparkSession.sql(sql).createOrReplaceTempView("user_pay_process_add_wechat_tmp")
  }

  def getOrderInfo(sparkSession: SparkSession, start:Integer, end:Integer) = {

    val start_format = intToStrDate(start)
    val end_format = intToStrDate(end)
    //订单属性信息
    val order_info_sql =
      s"""
         |SELECT user_id,
         |       order_id,
         |       pay_time,
         |       double(amount) amount
         |FROM aws.crm_order_info
         |WHERE is_test = false
         |  and in_salary = 1
         |  and worker_id <> 0
         |  and to_date(pay_time) BETWEEN to_date("$start_format") and  to_date("$end_format")
      """.stripMargin

    sparkSession.sql(order_info_sql).cache().createOrReplaceTempView("order_info_tmp")
  }

  def getUserInfo(sparkSession: SparkSession, start:Integer, end:Integer) = {

    //用户属性信息
    val user_info_sql =
      s"""
         |SELECT  a.u_user
         |       ,a.grade
         |       ,a.gender
         |       ,a.regist_time
         |       ,a.user_attribution
         |       ,a.active_user_attribution
         |       ,a.city_class
         |       ,a.province
         |       ,a.city
         |       ,a.user_pay_status_statistics
         |       ,a.user_pay_status_business
         |       ,b.stage_name
         |       ,row_number() over(PARTITION BY u_user order by day desc ) rn
         |FROM dws.topic_user_active_detail_day a
         |left join dw.dim_grade b
         |on a.grade=b.grade_name
         |WHERE DAY >= ${start} and DAY<=${end}
         |HAVING rn =1
      """.stripMargin

    sparkSession.sql(user_info_sql).createOrReplaceTempView("user_info_tmp")
  }

  def getWechatData(sparkSession: SparkSession, start:Integer, end:Integer) = {
    val start_format = intToStrDate(start)
    val end_format = intToStrDate(end)
    //坐席信息
    val crm_user_sql =
      s"""
        |SELECT user_id,
        |       channel_id,
        |       external_user_id,
        |       worker_id,
        |       created_at,
        |       to_date("${start_format}") start_time,
        |       concat(date_add(to_date("$end_format"), 1), " 01:00:00") end_time
        |FROM crm.new_user
        |WHERE user_id != ""
        |  AND channel = 3
        |  AND created_at between to_date("${start_format}") and concat(date_add(to_date("${end_format}"), 1), " 01:00:00")
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
        |   WHERE substr(created_at,1,7) = substr(start_time,1,7)
        |   GROUP BY user_id,
        |            channel_id) t1
        |LEFT JOIN
        |  (SELECT user_id,
        |          channel_id
        |   FROM crm_user_tmp
        |   WHERE length(external_user_id) > 0
        |   GROUP BY user_id,
        |            channel_id) t2 ON t1.user_id = t2.user_id
        |AND t1.channel_id = t2.channel_id
        |LEFT JOIN
        |  (SELECT user_id,
        |          qr_code_channel_id channel_id,
        |          info_uuid,
        |          row_number() over(PARTITION BY user_id,qr_code_channel_id order by created_at ) rn
        |   FROM aws.clue_info
        |   WHERE created_at BETWEEN to_date("$start_format") AND concat(date_add(to_date("$end_format"), 1), " 01:00:00")
        |     AND length(we_com_open_id) > 0
        |     and clue_source in ('WeCom','building_blocks_goods_wecom')
        |   having rn=1
        |     ) t3 ON t2.user_id = t3.user_id  and t2.channel_id=t3.channel_id
        |""".stripMargin

    sparkSession.sql(wechat_sql).createOrReplaceTempView("wechat_tmp")
  }

  def getEventData(sparkSession: SparkSession, start:Integer, end:Integer) = {

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
         |         WHERE  page_type IN ("引流",
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
         |         WHERE event_type = 'get'
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
    val start_format = intToStrDate(start)
    val end_format = intToStrDate(end)

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
         |     (
         |        SELECT t1.user_id,
         |               t2.name team_name,
         |               t1.start_time,
         |               t1.end_time
         |         FROM
         |          (SELECT user_id,
         |                  team_id,
         |                  start_time,
         |                  end_time
         |           FROM user_allocation.user_allocation
         |           WHERE '${end_format}'>=to_date(start_time)
         |            AND to_date(end_time)>='${start_format}'
         |            ) t1
         |         INNER JOIN user_allocation.team t2 ON t1.team_id = t2.id
         |      UNION
         |          SELECT user_id,
         |                   '入校' AS team_name,
         |                   start_time,
         |                   end_time
         |      FROM usercore.attribution_log
         |      WHERE to_date(start_time)<='${end_format}'
         |            AND to_date(end_time)>='${start_format}'
         |      ) t2 ON t1.u_user = t2.user_id
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
         |      WHERE page_type IN ("引流",
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
         |      WHERE event_type = 'click'
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


  def getFrontendEvent(sparkSession: SparkSession, start:Integer, end:Integer): Unit = {
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
         |where day >= ${start} and day<=${end}
         |      and event_type in ('click','get')
         |      and event_key in ('click_PaySceneEntrance','get_PaySceneEntrance')
         |""".stripMargin

    sparkSession.sql(sql).createOrReplaceTempView("frontend_event")
  }
}
