package com.captain.bigdata.taichi.demo.app

import java.text.DecimalFormat
import java.util.Date

import com.captain.bigdata.taichi.util.DateUtil
import com.google.gson.GsonBuilder
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 用户排序特征
 */

case class FeatureBean(device_id: String, biz_type: String, biz_id: String, pvid: String, posid: String, user_uniq_keywords_pref: String, user_uniq_series_pref: String, user_fp_click_series_seq: String, label: String, dt: String)

case class FeatureResultBean(device_id: String, biz_type: String, biz_id: String, pvid: String, posid: String, user_uniq_keywords_pref: String, user_uniq_series_pref: String, user_fp_click_series_seq: String, label: String, dt: String)

object VideoFeatureColumnApp {

  val isDebugJson = true

  def getTimeDecay(start: String, end: String): Double = {
    if (start == null || start.trim.equals("")) {
      0
    } else {
      val startTime = DateUtil.toDate(start, "yyyy-MM-dd HH:mm:ss")
      val startTimeStr = DateUtil.getDateTime(startTime, "yyyy-MM-dd HH:mm:ss.SSS")
      val diff = DateUtil.getTimeDiff(end, startTimeStr, "yyyy-MM-dd HH:mm:ss.SSS")
      Math.exp(-0.0005 * diff / 600.0)
    }
  }

  def isNumeric(str: String): Boolean = {
    var flag = false
    try {
      java.lang.Double.parseDouble(str);
      flag = true
    } catch {
      case e: Exception => flag = false
    }
    flag
  }

  def double2String(value: Double): String = {
    new DecimalFormat("##.######").format(value)
  }


  //source: WEY:0.181368;坦克300:0.231121;本田CR-V新能源:0.11664;
  //target: WEY;坦克300;本田CR-V新能源;
  def getKeysFromSemicolonList(column: String): String = {
    if (column != null && !column.equals("")) {
      val list1 = column.split(";")
      val list2 = list1.map(x => x.split(":")(0))
      list2.mkString(";")
    } else {
      column
    }
  }

  //source: 5772,2061,5395,5772,4232,2893,5239,166
  //target: 5772;2061;5395;5772;4232;2893;5239;166
  def replaceComma2Semicolon(column: String): String = {
    if (column != null && !column.equals("")) {
      column.replaceAll(",", ";")
    } else {
      column
    }
  }


  def main(args: Array[String]): Unit = {

    println("args:" + args.mkString(","))

    val options = new Options
    options.addOption("d", true, "date yyyy-MM-dd [default yesterday]")
    options.addOption("n", true, "preDateNum")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    //date
    var dt = DateUtil.getDate(new Date(), "yyyy-MM-dd")
    if (cmd.hasOption("d")) {
      dt = cmd.getOptionValue("d")
    }

    var num = 0
    if (cmd.hasOption("n")) {
      num = cmd.getOptionValue("n").toInt
    }

    val currDate = dt
    val preCount = num
    val date = DateUtil.toDate(currDate, "yyyy-MM-dd")
    val preDate = DateUtil.calcDateByFormat(date, "yyyy-MM-dd(-" + preCount + "D)")

    var sql =
      """select
        |device_id,
        |biz_type,
        |biz_id,
        |pvid,
        |posid,
        |get_json_object(msg, '$.userFeature.uniq_keywords_pref') as user_uniq_keywords_pref,
        |get_json_object(msg, '$.userFeature.uniq_series_pref') as user_uniq_series_pref,
        |get_json_object(msg, '$.userFeature.fp_click_series_seq') as user_fp_click_series_seq,
        |label,
        |dt
        |from dm_rca.dm_rca_train_sample_all_shucang_v2_filter_by_user
        |where dt<='currDate' and dt >='preDate'
        |and biz_type > 0 and biz_id > 0 and device_id is not null and biz_type in ('3','14','66') limit 10
        |""".stripMargin
    sql = sql.replaceAll("currDate", currDate)
    sql = sql.replaceAll("preDate", preDate)
    println("sql:" + sql)

    val tableName = "cmp_tmp_train_video_feature_v1"
    val result_path_json = "viewfs://AutoLfCluster/team/cmp/hive_db/tmp/" + tableName + "_json/dt=" + currDate
    val result_path = "viewfs://AutoLfCluster/team/cmp/hive_db/tmp/" + tableName + "/dt=" + currDate

    val sparkConf = new SparkConf();
    //sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .enableHiveSupport
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("getKeysFromSemicolonList", getKeysFromSemicolonList _)

    val df = spark.sql(sql)
    val rdd = df.as[FeatureBean]

    //加工
    val featureResultRdd = rdd.map(x => {

      val user_uniq_keywords_pref = getKeysFromSemicolonList(x.user_uniq_keywords_pref)
      val user_uniq_series_pref = getKeysFromSemicolonList(x.user_uniq_series_pref)
      val user_fp_click_series_seq = replaceComma2Semicolon(x.user_fp_click_series_seq)

      FeatureResultBean(x.device_id, x.biz_type, x.biz_id, x.pvid, x.posid, user_uniq_keywords_pref, user_uniq_series_pref, user_fp_click_series_seq, x.label, x.dt)

    })


    //是否输出json中间数据
    if (isDebugJson) {
      featureResultRdd.map(x => {
        val gson = new GsonBuilder().serializeSpecialFloatingPointValues().create()
        gson.toJson(x)
      }).toDF().write.option("header", "true").mode("overwrite").csv(result_path_json)
      println("result_path_json:" + result_path_json)
    }

    val resultDF = featureResultRdd.toDF()
    val columnList = "biz_id,biz_type,device_id,posid,user_uniq_keywords_pref,user_uniq_series_pref,user_fp_click_series_seq,label"
    val featuresListOutputReal = columnList.split(",")
    resultDF.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*).write.option("header", "true").mode("overwrite").csv(result_path)
    println("result_path:" + result_path)

    spark.stop()
  }

}
