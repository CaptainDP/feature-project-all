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

case class FeatureBean(device_id: String,
                       biz_type: String,
                       biz_id: String,
                       pvid: String,
                       posid: String,
                       user_uniq_keywords_pref: String,
                       user_uniq_series_pref: String,
                       user_fp_click_series_seq: String,
                       item_uniq_series_ids: String,
                       item_uniq_keywords_name: String,
                       user_rt_fp_click_series_seq: String,
                       user_uniq_category_pref: String,
                       item_uniq_category_name: String,
                       user_rt_category_list: String,
                       item_author_id: String,
                       user_rt_click_author_list_pre: String,
                       user_rt_click_tag_pref: String,
                       user_device_model: String,
                       user_energy_pref_top1: String,
                       recall_way: String,
                       gc_type: String,
                       rt_item_lst_list: String,
                       item_lst_list: String,
                       item_key: String,
                       label: String,
                       dt: String)

case class FeatureResultBean(device_id: String,
                             biz_type: String,
                             biz_id: String,
                             pvid: String,
                             posid: String,
                             user_uniq_keywords_pref: String,
                             user_uniq_series_pref: String,
                             user_fp_click_series_seq: String,
                             item_uniq_series_ids: String,
                             item_uniq_keywords_name: String,
                             user_rt_fp_click_series_seq: String,
                             user_uniq_category_pref: String,
                             item_uniq_category_name: String,
                             user_rt_category_list: String,
                             item_author_id: String,
                             user_rt_click_author_list_pre: String,
                             user_rt_click_tag_pref: String,
                             user_device_model: String,
                             user_energy_pref_top1: String,
                             recall_way: String,
                             gc_type: String,
                             rt_item_lst_list: String,
                             item_lst_list: String,
                             item_key: String,
                             label: String,
                             dt: String)

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

  //source: "78,496,2246"
  //target: 78,496,2246
  def replaceQuotes(column: String): String = {
    if (column != null && !column.equals("")) {
      column.replaceAll("\"", "")
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
        |get_json_object(msg, '$.itemFeature.uniq_series_ids') as item_uniq_series_ids,
        |get_json_object(msg, '$.itemFeature.uniq_keywords_name') as item_uniq_keywords_name,
        |get_json_object(msg, '$.userFeature.rt_fp_click_series_seq') as user_rt_fp_click_series_seq,
        |get_json_object(msg, '$.userFeature.uniq_category_pref') as user_uniq_category_pref,
        |get_json_object(msg, '$.itemFeature.uniq_category_name') as item_uniq_category_name,
        |get_json_object(msg, '$.userFeature.rt_category_list') as user_rt_category_list,
        |get_json_object(msg, '$.itemFeature.author_id') as item_author_id,
        |get_json_object(msg, '$.userFeature.rt_click_author_list_pre') as user_rt_click_author_list_pre,
        |get_json_object(msg, '$.userFeature.rt_click_tag_pref') as user_rt_click_tag_pref,
        |get_json_object(msg, '$.userFeature.device_model') as user_device_model,
        |get_json_object(msg, '$.userFeature.energy_pref') as user_energy_pref_top1,
        |get_json_object(msg, '$.itemFeature.recall_way') as recall_way,
        |get_json_object(msg, '$.itemFeature.gc_type') as gc_type,
        |get_json_object(msg, '$.itemFeature.rt_item_lst_list') as rt_item_lst_list,
        |get_json_object(msg, '$.itemFeature.item_lst_list') as item_lst_list,
        |concat(biz_type,'-',biz_id) as item_key,
        |label,
        |dt
        |from dm_rca.dm_rca_train_sample_all_shucang_v2_filter_by_user
        |where dt<='currDate' and dt >='preDate'
        |and biz_type > 0 and biz_id > 0 and device_id is not null and biz_type in ('3','14','66')
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
      val item_uniq_series_ids = replaceComma2Semicolon(x.item_uniq_series_ids)
      val item_uniq_keywords_name = replaceComma2Semicolon(x.item_uniq_keywords_name)
      val user_rt_fp_click_series_seq = replaceComma2Semicolon(x.user_rt_fp_click_series_seq)
      val user_uniq_category_pref = getKeysFromSemicolonList(x.user_uniq_category_pref)
      val item_uniq_category_name = replaceComma2Semicolon(x.item_uniq_category_name)
      val user_rt_category_list = replaceComma2Semicolon(x.user_rt_category_list)
      val item_author_id = x.item_author_id
      val user_rt_click_author_list_pre = replaceComma2Semicolon(x.user_rt_click_author_list_pre)
      val user_rt_click_tag_pref = replaceComma2Semicolon(x.user_rt_click_tag_pref)
      val user_device_model = replaceComma2Semicolon(x.user_device_model)
      val user_energy_pref_top1 = getKeysFromSemicolonList(x.user_energy_pref_top1)
      val recall_way = x.recall_way
      val gc_type = x.gc_type
      val rt_item_lst_list = replaceComma2Semicolon(x.rt_item_lst_list)
      val item_lst_list = replaceComma2Semicolon(x.item_lst_list)
      val item_key = replaceComma2Semicolon(x.item_key)

      FeatureResultBean(x.device_id, x.biz_type, x.biz_id, x.pvid, x.posid, user_uniq_keywords_pref,
        user_uniq_series_pref,
        user_fp_click_series_seq,
        item_uniq_series_ids,
        item_uniq_keywords_name,
        user_rt_fp_click_series_seq,
        user_uniq_category_pref,
        item_uniq_category_name,
        user_rt_category_list,
        item_author_id,
        user_rt_click_author_list_pre,
        user_rt_click_tag_pref,
        user_device_model,
        user_energy_pref_top1,
        recall_way,
        gc_type,
        rt_item_lst_list,
        item_lst_list,
        item_key,
        x.label,
        x.dt)

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
    val columnList = "biz_id,biz_type,pvid,device_id,posid,user_uniq_keywords_pref,user_uniq_series_pref,user_fp_click_series_seq,item_uniq_series_ids,item_uniq_keywords_name,user_rt_fp_click_series_seq,user_uniq_category_pref,item_uniq_category_name,user_rt_category_list,item_author_id,user_rt_click_author_list_pre,user_rt_click_tag_pref,user_device_model,user_energy_pref_top1,recall_way,gc_type,rt_item_lst_list,item_lst_list,item_key,label"
    val featuresListOutputReal = columnList.split(",")
    resultDF.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*).write.option("header", "true").option("emptyValue", "").mode("overwrite").csv(result_path)

    println("result_path:" + result_path)

    spark.stop()
  }

}
