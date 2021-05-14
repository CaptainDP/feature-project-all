package com.captain.bigdata.taichi.demo.app

import java.text.DecimalFormat

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import com.google.gson.GsonBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer


/**
 * 计算用户点击序列
 */
object UserClickSequenceApp {

  val isDebugJson = false

  val SERIES_IDS = "series_ids"
  val BIZ_TYPE = "biz_type"
  val AUTHOR_ID = "author_id"
  val UNIQ_CATEGORY_NAME = "uniq_category_name"
  val BRAND_IDS = "brand_ids"
  val BRAND_APPLE = "APPLE"
  val BRAND_HUAWEI = "HUAWEI"
  val BRAND_OTHER = "OTHER"
  val LIKE_COUNT = "like_count"
  val REPLY_COUNT = "reply_count"
  val param = """{"like_count":{"max":9911},"reply_count":{"max":6655}}""".stripMargin

  case class FeatureBean(device_id: String, biz_id: String, recommend_time: String, series_ids: String, biz_type: String, author_id: String, uniq_category_name: String, brand_ids: String, like_cnt_90d: String, reply_cnt_30d: String, device_brand: String, start_time: String, is_click: String, dt: String)

  case class FeatureResultBean(device_id: String, biz_id: String, biz_type: String, publish_time: String, match_series_weight: String, match_series_click_idx_weight: String, match_rtype_weight: String, match_rtype_click_idx_weight: String, match_author_weight: String, match_author_click_idx_weight: String, match_category_weight: String, match_category_click_idx_weight: String, match_brand_weight: String, match_brand_click_idx_weight: String, like_count: String, reply_count: String, device_brand_apple: String, device_brand_huawei: String, device_brand_other: String, current_hour: String, label: String)

  case class FeatureItemSeqBean(featureBean: FeatureBean, ItemSeqBean: Array[FeatureBean])

  def getString(x: FeatureBean): String = {
    x.dt + "," + x.device_id + "," + x.start_time + "," + x.biz_type + "," + x.biz_id + ","
  }

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

  def getNormalized(like_count: String, expectType: String): Double = {

    val json = JSON.parseObject(param)
    if (json.getJSONObject(expectType) == null || !isNumeric(like_count)) {
      0.0
    } else {
      val max = json.getJSONObject(expectType).getLongValue("max")

      if (like_count.toLong >= max) {
        1.0
      } else if (max <= 0) {
        0.0
      } else {
        1.0 * like_count.toLong / max
      }
    }

  }

  def getBrand(brand: String, expectBrand: String): Double = {
    if (expectBrand.equalsIgnoreCase(brand)) {
      1.0
    } else {
      0.0
    }
  }


  def getHour(start_time: String): Double = {
    val time = DateUtil.toDate(start_time, "yyyy-MM-dd HH:mm:ss.SSS")
    var hour = new DateTime(time).getHourOfDay
    if (hour < 0) {
      hour = 0
    } else if (hour > 23) {
      hour = 23
    }
    hour += 1
    Math.log10(hour) / Math.log10(24)
  }

  def getFieldValue(fieldName: String, featureBean: FeatureBean): String = {

    fieldName match {
      case SERIES_IDS => featureBean.series_ids
      case BIZ_TYPE => featureBean.biz_type
      case AUTHOR_ID => featureBean.author_id
      case UNIQ_CATEGORY_NAME => featureBean.uniq_category_name
      case BRAND_IDS => featureBean.brand_ids
    }
  }

  def getItemListByLabel(itemSeqBean: Array[FeatureBean], label: String): Array[FeatureBean] = {

    if (label.trim.equals("1")) {
      val newItemSeqBean = new ArrayBuffer[FeatureBean]()
      for (elem <- itemSeqBean) {
        if (elem.is_click.trim.equals("1")) {
          newItemSeqBean.append(elem)
        }
      }
      newItemSeqBean.toArray
    } else {
      itemSeqBean
    }
  }

  def getMatchSeriesWeight(series_ids: String, itemSeqBean: Array[FeatureBean], fieldName: String): Double = {

    var series_ids_list: Array[String] = null
    if (series_ids != null) {
      series_ids_list = series_ids.split(",")
    }

    if (series_ids_list == null || series_ids_list.isEmpty) {
      return 0.0
    }

    var count = 0
    for (elem <- itemSeqBean) {
      val tmp_series_ids = getFieldValue(fieldName, elem)
      if (tmp_series_ids != null) {
        val tmp_series_ids_list = tmp_series_ids.split(",")
        if (tmp_series_ids_list != null && tmp_series_ids_list.nonEmpty) {
          count += (tmp_series_ids_list.toSet & series_ids_list.toSet).size
        }
      }
    }

    count / (series_ids_list.length * itemSeqBean.length + 0.01)
  }

  def getMatchSeriesClickIdxWeight(series_ids: String, itemSeqBean: Array[FeatureBean], fieldName: String): Double = {
    var series_ids_list: Array[String] = null
    if (series_ids != null) {
      series_ids_list = series_ids.split(",")
    }

    if (series_ids_list == null || series_ids_list.isEmpty) {
      return 0.0
    }

    var i = 0
    for (elem <- itemSeqBean) {
      i += 1
      val tmp_series_ids = getFieldValue(fieldName, elem)
      if (tmp_series_ids != null) {
        val tmp_series_ids_list = tmp_series_ids.split(",")
        if (tmp_series_ids_list != null && tmp_series_ids_list.nonEmpty) {
          val count = (tmp_series_ids_list.toSet & series_ids_list.toSet).size
          if (count > 0) {
            return 1 - i / (itemSeqBean.length + 0.01)
          }
        }
      }
    }
    0
  }

  def main(args: Array[String]): Unit = {

    val currDate = "2021-05-01"
    val preCount = 0
    val date = DateUtil.toDate(currDate, "yyyy-MM-dd")
    val preDate = DateUtil.calcDateByFormat(date, "yyyy-MM-dd(-" + preCount + "D)")

    var sql =
      """select
        |device_id,
        |object_id as biz_id,
        |get_json_object(translate(item_feature,'\\;',''), '$.recommend_time') as recommend_time,
        |get_json_object(translate(item_feature,'\\;',''), '$.series_ids') as series_ids,
        |object_type as biz_type,
        |get_json_object(translate(item_feature,'\\;',''), '$.author_id') as author_id,
        |get_json_object(translate(item_feature,'\\;',''), '$.uniq_category_name') as uniq_category_name,
        |get_json_object(translate(item_feature,'\\;',''), '$.brand_ids') as brand_ids,
        |get_json_object(translate(item_feature,'\\;',''), '$.like_cnt_90d') as like_cnt_90d,
        |get_json_object(translate(item_feature,'\\;',''), '$.reply_cnt_30d') as reply_cnt_30d,
        |get_json_object(translate(user_feature,'\\;',''), '$.device_brand') as device_brand,
        |start_time as start_time,
        |is_click,
        |dt
        |from rdm.rdm_app_rcmd_ai_feature_di
        |where dt<='currDate' and dt >='preDate'
        |and object_type > 0 and object_id > 0 and device_id is not null and start_time is not null
        |""".stripMargin
    sql = sql.replaceAll("currDate", currDate)
    sql = sql.replaceAll("preDate", preDate)
    println("sql:" + sql)
    //        |and device_id = '0059C87C5D95374FD17C00DB2EFAA73D39597B3E'


    val tableName = "cmp_tmp_train_user_click_sequence_v1"
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

    val df = spark.sql(sql)
    val rdd = df.as[FeatureBean].rdd.map(x => (x.device_id, x))
    //    rdd.aggregateByKey()
    val groupRdd = rdd.groupByKey()
    val groupSeqRdd = groupRdd.map(x => {
      val lineBuffer = x._2.toBuffer
      val sortByTimeBuffer = lineBuffer.sortBy(_.start_time)
      val itemSeqList = new ArrayBuffer[FeatureItemSeqBean]()
      for (i <- sortByTimeBuffer.indices if sortByTimeBuffer(i).dt.equals(currDate)) {
        val oneSeq = new ArrayBuffer[FeatureBean]()
        val max = Math.min(i, 30)
        val min = i - max
        for (j <- (min until i).reverse) {
          val itemJ = sortByTimeBuffer(j)
          oneSeq += itemJ
        }
        if (oneSeq.nonEmpty) {
          itemSeqList.append(FeatureItemSeqBean(sortByTimeBuffer(i), oneSeq.toArray))
        }
      }
      itemSeqList
    }).flatMap(x => x)

    //加工
    val featureResultRdd = groupSeqRdd.map(x => {

      val featureBean = x.featureBean
      val itemSeqBean = getItemListByLabel(x.ItemSeqBean, featureBean.is_click)
      val publish_time = getTimeDecay(featureBean.recommend_time, featureBean.start_time)
      val match_series_weight = getMatchSeriesWeight(featureBean.series_ids, itemSeqBean, SERIES_IDS)
      val match_series_click_idx_weight = getMatchSeriesClickIdxWeight(featureBean.series_ids, itemSeqBean, SERIES_IDS)
      val match_rtype_weight = getMatchSeriesWeight(featureBean.biz_type, itemSeqBean, BIZ_TYPE)
      val match_rtype_click_idx_weight = getMatchSeriesClickIdxWeight(featureBean.biz_type, itemSeqBean, BIZ_TYPE)
      val match_author_weight = getMatchSeriesWeight(featureBean.author_id, itemSeqBean, AUTHOR_ID)
      val match_author_click_idx_weight = getMatchSeriesClickIdxWeight(featureBean.author_id, itemSeqBean, AUTHOR_ID)
      val match_category_weight = getMatchSeriesWeight(featureBean.uniq_category_name, itemSeqBean, UNIQ_CATEGORY_NAME)
      val match_category_click_idx_weight = getMatchSeriesClickIdxWeight(featureBean.uniq_category_name, itemSeqBean, UNIQ_CATEGORY_NAME)
      val match_brand_weight = getMatchSeriesWeight(featureBean.brand_ids, itemSeqBean, BRAND_IDS)
      val match_brand_click_idx_weight = getMatchSeriesClickIdxWeight(featureBean.brand_ids, itemSeqBean, BRAND_IDS)
      val like_count = getNormalized(featureBean.like_cnt_90d, LIKE_COUNT)
      val reply_count = getNormalized(featureBean.reply_cnt_30d, REPLY_COUNT)
      val device_brand_apple = getBrand(featureBean.device_brand, BRAND_APPLE)
      val device_brand_huawei = getBrand(featureBean.device_brand, BRAND_HUAWEI)
      val device_brand_other = if (device_brand_apple > 0 || device_brand_huawei > 0) 0 else 1
      val current_hour = getHour(featureBean.start_time)
      val label = featureBean.is_click.toInt

      FeatureResultBean(featureBean.device_id, featureBean.biz_id, featureBean.biz_type, double2String(publish_time), double2String(match_series_weight),
        double2String(match_series_click_idx_weight), double2String(match_rtype_weight), double2String(match_rtype_click_idx_weight), double2String(match_author_weight), double2String(match_author_click_idx_weight),
        double2String(match_category_weight), double2String(match_category_click_idx_weight), double2String(match_brand_weight), double2String(match_brand_click_idx_weight),
        double2String(like_count), double2String(reply_count), double2String(device_brand_apple), double2String(device_brand_huawei), double2String(device_brand_other), double2String(current_hour), double2String(label))
    })


    //是否输出json中间数据
    if (isDebugJson) {
      groupSeqRdd.map(x => {
        val gson = new GsonBuilder().serializeSpecialFloatingPointValues().create()
        gson.toJson(x)
      }).toDF().write.option("header", "true").mode("overwrite").csv(result_path_json)
    }

    val resultDF = featureResultRdd.toDF()
    val columnList = "biz_id,biz_type,device_id,publish_time,match_series_weight,match_series_click_idx_weight,match_rtype_weight,match_rtype_click_idx_weight,match_author_weight,match_author_click_idx_weight,match_category_weight,match_category_click_idx_weight,match_brand_weight,match_brand_click_idx_weight,like_count,reply_count,device_brand_apple,device_brand_huawei,device_brand_other,current_hour,label"
    val featuresListOutputReal = columnList.split(",")
    resultDF.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*).write.option("header", "true").mode("overwrite").csv(result_path)

    spark.stop()
  }

}
