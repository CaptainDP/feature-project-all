package com.captain.bigdata.taichi.demo.app

import java.text.DecimalFormat
import java.util.Date

import com.captain.bigdata.taichi.util.DateUtil
import com.google.gson.GsonBuilder
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 用户排序特征
 */

case class FeatureBeanV2(device_id: String,
                         biz_type: String,
                         biz_id: String,
                         pvid: String,
                         posid: String,

                         //车系交叉特征
                         item_series_list_7d: String,
                         item_series_list_15d: String,
                         item_series_list_30d: String,

                         //车系交叉匹配特征结果字段
                         match_series_pv_maxmin_7d: String,
                         match_series_dur_maxmin_7d: String,
                         match_series_pv_weight_7d: String,
                         match_series_dur_weight_7d: String,
                         match_series_pv_7d: String,
                         match_series_dur_7d: String,
                         match_series_pv_maxmin_15d: String,
                         match_series_dur_maxmin_15d: String,
                         match_series_pv_weight_15d: String,
                         match_series_dur_weight_15d: String,
                         match_series_pv_15d: String,
                         match_series_dur_15d: String,
                         match_series_pv_maxmin_30d: String,
                         match_series_dur_maxmin_30d: String,
                         match_series_pv_weight_30d: String,
                         match_series_dur_weight_30d: String,
                         match_series_pv_30d: String,
                         match_series_dur_30d: String,


                         user_click_num_15d: String,
                         user_click_num_30d: String,
                         user_click_num_60d: String,
                         user_click_num_7d: String,
                         user_click_num_90d: String,
                         user_duration_15d: String,
                         user_duration_30d: String,
                         user_duration_60d: String,
                         user_duration_7d: String,
                         user_duration_90d: String,


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

object VideoFeatureColumnV2App {

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

  def getMaxMin(list: ArrayBuffer[Float], min: Float, max: Float): ArrayBuffer[Float] = {
    val diff = max - min + 1.0
    list.map(x => {
      val d = (x - min + 1.0) / diff
      d.toFloat
    })
  }

  def getSeriesRate(list: ArrayBuffer[Float], sum: Float): ArrayBuffer[Float] = {
    list.map(x => {
      x / sum
    })
  }

  case class MatchSeries(pvMaxMin: Float, durMaxMin: Float, pvRate: Float, durRate: Float, pv: Float, dur: Float)

  //userFeature.series_list_XX
  //source: 4693-2-17;771-2-4;770-12-114;614-19-34;2313-7-38;5373-1-6;314-36-355;1-2-119;4869-1-3;
  //_ser, _pv, _dur = source.split("-")
  def getSeriesList(column: String): mutable.HashMap[String, MatchSeries] = {
    val map = mutable.HashMap[String, MatchSeries]()
    if (column != null && !column.equals("")) {

      val _series = ArrayBuffer[String]()
      val _pvs = ArrayBuffer[Float]()
      val _durs = ArrayBuffer[Float]()

      //pv
      var pvSum = 0.0f
      var pvMin = 0.0f
      var pvMax = 0.0f

      //dur
      var durSum = 0.0f
      var durMin = 0.0f
      var durMax = 0.0f

      val series_list = column.split(";")
      series_list.foreach(x => {
        val ss = x.split("-")
        val _ser = ss(0)
        val _pv = ss(1).toString.toFloat
        val _dur = ss(2).toString.toFloat

        _series.append(_ser)
        _pvs.append(_pv)
        _durs.append(_dur)

        //pv
        pvSum += _pv.toString.toFloat
        if (_pv < pvMin) {
          pvMin = _pv
        }

        if (_pv > pvMax) {
          pvMax = _pv
        }

        //dur
        durSum += _dur.toString.toFloat
        if (_dur < durMin) {
          durMin = _dur
        }

        if (_dur > durMax) {
          durMax = _dur
        }

      }
      )

      val _pvsRate = getSeriesRate(_pvs, pvSum)
      val _dursRate = getSeriesRate(_pvs, durSum)

      val pvMaxMin = getMaxMin(_pvs, pvMin, pvMax)
      val durMaxMin = getMaxMin(_durs, durMin, durMax)

      for (i <- _series.indices) {
        map(_series(i)) = MatchSeries(pvMaxMin(i), durMaxMin(i), _pvsRate(i), _dursRate(i), _pvs(i), _durs(i))
      }

      map
    } else {
      map
    }
  }

  def get_match_series(uniq_series_ids: String, user_series_list_xx: String): MatchSeries = {

    if (uniq_series_ids != null && !uniq_series_ids.equals("")) {

      val user_series_list_xx_Map = getSeriesList(user_series_list_xx)

      val uniq_series_ids_list = uniq_series_ids.split(";")
      var match_series_pv_maxmin_xx = 0.0f
      var match_series_dur_maxmin_xx = 0.0f
      var match_series_pv_weight_xx = 0.0f
      var match_series_dur_weight_xx = 0.0f
      var match_series_pv_xx = 0.0f
      var match_series_dur_xx = 0.0f

      var count = 0
      for (i <- 0 until uniq_series_ids_list.length) {
        val oneMatchSeries = user_series_list_xx_Map(uniq_series_ids_list(i))
        if (oneMatchSeries != null) {
          match_series_pv_maxmin_xx += oneMatchSeries.pvMaxMin
          match_series_dur_maxmin_xx += oneMatchSeries.durMaxMin
          match_series_pv_weight_xx += oneMatchSeries.pvRate
          match_series_dur_weight_xx += oneMatchSeries.durRate
          match_series_pv_xx += oneMatchSeries.pv
          match_series_dur_xx += oneMatchSeries.dur
          count += 1
        }
      }

      if (count > 0) {
        match_series_pv_maxmin_xx = match_series_pv_maxmin_xx / count
        match_series_dur_maxmin_xx = match_series_dur_maxmin_xx / count
        match_series_pv_weight_xx = match_series_pv_weight_xx / count
        match_series_dur_weight_xx = match_series_dur_weight_xx / count
        match_series_pv_xx = match_series_pv_xx / count
        match_series_dur_xx = match_series_dur_xx / count
      }
      MatchSeries(match_series_pv_maxmin_xx, match_series_dur_maxmin_xx, match_series_pv_weight_xx, match_series_dur_weight_xx, match_series_pv_xx, match_series_dur_xx)

    } else {
      MatchSeries(0, 0, 0, 0, 0, 0)
    }
  }

  //source:
  //target:
  def log(column: String): String = {
    if (column != null && !column.equals("") && isNumeric(column)) {
      double2String(math.log(column.toDouble + 1))
    } else {
      "0.0"
    }
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

  //替换逗号为分号，并去重
  //source: 5772,5772,2061,5395,5772,4232,2893,5239,166
  //target: 5772;2061;5395;5772;4232;2893;5239;166
  def replaceComma2SemicolonDistinct(column: String): String = {
    if (column != null && !column.equals("")) {
      column.split(",").distinct.mkString(";")
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
        |
        |--deal_click_dur_log
        |get_json_object(msg, '$.userFeature.click_num_15d') as user_click_num_15d,
        |get_json_object(msg, '$.userFeature.click_num_30d') as user_click_num_30d,
        |get_json_object(msg, '$.userFeature.click_num_60d') as user_click_num_60d,
        |get_json_object(msg, '$.userFeature.click_num_7d') as user_click_num_7d,
        |get_json_object(msg, '$.userFeature.click_num_90d') as user_click_num_90d,
        |get_json_object(msg, '$.userFeature.duration_15d') as user_duration_15d,
        |get_json_object(msg, '$.userFeature.duration_30d') as user_duration_30d,
        |get_json_object(msg, '$.userFeature.duration_60d') as user_duration_60d,
        |get_json_object(msg, '$.userFeature.duration_7d') as user_duration_7d,
        |get_json_object(msg, '$.userFeature.duration_90d') as user_duration_90d,
        |
        |--deal_matcher
        |get_json_object(msg, '$.userFeature.series_list_7d') as item_series_list_7d,
        |get_json_object(msg, '$.userFeature.series_list_15d') as item_series_list_15d,
        |get_json_object(msg, '$.userFeature.series_list_30d') as item_series_list_30d,
        |
        |
        |
        |--新增类别特征
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
        |
        |
        |label,
        |dt
        |from dm_rca.dm_rca_train_sample_all_shucang_v2_filter_by_user
        |where dt<='currDate' and dt >='preDate'
        |and biz_type > 0 and biz_id > 0 and device_id is not null and biz_type in ('3','14','66')
        |""".stripMargin
    sql = sql.replaceAll("currDate", currDate)
    sql = sql.replaceAll("preDate", preDate)
    println("sql:" + sql)

    val tableName = "cmp_tmp_train_video_feature_v2"
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
    val rdd = df.as[FeatureBeanV2]

    //加工
    val featureResultRdd = rdd.map(x => {

      //deal_click_dur_log
      val user_click_num_15d = log(x.user_click_num_15d)
      val user_click_num_30d = log(x.user_click_num_30d)
      val user_click_num_60d = log(x.user_click_num_60d)
      val user_click_num_7d = log(x.user_click_num_7d)
      val user_click_num_90d = log(x.user_click_num_90d)
      val user_duration_15d = log(x.user_duration_15d)
      val user_duration_30d = log(x.user_duration_30d)
      val user_duration_60d = log(x.user_duration_60d)
      val user_duration_7d = log(x.user_duration_7d)
      val user_duration_90d = log(x.user_duration_90d)

      val item_uniq_series_ids = replaceComma2SemicolonDistinct(x.item_uniq_series_ids)

      //车系交叉特征
      val item_series_list_7d = getKeysFromSemicolonList(x.item_series_list_7d)
      val item_series_list_15d = getKeysFromSemicolonList(x.item_series_list_15d)
      val item_series_list_30d = getKeysFromSemicolonList(x.item_series_list_30d)
      val matchSeries7d = get_match_series(item_uniq_series_ids, item_series_list_7d)
      val matchSeries15d = get_match_series(item_uniq_series_ids, item_series_list_15d)
      val matchSeries30d = get_match_series(item_uniq_series_ids, item_series_list_30d)

      val match_series_pv_maxmin_7d = double2String(matchSeries7d.pvMaxMin)
      val match_series_dur_maxmin_7d = double2String(matchSeries7d.durMaxMin)
      val match_series_pv_weight_7d = double2String(matchSeries7d.pvRate)
      val match_series_dur_weight_7d = double2String(matchSeries7d.durRate)
      val match_series_pv_7d = double2String(matchSeries7d.pv)
      val match_series_dur_7d = double2String(matchSeries7d.dur)
      val match_series_pv_maxmin_15d = double2String(matchSeries15d.pvMaxMin)
      val match_series_dur_maxmin_15d = double2String(matchSeries15d.durMaxMin)
      val match_series_pv_weight_15d = double2String(matchSeries15d.pvRate)
      val match_series_dur_weight_15d = double2String(matchSeries15d.durRate)
      val match_series_pv_15d = double2String(matchSeries15d.pv)
      val match_series_dur_15d = double2String(matchSeries15d.dur)
      val match_series_pv_maxmin_30d = double2String(matchSeries30d.pvMaxMin)
      val match_series_dur_maxmin_30d = double2String(matchSeries30d.durMaxMin)
      val match_series_pv_weight_30d = double2String(matchSeries30d.pvRate)
      val match_series_dur_weight_30d = double2String(matchSeries30d.durRate)
      val match_series_pv_30d = double2String(matchSeries30d.pv)
      val match_series_dur_30d = double2String(matchSeries30d.dur)

      //新增类别特征
      val user_uniq_keywords_pref = getKeysFromSemicolonList(x.user_uniq_keywords_pref)
      val user_uniq_series_pref = getKeysFromSemicolonList(x.user_uniq_series_pref)
      val user_fp_click_series_seq = replaceComma2SemicolonDistinct(x.user_fp_click_series_seq)
      val item_uniq_keywords_name = replaceComma2SemicolonDistinct(x.item_uniq_keywords_name)
      val user_rt_fp_click_series_seq = replaceComma2SemicolonDistinct(x.user_rt_fp_click_series_seq)
      val user_uniq_category_pref = getKeysFromSemicolonList(x.user_uniq_category_pref)
      val item_uniq_category_name = replaceComma2SemicolonDistinct(x.item_uniq_category_name)
      val user_rt_category_list = replaceComma2SemicolonDistinct(x.user_rt_category_list)
      val item_author_id = x.item_author_id
      val user_rt_click_author_list_pre = replaceComma2SemicolonDistinct(x.user_rt_click_author_list_pre)
      val user_rt_click_tag_pref = replaceComma2SemicolonDistinct(x.user_rt_click_tag_pref)
      val user_device_model = replaceComma2SemicolonDistinct(x.user_device_model)
      val user_energy_pref_top1 = getKeysFromSemicolonList(x.user_energy_pref_top1)
      val recall_way = x.recall_way
      val gc_type = x.gc_type
      val rt_item_lst_list = replaceComma2SemicolonDistinct(x.rt_item_lst_list)
      val item_lst_list = replaceComma2SemicolonDistinct(x.item_lst_list)
      val item_key = replaceComma2SemicolonDistinct(x.item_key)

      FeatureBeanV2(
        //base
        x.device_id,
        x.biz_type,
        x.biz_id,
        x.pvid,
        x.posid,

        //deal_click_dur_log
        user_click_num_15d,
        user_click_num_30d,
        user_click_num_60d,
        user_click_num_7d,
        user_click_num_90d,
        user_duration_15d,
        user_duration_30d,
        user_duration_60d,
        user_duration_7d,
        user_duration_90d,

        //车系交叉特征
        item_series_list_7d,
        item_series_list_15d,
        item_series_list_30d,

        //车系交叉匹配特征结果字段
        match_series_pv_maxmin_7d,
        match_series_dur_maxmin_7d,
        match_series_pv_weight_7d,
        match_series_dur_weight_7d,
        match_series_pv_7d,
        match_series_dur_7d,
        match_series_pv_maxmin_15d,
        match_series_dur_maxmin_15d,
        match_series_pv_weight_15d,
        match_series_dur_weight_15d,
        match_series_pv_15d,
        match_series_dur_15d,
        match_series_pv_maxmin_30d,
        match_series_dur_maxmin_30d,
        match_series_pv_weight_30d,
        match_series_dur_weight_30d,
        match_series_pv_30d,
        match_series_dur_30d,


        //新增类别特征
        user_uniq_keywords_pref,
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

    val featuresListOutputReal = Array(

      //base
      "biz_id",
      "biz_type",
      "pvid",
      "device_id",
      "posid",

      //deal_click_dur_log
      "user_click_num_15d",
      "user_click_num_30d",
      "user_click_num_60d",
      "user_click_num_7d",
      "user_click_num_90d",
      "user_duration_15d",
      "user_duration_30d",
      "user_duration_60d",
      "user_duration_7d",
      "user_duration_90d",

      //车系交叉匹配特征结果字段
      "match_series_pv_maxmin_7d",
      "match_series_dur_maxmin_7d",
      "match_series_pv_weight_7d",
      "match_series_dur_weight_7d",
      "match_series_pv_7d",
      "match_series_dur_7d",
      "match_series_pv_maxmin_15d",
      "match_series_dur_maxmin_15d",
      "match_series_pv_weight_15d",
      "match_series_dur_weight_15d",
      "match_series_pv_15d",
      "match_series_dur_15d",
      "match_series_pv_maxmin_30d",
      "match_series_dur_maxmin_30d",
      "match_series_pv_weight_30d",
      "match_series_dur_weight_30d",
      "match_series_pv_30d",
      "match_series_dur_30d",


      //新增类别特征
      "user_uniq_keywords_pref",
      "user_uniq_series_pref",
      "user_fp_click_series_seq",
      "item_uniq_series_ids",
      "item_uniq_keywords_name",
      "user_rt_fp_click_series_seq",
      "user_uniq_category_pref",
      "item_uniq_category_name",
      "user_rt_category_list",
      "item_author_id",
      "user_rt_click_author_list_pre",
      "user_rt_click_tag_pref",
      "user_device_model",
      "user_energy_pref_top1",
      "recall_way",
      "gc_type",
      "biz_type",
      "rt_item_lst_list",
      "item_lst_list",
      "item_key",
      "label"
    )
    //    val columnList = "biz_id,biz_type,pvid,device_id,posid,user_click_num_15d,user_click_num_30d,user_click_num_60d,user_click_num_7d,user_click_num_90d,user_duration_15d,user_duration_30d,user_duration_60d,user_duration_7d,user_duration_90d,user_uniq_keywords_pref,user_uniq_series_pref,user_fp_click_series_seq,item_uniq_series_ids,item_uniq_keywords_name,user_rt_fp_click_series_seq,user_uniq_category_pref,item_uniq_category_name,user_rt_category_list,item_author_id,user_rt_click_author_list_pre,user_rt_click_tag_pref,user_device_model,user_energy_pref_top1,recall_way,gc_type,rt_item_lst_list,item_lst_list,item_key,label"
    //    val featuresListOutputReal = columnList.split(",")
    resultDF.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*).write.option("header", "true").option("emptyValue", "").mode("overwrite").csv(result_path)

    println("result_path:" + result_path)

    val sqlStr = "alter table dm_rca." + tableName + " add if not exists partition(dt='" + dt + "')"
    println("sqlStr: " + sqlStr)
    spark.sql(sqlStr)

    spark.stop()
  }

}
