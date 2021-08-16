package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.captain.bigdata.taichi.demo.bean.FeatureBeanV2
import com.captain.bigdata.taichi.demo.output.VideoFeatureColumnV2Output
import com.captain.bigdata.taichi.demo.sql.VideoFeatureColumnV2Sql
import com.captain.bigdata.taichi.demo.utils.VideoFeatureColumnV2Utils._
import com.captain.bigdata.taichi.util.DateUtil
import com.google.gson.GsonBuilder
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 用户排序特征
 */


object VideoFeatureColumnV2App {

  val isDebugJson = true


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

    var sql = VideoFeatureColumnV2Sql.sql
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

      //pv-uv
      val item_callback_ratio_1d = callback_ratio(x.pv_1d, x.uv_1d)
      val item_callback_ratio_3d = callback_ratio(x.pv_3d, x.uv_3d)
      val item_callback_ratio_7d = callback_ratio(x.pv_7d, x.uv_7d)
      val item_callback_ratio_15d = callback_ratio(x.pv_15d, x.uv_15d)
      val item_callback_ratio_30d = callback_ratio(x.pv_30d, x.uv_30d)

      //click_pv ratio
      val item_click_pv_wilson_1_3d = wilson_score(x.click_pv_1d, x.click_pv_3d)
      val item_click_pv_wilson_1_7d = wilson_score(x.click_pv_1d, x.click_pv_7d)
      val item_click_pv_wilson_1_15d = wilson_score(x.click_pv_1d, x.click_pv_15d)
      val item_click_pv_wilson_1_30d = wilson_score(x.click_pv_1d, x.click_pv_30d)
      val item_click_pv_wilson_3_7d = wilson_score(x.click_pv_3d, x.click_pv_7d)
      val item_click_pv_wilson_3_15d = wilson_score(x.click_pv_3d, x.click_pv_15d)
      val item_click_pv_wilson_3_30d = wilson_score(x.click_pv_3d, x.click_pv_30d)

      //collect ratio
      val item_collect_uv_1d = wilson_score(x.collect_cnt_1d, x.click_uv_1d)
      val item_collect_uv_3d = wilson_score(x.collect_cnt_3d, x.click_uv_3d)
      val item_collect_uv_7d = wilson_score(x.collect_cnt_7d, x.click_uv_7d)
      val item_collect_uv_15d = wilson_score(x.collect_cnt_15d, x.click_uv_15d)
      val item_collect_uv_30d = wilson_score(x.collect_cnt_30d, x.click_uv_30d)

      //sight_pv ratio
      val item_sight_pv_wilson_1_3d = wilson_score(x.sight_pv_1d, x.sight_pv_3d)
      val item_sight_pv_wilson_1_7d = wilson_score(x.sight_pv_1d, x.sight_pv_7d)
      val item_sight_pv_wilson_1_15d = wilson_score(x.sight_pv_1d, x.sight_pv_15d)
      val item_sight_pv_wilson_1_30d = wilson_score(x.sight_pv_1d, x.sight_pv_30d)
      val item_sight_pv_wilson_3_7d = wilson_score(x.sight_pv_3d, x.sight_pv_7d)
      val item_sight_pv_wilson_3_15d = wilson_score(x.sight_pv_3d, x.sight_pv_15d)
      val item_sight_pv_wilson_3_30d = wilson_score(x.sight_pv_3d, x.sight_pv_30d)

      //frontpage agee ctr
      val item_frontpage_agee_pv_ctr_1d = agee_score(x.click_pv_1d, 5, x.sight_pv_1d, 50)
      val item_frontpage_agee_pv_ctr_3d = agee_score(x.click_pv_1d, 8, x.sight_pv_1d, 80)
      val item_frontpage_agee_pv_ctr_7d = agee_score(x.click_pv_1d, 11, x.sight_pv_1d, 115)
      val item_frontpage_agee_pv_ctr_15d = agee_score(x.click_pv_1d, 21, x.sight_pv_1d, 220)
      val item_frontpage_agee_pv_ctr_30d = agee_score(x.click_pv_1d, 85, x.sight_pv_1d, 1000)

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

      //物料与用户车系点击序列
      val match_series_pv_maxmin_7d = double2String6(matchSeries7d.pvMaxMin)
      val match_series_dur_maxmin_7d = double2String6(matchSeries7d.durMaxMin)
      val match_series_pv_weight_7d = double2String6(matchSeries7d.pvRate)
      val match_series_dur_weight_7d = double2String6(matchSeries7d.durRate)
      val match_series_pv_7d = double2String6(matchSeries7d.pv)
      val match_series_dur_7d = double2String6(matchSeries7d.dur)
      val match_series_pv_maxmin_15d = double2String6(matchSeries15d.pvMaxMin)
      val match_series_dur_maxmin_15d = double2String6(matchSeries15d.durMaxMin)
      val match_series_pv_weight_15d = double2String6(matchSeries15d.pvRate)
      val match_series_dur_weight_15d = double2String6(matchSeries15d.durRate)
      val match_series_pv_15d = double2String6(matchSeries15d.pv)
      val match_series_dur_15d = double2String6(matchSeries15d.dur)
      val match_series_pv_maxmin_30d = double2String6(matchSeries30d.pvMaxMin)
      val match_series_dur_maxmin_30d = double2String6(matchSeries30d.durMaxMin)
      val match_series_pv_weight_30d = double2String6(matchSeries30d.pvRate)
      val match_series_dur_weight_30d = double2String6(matchSeries30d.durRate)
      val match_series_pv_30d = double2String6(matchSeries30d.pv)
      val match_series_dur_30d = double2String6(matchSeries30d.dur)

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

      //pv-uv
      x.item_callback_ratio_1d = item_callback_ratio_1d
      x.item_callback_ratio_3d = item_callback_ratio_3d
      x.item_callback_ratio_7d = item_callback_ratio_7d
      x.item_callback_ratio_15d = item_callback_ratio_15d
      x.item_callback_ratio_30d = item_callback_ratio_30d

      //click_pv
      x.item_click_pv_wilson_1_3d = item_click_pv_wilson_1_3d
      x.item_click_pv_wilson_1_7d = item_click_pv_wilson_1_7d
      x.item_click_pv_wilson_1_15d = item_click_pv_wilson_1_15d
      x.item_click_pv_wilson_1_30d = item_click_pv_wilson_1_30d
      x.item_click_pv_wilson_3_7d = item_click_pv_wilson_3_7d
      x.item_click_pv_wilson_3_15d = item_click_pv_wilson_3_15d
      x.item_click_pv_wilson_3_30d = item_click_pv_wilson_3_30d

      //sight_pv
      x.item_sight_pv_wilson_1_3d = item_sight_pv_wilson_1_3d
      x.item_sight_pv_wilson_1_7d = item_sight_pv_wilson_1_7d
      x.item_sight_pv_wilson_1_15d = item_sight_pv_wilson_1_15d
      x.item_sight_pv_wilson_1_30d = item_sight_pv_wilson_1_30d
      x.item_sight_pv_wilson_3_7d = item_sight_pv_wilson_3_7d
      x.item_sight_pv_wilson_3_15d = item_sight_pv_wilson_3_15d
      x.item_sight_pv_wilson_3_30d = item_sight_pv_wilson_3_30d


      //collect_uv结果
      x.item_collect_uv_1d = item_collect_uv_1d
      x.item_collect_uv_3d = item_collect_uv_3d
      x.item_collect_uv_7d = item_collect_uv_7d
      x.item_collect_uv_15d = item_collect_uv_15d
      x.item_collect_uv_30d = item_collect_uv_30d

      //frontpage agee ctr
      x.item_frontpage_agee_pv_ctr_1d = item_frontpage_agee_pv_ctr_1d
      x.item_frontpage_agee_pv_ctr_3d = item_frontpage_agee_pv_ctr_3d
      x.item_frontpage_agee_pv_ctr_7d = item_frontpage_agee_pv_ctr_7d
      x.item_frontpage_agee_pv_ctr_15d = item_frontpage_agee_pv_ctr_15d
      x.item_frontpage_agee_pv_ctr_30d = item_frontpage_agee_pv_ctr_30d

      //deal_click_dur_log
      x.user_click_num_15d = user_click_num_15d
      x.user_click_num_30d = user_click_num_30d
      x.user_click_num_60d = user_click_num_60d
      x.user_click_num_7d = user_click_num_7d
      x.user_click_num_90d = user_click_num_90d
      x.user_duration_15d = user_duration_15d
      x.user_duration_30d = user_duration_30d
      x.user_duration_60d = user_duration_60d
      x.user_duration_7d = user_duration_7d
      x.user_duration_90d = user_duration_90d

      //车系交叉特征
      x.item_series_list_7d = item_series_list_7d
      x.item_series_list_15d = item_series_list_15d
      x.item_series_list_30d = item_series_list_30d

      //车系交叉匹配特征结果字段
      x.match_series_pv_maxmin_7d = match_series_pv_maxmin_7d
      x.match_series_dur_maxmin_7d = match_series_dur_maxmin_7d
      x.match_series_pv_weight_7d = match_series_pv_weight_7d
      x.match_series_dur_weight_7d = match_series_dur_weight_7d
      x.match_series_pv_7d = match_series_pv_7d
      x.match_series_dur_7d = match_series_dur_7d
      x.match_series_pv_maxmin_15d = match_series_pv_maxmin_15d
      x.match_series_dur_maxmin_15d = match_series_dur_maxmin_15d
      x.match_series_pv_weight_15d = match_series_pv_weight_15d
      x.match_series_dur_weight_15d = match_series_dur_weight_15d
      x.match_series_pv_15d = match_series_pv_15d
      x.match_series_dur_15d = match_series_dur_15d
      x.match_series_pv_maxmin_30d = match_series_pv_maxmin_30d
      x.match_series_dur_maxmin_30d = match_series_dur_maxmin_30d
      x.match_series_pv_weight_30d = match_series_pv_weight_30d
      x.match_series_dur_weight_30d = match_series_dur_weight_30d
      x.match_series_pv_30d = match_series_pv_30d
      x.match_series_dur_30d = match_series_dur_30d


      //新增类别特征
      x.user_uniq_keywords_pref = user_uniq_keywords_pref
      x.user_uniq_series_pref = user_uniq_series_pref
      x.user_fp_click_series_seq = user_fp_click_series_seq
      x.item_uniq_series_ids = item_uniq_series_ids
      x.item_uniq_keywords_name = item_uniq_keywords_name
      x.user_rt_fp_click_series_seq = user_rt_fp_click_series_seq
      x.user_uniq_category_pref = user_uniq_category_pref
      x.item_uniq_category_name = item_uniq_category_name
      x.user_rt_category_list = user_rt_category_list
      x.item_author_id = item_author_id
      x.user_rt_click_author_list_pre = user_rt_click_author_list_pre
      x.user_rt_click_tag_pref = user_rt_click_tag_pref
      x.user_device_model = user_device_model
      x.user_energy_pref_top1 = user_energy_pref_top1
      x.recall_way = recall_way
      x.gc_type = gc_type
      x.rt_item_lst_list = rt_item_lst_list
      x.item_lst_list = item_lst_list
      x.item_key = item_key

      x
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

    val featuresListOutputReal = VideoFeatureColumnV2Output.featuresListOutputReal

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
