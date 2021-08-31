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
      x.item_callback_ratio_1d = callback_ratio(x.pv_1d, x.uv_1d)
      x.item_callback_ratio_3d = callback_ratio(x.pv_3d, x.uv_3d)
      x.item_callback_ratio_7d = callback_ratio(x.pv_7d, x.uv_7d)
      x.item_callback_ratio_15d = callback_ratio(x.pv_15d, x.uv_15d)
      x.item_callback_ratio_30d = callback_ratio(x.pv_30d, x.uv_30d)

      //click_pv ratio
      x.item_click_pv_wilson_1_3d = wilson_score(x.click_pv_1d, x.click_pv_3d)
      x.item_click_pv_wilson_1_7d = wilson_score(x.click_pv_1d, x.click_pv_7d)
      x.item_click_pv_wilson_1_15d = wilson_score(x.click_pv_1d, x.click_pv_15d)
      x.item_click_pv_wilson_1_30d = wilson_score(x.click_pv_1d, x.click_pv_30d)
      x.item_click_pv_wilson_3_7d = wilson_score(x.click_pv_3d, x.click_pv_7d)
      x.item_click_pv_wilson_3_15d = wilson_score(x.click_pv_3d, x.click_pv_15d)
      x.item_click_pv_wilson_3_30d = wilson_score(x.click_pv_3d, x.click_pv_30d)

      //collect ratio
      x.item_collect_uv_1d = wilson_score(x.collect_cnt_1d, x.click_uv_1d)
      x.item_collect_uv_3d = wilson_score(x.collect_cnt_3d, x.click_uv_3d)
      x.item_collect_uv_7d = wilson_score(x.collect_cnt_7d, x.click_uv_7d)
      x.item_collect_uv_15d = wilson_score(x.collect_cnt_15d, x.click_uv_15d)
      x.item_collect_uv_30d = wilson_score(x.collect_cnt_30d, x.click_uv_30d)

      //sight_pv ratio
      x.item_sight_pv_wilson_1_3d = wilson_score(x.sight_pv_1d, x.sight_pv_3d)
      x.item_sight_pv_wilson_1_7d = wilson_score(x.sight_pv_1d, x.sight_pv_7d)
      x.item_sight_pv_wilson_1_15d = wilson_score(x.sight_pv_1d, x.sight_pv_15d)
      x.item_sight_pv_wilson_1_30d = wilson_score(x.sight_pv_1d, x.sight_pv_30d)
      x.item_sight_pv_wilson_3_7d = wilson_score(x.sight_pv_3d, x.sight_pv_7d)
      x.item_sight_pv_wilson_3_15d = wilson_score(x.sight_pv_3d, x.sight_pv_15d)
      x.item_sight_pv_wilson_3_30d = wilson_score(x.sight_pv_3d, x.sight_pv_30d)

      //frontpage agee ctr
      x.item_frontpage_agee_pv_ctr_1d = agee_score(x.click_pv_1d, 5, x.sight_pv_1d, 50)
      x.item_frontpage_agee_pv_ctr_3d = agee_score(x.click_pv_1d, 8, x.sight_pv_1d, 80)
      x.item_frontpage_agee_pv_ctr_7d = agee_score(x.click_pv_1d, 11, x.sight_pv_1d, 115)
      x.item_frontpage_agee_pv_ctr_15d = agee_score(x.click_pv_1d, 21, x.sight_pv_1d, 220)
      x.item_frontpage_agee_pv_ctr_30d = agee_score(x.click_pv_1d, 85, x.sight_pv_1d, 1000)

      //frontpage_click_pv
      x.item_frontpage_click_pv_1d = log(x.click_pv_1d, 1, 5)
      x.item_frontpage_click_pv_3d = log(x.click_pv_3d, 1, 5)
      x.item_frontpage_click_pv_7d = log(x.click_pv_7d, 1, 5)
      x.item_frontpage_click_pv_15d = log(x.click_pv_15d, 1, 5)
      x.item_frontpage_click_pv_30d = log(x.click_pv_30d, 1, 5)

      //frontpage_sight_pv
      x.item_frontpage_sight_pv_1d = log(x.sight_pv_1d, 1, 5)
      x.item_frontpage_sight_pv_3d = log(x.sight_pv_3d, 1, 5)
      x.item_frontpage_sight_pv_7d = log(x.sight_pv_7d, 1, 5)
      x.item_frontpage_sight_pv_15d = log(x.sight_pv_15d, 1, 5)
      x.item_frontpage_sight_pv_30d = log(x.sight_pv_30d, 1, 5)

      //prop_author
      x.item_prop_author_fellowsnum = log10(x.fans_num, 1, 5)
      x.item_prop_author_articlenum = log10(x.works_num, 1, 5)
      x.item_prop_author_comment_num = log10(x.comment_cnt_90d, 1, 5)
      x.item_prop_author_favorites_num = log10(x.favorites_cnt_90d, 1, 5)
      x.item_prop_author_share_cnt = log10(x.share_cnt_90d, 1, 5)
      x.item_prop_author_like_num = log10(x.like_cnt_90d, 1, 5)

      //time
      x.item_prop_publishtime = getTimeDecaySecond(x.start_time, x.recommend_time)
      x.item_prop_time_decay = getTimeDecayExp(x.start_time, x.recommend_time)

      //quality feature
      x.item_quality_cover_score = x.cover_score
      x.item_quality_effect_score = x.effect_score
      x.item_quality_rare_score = x.rare_score
      x.item_quality_time_score = x.time_score

      //reply ratio
      x.item_reply_uv_1d = wilson_score(x.reply_cnt_1d, x.click_uv_1d, 1)
      x.item_reply_uv_3d = wilson_score(x.reply_cnt_3d, x.click_uv_3d, 1)
      x.item_reply_uv_7d = wilson_score(x.reply_cnt_7d, x.click_uv_7d, 1)
      x.item_reply_uv_15d = wilson_score(x.reply_cnt_15d, x.click_uv_15d, 1)
      x.item_reply_uv_30d = wilson_score(x.reply_cnt_30d, x.click_uv_30d, 1)

      //frontpage ctr
      x.item_frontpage_wilson_pv_ctr_1d = wilson_score(x.click_pv_1d, x.sight_pv_1d)
      x.item_frontpage_wilson_pv_ctr_3d = wilson_score(x.click_pv_3d, x.sight_pv_3d)
      x.item_frontpage_wilson_pv_ctr_7d = wilson_score(x.click_pv_7d, x.sight_pv_7d)
      x.item_frontpage_wilson_pv_ctr_15d = wilson_score(x.click_pv_15d, x.sight_pv_15d)
      x.item_frontpage_wilson_pv_ctr_30d = wilson_score(x.click_pv_30d, x.sight_pv_30d)

      //like ratio
      x.item_like_uv_1d = wilson_score(x.like_cnt_1d, x.click_uv_1d)
      x.item_like_uv_3d = wilson_score(x.like_cnt_3d, x.click_uv_3d)
      x.item_like_uv_7d = wilson_score(x.like_cnt_7d, x.click_uv_7d)
      x.item_like_uv_15d = wilson_score(x.like_cnt_15d, x.click_uv_15d)
      x.item_like_uv_30d = wilson_score(x.like_cnt_30d, x.click_uv_30d)


      //deal_click_dur_log
      x.user_click_num_15d = log(x.user_click_num_15d, 1, 6)
      x.user_click_num_30d = log(x.user_click_num_30d, 1, 6)
      x.user_click_num_60d = log(x.user_click_num_60d, 1, 6)
      x.user_click_num_7d = log(x.user_click_num_7d, 1, 6)
      x.user_click_num_90d = log(x.user_click_num_90d, 1, 6)
      x.user_duration_15d = log(x.user_duration_15d, 1, 6)
      x.user_duration_30d = log(x.user_duration_30d, 1, 6)
      x.user_duration_60d = log(x.user_duration_60d, 1, 6)
      x.user_duration_7d = log(x.user_duration_7d, 1, 6)
      x.user_duration_90d = log(x.user_duration_90d, 1, 6)

      x.item_uniq_series_ids = replaceComma2SemicolonDistinct(x.item_uniq_series_ids)

      //车系交叉特征
      x.item_series_list_7d = getKeysFromSemicolonList(x.item_series_list_7d)
      x.item_series_list_15d = getKeysFromSemicolonList(x.item_series_list_15d)
      x.item_series_list_30d = getKeysFromSemicolonList(x.item_series_list_30d)

      val matchSeries7d = get_match_series(x.item_uniq_series_ids, x.item_series_list_7d)
      val matchSeries15d = get_match_series(x.item_uniq_series_ids, x.item_series_list_15d)
      val matchSeries30d = get_match_series(x.item_uniq_series_ids, x.item_series_list_30d)

      //物料与用户车系点击序列
      x.match_series_pv_maxmin_7d = double2String(matchSeries7d.pvMaxMin, 6)
      x.match_series_dur_maxmin_7d = double2String(matchSeries7d.durMaxMin, 6)
      x.match_series_pv_weight_7d = double2String(matchSeries7d.pvRate, 6)
      x.match_series_dur_weight_7d = double2String(matchSeries7d.durRate, 6)
      x.match_series_pv_7d = double2String(matchSeries7d.pv, 6)
      x.match_series_dur_7d = double2String(matchSeries7d.dur, 6)
      x.match_series_pv_maxmin_15d = double2String(matchSeries15d.pvMaxMin, 6)
      x.match_series_dur_maxmin_15d = double2String(matchSeries15d.durMaxMin, 6)
      x.match_series_pv_weight_15d = double2String(matchSeries15d.pvRate, 6)
      x.match_series_dur_weight_15d = double2String(matchSeries15d.durRate, 6)
      x.match_series_pv_15d = double2String(matchSeries15d.pv, 6)
      x.match_series_dur_15d = double2String(matchSeries15d.dur, 6)
      x.match_series_pv_maxmin_30d = double2String(matchSeries30d.pvMaxMin, 6)
      x.match_series_dur_maxmin_30d = double2String(matchSeries30d.durMaxMin, 6)
      x.match_series_pv_weight_30d = double2String(matchSeries30d.pvRate, 6)
      x.match_series_dur_weight_30d = double2String(matchSeries30d.durRate, 6)
      x.match_series_pv_30d = double2String(matchSeries30d.pv, 6)
      x.match_series_dur_30d = double2String(matchSeries30d.dur, 6)

      //新增类别特征
      x.user_uniq_keywords_pref = getKeysFromSemicolonList(x.user_uniq_keywords_pref)
      x.user_uniq_series_pref = getKeysFromSemicolonList(x.user_uniq_series_pref)
      x.user_fp_click_series_seq = replaceComma2SemicolonDistinct(x.user_fp_click_series_seq)
      x.item_uniq_keywords_name = replaceComma2SemicolonDistinct(x.item_uniq_keywords_name)
      x.user_rt_fp_click_series_seq = replaceComma2SemicolonDistinct(x.user_rt_fp_click_series_seq)
      x.user_uniq_category_pref = getKeysFromSemicolonList(x.user_uniq_category_pref)
      x.item_uniq_category_name = replaceComma2SemicolonDistinct(x.item_uniq_category_name)
      x.user_rt_category_list = replaceComma2SemicolonDistinct(x.user_rt_category_list)
      x.item_author_id = x.item_author_id
      x.user_rt_click_author_list_pre = replaceComma2SemicolonDistinct(x.user_rt_click_author_list_pre)
      x.user_rt_click_tag_pref = replaceComma2SemicolonDistinct(x.user_rt_click_tag_pref)
      x.user_device_model = replaceComma2SemicolonDistinct(x.user_device_model)
      x.user_energy_pref_top1 = getKeysFromSemicolonList(x.user_energy_pref_top1)
      x.recall_way = x.recall_way
      x.gc_type = x.gc_type
      x.rt_item_lst_list = replaceComma2SemicolonDistinct(x.rt_item_lst_list)
      x.item_lst_list = replaceComma2SemicolonDistinct(x.item_lst_list)
      x.item_key = replaceComma2SemicolonDistinct(x.item_key)

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

    val featuresListOutputReal = VideoFeatureColumnV2Output.featuresListOutputReal

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val resultDF = featureResultRdd.toDF()
    resultDF.createOrReplaceTempView("TMP_TBL_01")

    val columnList = featuresListOutputReal.mkString(",")
    val sqlStr = "insert overwrite table dm_rca." + tableName + " PARTITION(dt='" + dt + "')  select " + columnList + " from TMP_TBL_01"
    println(sqlStr)
    spark.sql(sqlStr)

    spark.stop()
  }

}
