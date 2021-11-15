package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sun.misc.BASE64Decoder

import scala.collection.mutable.ArrayBuffer


object FeatureSampleMergeGeneral {

  def main(args: Array[String]): Unit = {

    println("args:" + args.mkString(","))

    val options = new Options
    options.addOption("d", true, "date yyyy-MM-dd [default yesterday]")
    options.addOption("b", true, "base64")
    options.addOption("j", true, "json:preDateNum,sourceTableName,targetHdfsPath,columnList")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    //date
    var dt = DateUtil.getDate(new Date(), "yyyy-MM-dd")
    if (cmd.hasOption("d")) {
      dt = cmd.getOptionValue("d")
    }

    val decoder = new BASE64Decoder

    var jsonStr = "{}"
    if (cmd.hasOption("j")) {
      jsonStr = cmd.getOptionValue("j")
    }

    val base64 = cmd.getOptionValue("b")
    if (base64 != null && base64.equals("true")) {
      println("jsonStr:" + jsonStr)
      jsonStr = new String(decoder.decodeBuffer(jsonStr))
      println("jsonStr base64:" + jsonStr)
    }


    val jsonObj = JSON.parseObject(jsonStr)
    val preCount = jsonObj.getInteger("preDateNum")
    val sourceTableName = jsonObj.getString("sourceTableName")
    if (sourceTableName == null || sourceTableName.toString.trim.equals("")) {
      println("sourceTableName is null")
      System.exit(-1)
    }
    val targetHdfsPath = jsonObj.getString("targetHdfsPath")
    if (sourceTableName == null || sourceTableName.toString.trim.equals("")) {
      println("sourceTableName is null")
      System.exit(-1)
    }
    val columnList = jsonObj.getString("columnList").replaceAll(" ", "")
    if (columnList == null || columnList.toString.trim.equals("")) {
      println("columnList is null")
      System.exit(-1)
    }

    var filterLowUser = jsonObj.getBoolean("filterLowUser")
    if (filterLowUser == null) {
      filterLowUser = false
    }

    var addPosSample = jsonObj.getBoolean("addPosSample")
    if (addPosSample == null) {
      addPosSample = false
    }

    val filterCondition = jsonObj.getString("filterCondition")
    val filterSql = jsonObj.getString("filterSql")
    val castTypeList = jsonObj.getString("castTypeList")


    val currDate = dt
    val date = DateUtil.toDate(currDate, "yyyy-MM-dd")
    val endDate = DateUtil.getDate(date, "yyyy-MM-dd")
    val startDate = DateUtil.calcDateByFormat(date, "yyyy-MM-dd(-" + preCount + "D)")

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    //    val castTypeList = "item_videosound,item_video_subtitle,item_video_stability,item_video_coverimage_blackborder,item_video_blackborder,item_cms_quality,user_video_score"

    val castTypeSet = castTypeList.replaceAll(" ", "").split(",").toSet
    //类别特征异常值处理
    val filterColumnStr = "item_author_id,item_r_type,gc_type,recall_way,user_device_brand,user_device_model,item_uniq_series_ids,item_uniq_brands_ids,item_uniq_category_name,item_uniq_keywords_name,user_fp_click_series_seq,user_rt_fp_click_series_seq,rt_item_lst_list,item_lst_list,user_rt_click_brand_pref,user_rt_click_author_list_pre,user_rt_click_tag_pref,user_rtype_pref,user_uniq_keywords_pref,user_uniq_category_pref,user_uniq_series_pref,user_uniq_brand_pref,user_rt_category_list,user_fp_click_rtype_list,user_series_search_cnt_7d"
    val filterColumnSet = filterColumnStr.replaceAll(" ", "").split(",").toSet

    val columnListNew = ArrayBuffer[String]()
    columnList.split(",").foreach(x => {
      if (castTypeSet(x)) {
        val xx = "cast(if(" + x + " = '','-1'," + x + ") as double) as " + x
        columnListNew.append(xx)
      } else {
        if (filterColumnSet(x)) {
          val xx = "case when dt <= '2021-09-12' then '' else " + x + " end as " + x
          columnListNew.append(xx)
        } else {
          columnListNew.append(x)
        }
      }
    })

    val columnStr = columnListNew.mkString(",")
    println("columnStr:" + columnStr)

    val sql = s"select $columnStr from $sourceTableName where dt >= '$startDate' and dt <= '$endDate' and biz_type in ('14','3','66') $filterCondition"
    println("sql:" + sql)
    var dataFrame = spark.sql(sql)
    dataFrame.createOrReplaceTempView("TMP_TBL_01")

    println("filterSql:" + filterSql)
    if (filterSql != null && !filterSql.trim.equals("")) {
      dataFrame = spark.sql(filterSql)
    }

    //过滤低质用户无效的曝光样本（当日点击次数为0的）
    if (filterLowUser) {
      val filterSql = "select a.* from TMP_TBL_01 a join cmp_tmp.cmp_tmp_user_ctr b on a.dt = b.dt and upper(a.device_id) = upper(b.device_id) and b.click_num > 0 and b.ctr is not null "
      //      val filterSql = "select a.* from TMP_TBL_01 a join cmp_tmp.cmp_tmp_user_ctr b on a.dt = b.dt and upper(a.device_id) = upper(b.device_id) and b.click_num > 0 and b.click_num < b.sight_show_num"
      println("filterSql:" + filterSql)
      dataFrame = spark.sql(filterSql)
    }

    //增加正样本：点击图文，但未点击视频
    if (addPosSample) {
      val addSql = s"select $columnStr from $sourceTableName a join cmp_tmp.cmp_tmp_user_ctr b on upper(a.device_id) = upper(b.device_id) and a.dt = b.dt where dt >= '$startDate' and dt <= '$endDate' and a.label = 1 and b.click_num > 0 and b.ctr is not null and b.video_click_num = 0 and rand() < 0.1"
      println("addSql:" + addSql)
      val addDataFrame = spark.sql(addSql)
      dataFrame = dataFrame.union(addDataFrame)
    }

    val featuresListOutputReal = columnList.split(",")
    dataFrame = dataFrame.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*)
    dataFrame = dataFrame.repartition(2000)
    dataFrame.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(targetHdfsPath)
    println("targetHdfsPath:" + targetHdfsPath)

    spark.stop()
  }

}
