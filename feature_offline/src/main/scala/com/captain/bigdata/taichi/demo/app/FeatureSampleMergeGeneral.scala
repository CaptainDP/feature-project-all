package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
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
    if (targetHdfsPath == null || targetHdfsPath.toString.trim.equals("")) {
      println("targetHdfsPath is null")
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

    var filterLastNegativeSample = jsonObj.getBoolean("filterLastNegativeSample")
    if (filterLastNegativeSample == null) {
      filterLastNegativeSample = false
    }

    var addPosSample = jsonObj.getBoolean("addPosSample")
    if (addPosSample == null) {
      addPosSample = false
    }

    var isHour = jsonObj.getBoolean("isHour")
    if (isHour == null) {
      isHour = false
    }

    var filterCondition = jsonObj.getString("filterCondition")
    if (filterCondition == null) {
      filterCondition = ""
    }

    val filterSql = jsonObj.getString("filterSql")
    val castTypeList = jsonObj.getString("castTypeList")


    val currDate = dt
    val date = DateUtil.toDate(currDate, "yyyy-MM-dd")
    var endDate = DateUtil.getDate(date, "yyyy-MM-dd")
    var startDate = DateUtil.calcDateByFormat(date, "yyyy-MM-dd(-" + preCount + "D)")
    var hour = "00"

    if (isHour) {
      val preDate = getPreDateHour(diffHour = 4)
      startDate = DateUtil.calcDateByFormat(preDate, "yyyy-MM-dd")
      endDate = startDate
      hour = DateUtil.calcDateByFormat(preDate, "HH")
    }

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
    //???????????????????????????
    val filterColumnStr = "item_author_id,item_r_type,gc_type,recall_way,user_device_brand,user_device_model,item_uniq_series_ids,item_uniq_brands_ids,item_uniq_category_name,item_uniq_keywords_name,user_fp_click_series_seq,user_rt_fp_click_series_seq,rt_item_lst_list,item_lst_list,user_rt_click_brand_pref,user_rt_click_author_list_pre,user_rt_click_tag_pref,user_rtype_pref,user_uniq_keywords_pref,user_uniq_category_pref,user_uniq_series_pref,user_uniq_brand_pref,user_rt_category_list,user_fp_click_rtype_list,user_series_search_cnt_7d"
    val filterColumnSet = filterColumnStr.replaceAll(" ", "").split(",").toSet

    val columnListNew = ArrayBuffer[String]()
    columnList.split(",").foreach(x => {
      if (castTypeSet(x)) {
        val xx = "cast(if(" + x + " = '','-1'," + x + ") as double) as " + x
        columnListNew.append(xx)
      } else {
        if (filterColumnSet(x)) {
          val xx = "case when a.dt <= '2021-09-12' then '' else " + x + " end as " + x
          columnListNew.append(xx)
        } else {
          columnListNew.append("a." + x)
          //????????????dur_label
          //          if ("dur_label".equals(x)) {
          //            columnListNew.append("case when a.biz_type=3 and a.duration>=10 then 1 when a.biz_type=14 and a.duration>=11 then 1 when a.biz_type=66 and a.duration>=9 then 1 else 0 end as dur_label")
          //          } else {
          //            columnListNew.append("a." + x)
          //          }
        }
      }
    })

    val columnStr = columnListNew.mkString(",")
    println("columnStr:" + columnStr)

    var sql = s"select $columnStr from $sourceTableName a where dt >= '$startDate' and dt <= '$endDate' and biz_type in ('14','3','66') $filterCondition"
    if (isHour) {
      //TODO:??????????????????
      //sql = sql + " and hour = " + hour
    }
    println("sql:" + sql)
    var dataFrame = spark.sql(sql)
    dataFrame.createOrReplaceTempView("TMP_TBL_01")

    println("filterSql:" + filterSql)
    if (filterSql != null && !filterSql.trim.equals("")) {
      dataFrame = spark.sql(filterSql)
    }

    //???????????????????????????????????????????????????????????????0??????
    if (filterLowUser) {
      val filterSql = "select a.* from TMP_TBL_01 a join cmp_tmp.cmp_tmp_user_ctr b on a.dt = b.dt and upper(a.device_id) = upper(b.device_id) and b.click_num > 0 and b.ctr is not null "
      //      val filterSql = "select a.* from TMP_TBL_01 a join cmp_tmp.cmp_tmp_user_ctr b on a.dt = b.dt and upper(a.device_id) = upper(b.device_id) and b.click_num > 0 and b.click_num < b.sight_show_num"
      println("filterSql:" + filterSql)
      dataFrame = spark.sql(filterSql)
      dataFrame.createOrReplaceTempView("TMP_TBL_01")
    }

    //?????????????????????????????????
    if (filterLastNegativeSample) {
      val filterLastSql = s"select * from (select dt,device_id,pvid,biz_id,biz_type,posid,label,ROW_NUMBER() over(partition by dt,device_id,pvid order by posid desc) as rn from $sourceTableName a where a.dt >= '$startDate' and a.dt <= '$endDate' ) tmp where label=0 and rn = 1 and biz_type in (3,14,66)"
      println("filterLastSql:" + filterLastSql)
      dataFrame = spark.sql(filterLastSql)
      dataFrame.createOrReplaceTempView("TMP_TBL_02")
      val filterLastSql2 = s"select a.* from TMP_TBL_01 a left join TMP_TBL_02 b on a.dt=b.dt and upper(a.device_id)=upper(b.device_id) and a.pvid=b.pvid and a.biz_id=b.biz_id and a.biz_type=b.biz_type and a.label=b.label where b.device_id is null"
      dataFrame = spark.sql(filterLastSql2)
      dataFrame.createOrReplaceTempView("TMP_TBL_01")
    }

    //???????????????????????????????????????????????????
    if (addPosSample) {
      val addSql = s"select $columnStr from $sourceTableName a join cmp_tmp.cmp_tmp_user_ctr b on upper(a.device_id) = upper(b.device_id) and a.dt = b.dt where a.dt >= '$startDate' and a.dt <= '$endDate' and a.label = 1 and b.click_num > 0 and b.ctr is not null and b.video_click_num = 0 and rand() < 0.1"
      println("addSql:" + addSql)
      val addDataFrame = spark.sql(addSql)
      dataFrame = spark.sql("select * from TMP_TBL_01")
      dataFrame = dataFrame.union(addDataFrame)
      dataFrame.createOrReplaceTempView("TMP_TBL_01")
    }

    dataFrame = spark.sql("select * from TMP_TBL_01")
    val featuresListOutputReal = columnList.split(",")
    dataFrame = dataFrame.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*)
    dataFrame = dataFrame.repartition(2000)
    dataFrame.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(targetHdfsPath)
    println("targetHdfsPath:" + targetHdfsPath)

    spark.stop()
  }

  def getPreDateHour(date: Date = new Date(), diffHour: Int = 0): Date = {
    val now = new DateTime(date)
    val dateTime = now.minusHours(diffHour).toDate
    dateTime
  }

}
