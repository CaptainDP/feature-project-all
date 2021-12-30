package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sun.misc.BASE64Decoder


object RankScoreLabel {

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

    val targetHdfsPath = jsonObj.getString("targetHdfsPath")
    if (targetHdfsPath == null || targetHdfsPath.toString.trim.equals("")) {
      println("targetHdfsPath is null")
      System.exit(-1)
    }

    val currDate = dt
    val date = DateUtil.toDate(currDate, "yyyy-MM-dd")
    val date1 = DateUtil.getDate(date, "yyyy-MM-dd")
    val date2 = DateUtil.getDate(date, "yyyyMMdd")

    val targetHdfsPathNew = targetHdfsPath + "/" + date

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("create temporary function RankScoreUDF as 'com.captain.bigdata.taichi.demo.udf.RankScoreUDF'")

    val sql1 = s"select dtNew,device_id,pvid,rtype,biz_id,score,model_name,push_time from dc_pds.dc_rcm_feature_deep_info_tb LATERAL VIEW RankScoreUDF(dt,value) as dtNew,device_id,pvid,rtype,biz_id,score,model_name,push_time where dt = '$date2'"
    println("sql1:" + sql1)
    var dataFrame = spark.sql(sql1)
    dataFrame.createOrReplaceTempView("TMP_TBL_01")

    val sql2 = s"select dt,device_id,pvid,rtype,biz_id,label,dur_label,duration,posid,biz_type from cmp_gdm.cmp_gdm_rcmd_train_rank_sample_new_di a where dt = '$date1' and biz_type in ('14','3','66') "
    println("sql2:" + sql2)
    dataFrame = spark.sql(sql2)
    dataFrame.createOrReplaceTempView("TMP_TBL_02")

    val sql3 = s"select b.dt,b.device_id,b.pvid,b.rtype,b.biz_id,b.biz_type,b.label,b.dur_label,b.posid,a.score,a.model_name,a.push_time from TMP_TBL_01 a join TMP_TBL_02 b on a.dtNew=b.dt and upper(a.device_id)=upper(b.device_id) and a.pvid=b.pvid and a.rtype=b.rtype and a.biz_id=b.biz_id"
    println("sql3:" + sql3)
    dataFrame = spark.sql(sql3)
    dataFrame.createOrReplaceTempView("TMP_TBL_03")

    val sql4 = "select dt,device_id,pvid,rtype,biz_id,biz_type,label,dur_label,posid,score,score as prob,model_name,push_time from TMP_TBL_03"
    println("sql4:" + sql4)
    dataFrame = spark.sql(sql4)

    println("targetHdfsPath:" + targetHdfsPathNew)
    dataFrame.write.mode("overwrite").json(targetHdfsPathNew)

    spark.stop()
  }

}
