package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object FeatureSampleMergeGeneral {

  def main(args: Array[String]): Unit = {

    println("args:" + args.mkString(","))

    val options = new Options
    options.addOption("d", true, "date yyyy-MM-dd [default yesterday]")
    options.addOption("j", true, "json:preDateNum,sourceTableName,targetHdfsPath,columnList")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    //date
    var dt = DateUtil.getDate(new Date(), "yyyy-MM-dd")
    if (cmd.hasOption("d")) {
      dt = cmd.getOptionValue("d")
    }

    var jsonStr = "{}"
    if (cmd.hasOption("j")) {
      jsonStr = cmd.getOptionValue("j")
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


    val sql = s"select $columnList from $sourceTableName where dt >= '$startDate' and dt <= '$endDate' and biz_type in ('14','3','66') and flag_data = 'shouye'"
    println("sql:" + sql)
    var dataFrame = spark.sql(sql)
    dataFrame.createOrReplaceTempView("TMP_TBL_01")

    //过滤低质用户无效的曝光样本（当日点击次数为0的）
    //    val filterSql = "select a.* from TMP_TBL_01 a join cmp_tmp.cmp_tmp_user_ctr b on a.dt = b.dt and upper(a.device_id) = upper(b.device_id) and b.click_num >0"
    //    println("filterSql:" + filterSql)
    //    dataFrame = spark.sql(filterSql)

    val featuresListOutputReal = columnList.split(",")
    dataFrame = dataFrame.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*)
    dataFrame = dataFrame.repartition(200)
    dataFrame.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(targetHdfsPath)

    spark.stop()
  }

}
