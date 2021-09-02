package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
 * 计算每个特征的分位数
 */
object FeatureBucketRangeApp {

  def firstLineList(fielName: String): Array[String] = {
    val src = Source.fromFile(fielName)
    var list = new Array[String](0)
    try {
      list = src.getLines().next().toString.split(",")
    } finally {
      src.close()
    }
    list
  }


  def getFeatureBucket(fileName: String): Map[String, Array[Double]] = {
    val src = Source.fromFile(fileName)
    val featureBucketMap = new mutable.HashMap[String, Array[Double]]()
    try {
      val fileContents = src.getLines().mkString
      val json = JSON.parseObject(fileContents)

      for (entry <- json.entrySet()) {
        val key = entry.getKey
        val values = json.getJSONArray(key)
        val newValues = new ArrayBuffer[Double]()
        newValues.append(Double.NegativeInfinity)
        for (value <- values) {
          newValues.append(value.toString.toDouble)
        }
        newValues.append(Double.PositiveInfinity)
        featureBucketMap.put(key, newValues.toArray)
      }

    } finally {
      src.close()
    }
    //手工加入label标签的分桶
    featureBucketMap("label") = Array(Double.NegativeInfinity, 0.1, 1.1, Double.PositiveInfinity)
    featureBucketMap.toMap
  }

  def getFeatureBucketList(featureBucketMap: Map[String, Array[Double]], featuresList: Array[String]): Array[Array[Double]] = {

    val featureSplitList = new ArrayBuffer[Array[Double]]()

    for (featureName <- featuresList) {
      val featureList = featureBucketMap.getOrElse(featureName, null)
      if (featureList == null) {
        throw new RuntimeException(featureName + " featureList not exist")
      }
      featureSplitList.append(featureList)
    }

    featureSplitList.toArray
  }

  //agg的expr使用
  trait AggregationOp {
    def expr: Column
  }

  case class StringAggregationOp(c: String, func: String, alias: String
                                ) extends AggregationOp {
    def expr = org.apache.spark.sql.functions.expr(s"${func}(`${c}`)").alias(alias)
  }

  def main(args: Array[String]): Unit = {

    val startTime = DateUtil.getDateTime()
    val options = new Options
    options.addOption("d", true, "date yyyy-MM-dd [default yesterday]")
    options.addOption("p", true, "path")
    options.addOption("n", true, "path")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    //date
    var dt = DateUtil.getDate(new Date(), "yyyy-MM-dd")
    if (cmd.hasOption("d")) {
      dt = cmd.getOptionValue("d")
    }
    var path = ""
    if (cmd.hasOption("p")) {
      path = cmd.getOptionValue("p")
    }

    var num = 13
    if (cmd.hasOption("n")) {
      num = cmd.getOptionValue("n").toInt
    }

    //线上环境
    val tableName = "cmp_gdm.cmp_gdm_rcmd_train_rank_sample_new_di"
    val addColumnNames = "label,dt"
    val columnNames = "match_category_dur_maxmin_7d, match_category_dur_maxmin_15d, match_category_dur_maxmin_30d, match_category_dur_weight_7d, match_category_dur_weight_15d, match_category_dur_weight_30d, match_rtype_dur_maxmin_7d, match_rtype_dur_maxmin_15d, match_rtype_dur_maxmin_30d, match_rtype_dur_weight_7d, match_rtype_dur_weight_15d, match_rtype_dur_weight_30d, item_uv_duration_15d, item_uv_duration_30d"
    val allColumnNames = columnNames + "," + addColumnNames
    val numBuckets = 100

    //spark
    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .enableHiveSupport
      .config(sparkConf)
      .getOrCreate()


    val dataFormat = "yyyy-MM-dd(-" + num + "D)"
    val startDate = DateUtil.calcDateByFormat(DateUtil.toDate(dt, "yyyy-MM-dd"), dataFormat)
    val endDate = dt

    //本地计算demo使用
    //    import spark.implicits._
    //
    //    val path2 = "D:\\workspace\\autohome_workspace\\feature-project-all\\feature_offline\\src\\main\\scala\\com\\captain\\bigdata\\taichi\\demo\\app\\demo.json"
    //    val peopleDS = spark.read.json(path2)
    //    var ss = peopleDS.as[Person2]
    //    ss.createOrReplaceTempView("cmp_gdm_rcmd_train_rank_sample_new_di")

    val sql = s"select $allColumnNames from $tableName where dt >= '$startDate' and dt <= '$endDate' and biz_type in ('14','3','66') "

    println("sql:" + sql)
    var dataFrame0 = spark.sql(sql)

    //读取特征列转换后列配置
    val featuresList = columnNames.replaceAll(" ", "").split(",")

    //计算特征覆盖度
    val totalNum = dataFrame0.count()
    featuresList.foreach(x => {
      dataFrame0 = dataFrame0.withColumn(x + "_notnull", when(col(x).isNull, 0).when(col(x) === null, 0).when(col(x) === "", 0).when(upper(col(x)) === "NULL", 0).when(length(col(x)) === null, 0).when(col(x) === "0", 0).otherwise(1))
    })

    val exprs = featuresList.map(x => StringAggregationOp(x + "_notnull", "sum", x + "_sum")).map(_.expr)
    dataFrame0 = dataFrame0.agg(exprs.head, exprs.tail: _*)

    val coverageMap = new JSONObject()
    val dd = dataFrame0.collect()
    for (i <- 0 until featuresList.size) {
      val oneMap = new JSONObject()
      oneMap.put("notnull_count", dd(0)(i))
      oneMap.put("notnull_coverage", dd(0)(i).toString.toDouble / totalNum)
      coverageMap.put(featuresList(i), oneMap)
    }


    //
    var dataFrame = spark.sql(sql)

    //将空值转成默认值0.0
    val defaultValue = -0.00000001
    featuresList.foreach(x => {
      dataFrame = dataFrame.withColumn(x, when(col(x).isNull, defaultValue).when(col(x) === null, defaultValue).when(col(x) === "", defaultValue).when(upper(col(x)) === "NULL", defaultValue).when(length(col(x)) === null, defaultValue).otherwise(col(x)))
    })
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK)

    //计算分桶
    val allMap = new JSONObject()
    val statMap = new JSONObject()

    featuresList.foreach(x => {

      val oneMap = new JSONObject()
      //计算分桶区间
      val discretizer = new QuantileDiscretizer()
        .setHandleInvalid("skip")
        .setInputCol(x)
        .setOutputCol(x + "_out")
        .setNumBuckets(numBuckets)
      val result = discretizer.fit(dataFrame).getSplits.toBuffer
      result -= (Double.NegativeInfinity, Double.PositiveInfinity)
      oneMap.put("bucket", result.toArray)

      //计算统计指标：mean，stddev，min，max
      val analysisMap = new JSONObject()
      val describe = dataFrame.describe(x).collect()
      describe.foreach(x => {
        analysisMap.put(x.get(0) + "", x.get(1).toString.toDouble)
      })

      //计算分位数：25%，50%，75%
      val list = Array(0.25, 0.5, 0.75)
      val stat = dataFrame.stat.approxQuantile(x, list, 0.05)
      for (i <- list.indices) {
        analysisMap.put(list(i) + "", stat(i))
      }
      oneMap.put("analysis", analysisMap)
      oneMap.put("coverage", coverageMap.get(x))

      statMap.put(x, oneMap)

    })

    allMap.put("stat", statMap)

    // 统计label正负样本数量
    val summaryMap = new JSONObject()
    val label = dataFrame.groupBy("label").count()
    val labelMap = new JSONObject()
    label.collect().foreach(x => {
      labelMap.put(x.get(0) + "", x.get(1))
    })
    summaryMap.put("label", labelMap)
    summaryMap.put("total", totalNum)

    //统计每天数量量
    val dtColumn = dataFrame.groupBy("dt").count().sort(asc("dt"))
    val dtMap = new JSONObject()
    dtColumn.collect().foreach(x => {
      dtMap.put(x.get(0) + "", x.get(1))
    })
    summaryMap.put("dt", dtMap)

    //记录日期区间
    val endTime = DateUtil.getDateTime()
    val costTime = DateUtil.getTimeDiff(endTime, startTime)
    summaryMap.put("costTime(s)", costTime)
    summaryMap.put("startDate", startDate)
    summaryMap.put("endDate", endDate)

    allMap.put("summary", summaryMap)

    println(s"------------------bucketJson--------$startDate-------------------")
    println(allMap.toString())
    println(s"------------------bucketJson--------$endDate-------------------")
    println("costTime(s):" + costTime)

    spark.stop()
  }
}
