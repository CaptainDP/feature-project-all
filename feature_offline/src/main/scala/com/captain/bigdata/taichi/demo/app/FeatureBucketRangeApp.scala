package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.captain.bigdata.taichi.util.{DateUtil, UrlUtil}
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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

  def main(args: Array[String]): Unit = {

    val options = new Options
    options.addOption("d", true, "date yyyy-MM-dd [default yesterday]")
    options.addOption("p", true, "path")
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

    //本地测试
    //    val train_data_path = "D:\\workspace\\autohome_workspace\\feature-project-all\\feature_offline\\src\\main\\resources\\data\\demo.csv"
    //    val result_path = "D:\\workspace\\autohome_workspace\\feature-project-all\\feature_offline\\src\\main\\resources\\data_out"
    //    val demo_path = "D:\\workspace\\autohome_workspace\\feature-project-all\\feature_offline\\src\\main\\resources\\conf\\demo.csv"
    //    val feature_fm_bucket_path = "D:\\workspace\\autohome_workspace\\feature-project-all\\feature_offline\\src\\main\\resources\\conf\\feature_fm_bucket.json"
    //线上环境
    val tableName = "cmp_tmp_train_sample_all_shucang_multi_v6_sample"
    val result_path = "viewfs://AutoLfCluster/team/cmp/hive_db/tmp/" + tableName + "_cdp_bucket/dt=" + dt
    println("result_path:" + result_path)
    val basePath = path
    println("basePath:" + basePath)
    val demo_path = UrlUtil.get("./conf/conf/demo.csv", basePath).getPath
    val feature_fm_bucket_path = UrlUtil.get("./conf/conf/feature_fm_bucket.json", basePath).getPath
    val startTime = DateUtil.getDateTime()

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)

    val spark = SparkSession
      .builder
      .enableHiveSupport
      .config(sparkConf)
      .getOrCreate()


    //读取特征列转换后列配置
    val featuresList = firstLineList(demo_path)
    val featureBucketMap = getFeatureBucket(feature_fm_bucket_path)
    val featureBucketList = getFeatureBucketList(featureBucketMap, featuresList)

    //指定输出列名
    val addList = Array("biz_id", "biz_type", "device_id")
    val featuresListOutput = featuresList.map(x => x + "_out")
    val featuresListOutputReal = addList ++ featuresListOutput
    val featuresListOutputName = featuresListOutputReal.mkString(",")

    //读入数据-此处可以改成sql
    //    val dataFrame = spark.read.format("csv")
    //      .option("delimiter", ",")
    //      .option("header", "true")
    //      .option("quote", "'")
    //      .option("nullValue", "\\N")
    //      .option("inferSchema", "true")
    //      .load(train_data_path).toDF()

    val startDate = DateUtil.calcDateByFormat(DateUtil.toDate(dt, "yyyy-MM-dd"), "yyyy-MM-dd(-13D)")
    val endDate = dt
    val sql = s"select * from cmp_tmp.$tableName where dt >= '$startDate' and dt <= '$endDate' and label in ('0', '1') and length(device_id) > 0 and cast(biz_id as long) > 0 and cast(biz_type as int) > 0"

    println("sql:" + sql)
    var dataFrame = spark.sql(sql)

    //将空值转成默认值0.0
    val defaultValue = -0.00000001
    featuresList.foreach(x => {
      dataFrame = dataFrame.withColumn(x, when(col(x).isNull, defaultValue).when(col(x) === null, defaultValue).when(col(x) === "", defaultValue).when(upper(col(x)) === "NULL", defaultValue).otherwise(col(x)))
    })
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK)

    //计算分桶
    val bucketMap = new JSONObject()
    val statMap = new JSONObject()

    featuresList.foreach(x => {

      //计算分桶区间
      val discretizer = new QuantileDiscretizer()
        .setHandleInvalid("skip")
        .setInputCol(x)
        .setOutputCol(x + "_out")
        .setNumBuckets(10)
      val result = discretizer.fit(dataFrame).getSplits.toBuffer
      result -= (Double.NegativeInfinity, Double.PositiveInfinity)
      bucketMap.put(x, result.toArray)

      //计算统计指标：mean，stddev，min，max
      val tmpMap = new JSONObject()
      val describe = dataFrame.describe(x).collect()
      describe.foreach(x => {
        tmpMap.put(x.get(0) + "", x.get(1))
      })

      //计算分位数：25%，50%，75%
      val list = Array(0.25, 0.5, 0.75)
      val stat = dataFrame.stat.approxQuantile(x, list, 0.05)
      for (i <- list.indices) {
        tmpMap.put(list(i) + "", stat(i))
      }

      statMap.put(x + "_analysis", tmpMap)

    })

    // 统计label正负样本数量
    val summaryMap = new JSONObject()
    val label = dataFrame.groupBy("label").count()
    val labelMap = new JSONObject()
    var total = 0L
    label.collect().foreach(x => {
      labelMap.put(x.get(0) + "", x.get(1))
      total += x.get(1).toString.toLong
    })
    summaryMap.put("label", labelMap)
    summaryMap.put("total", total)

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

    bucketMap.put("stat", statMap)
    bucketMap.put("summary", summaryMap)

    println(s"------------------bucketJson--------$startDate-------------------")
    println(bucketMap.toString())
    println(s"------------------bucketJson--------$endDate-------------------")
    println("costTime(s):" + costTime)

    spark.stop()
  }
}
