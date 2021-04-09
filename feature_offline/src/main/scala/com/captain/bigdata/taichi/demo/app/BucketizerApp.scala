package com.captain.bigdata.taichi.demo.app

import java.io.File
import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.{DateUtil, UrlUtil}
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object BucketizerApp {

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
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    //date
    var dt = DateUtil.getDate(new Date(), "yyyy-MM-dd")
    if (cmd.hasOption("d")) {
      dt = cmd.getOptionValue("d")
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
    val basePath = new File("").getCanonicalPath
    println("basePath:" + basePath)
    val demo_path = UrlUtil.get("../../conf/conf/demo.csv", basePath).getPath
    val feature_fm_bucket_path = UrlUtil.get("../../conf/conf/feature_fm_bucket.json", basePath).getPath

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
    val featuresListOutput = featuresList.map(x => x + "_out")
    val featuresListOutputName = featuresListOutput.mkString(",")

    //读入数据-此处可以改成sql
    //    val dataFrame = spark.read.format("csv")
    //      .option("delimiter", ",")
    //      .option("header", "true")
    //      .option("quote", "'")
    //      .option("nullValue", "\\N")
    //      .option("inferSchema", "true")
    //      .load(train_data_path).toDF()

    val addList = Array("biz_id", "biz_type", "device_id")
    val sql = "select " + (addList ++ featuresList).mkString(",") + " from cmp_tmp." + tableName + " where dt = '" + dt + "'"
    println("sql:" + sql)
    val dataFrame = spark.sql(sql)

    //构造分桶器
    val bucketizer = new Bucketizer()
      .setInputCols(featuresList)
      .setOutputCols(featuresListOutput)
      .setSplitsArray(featureBucketList)

    // Transform original data into its bucket index.
    var bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with [" +
      s"${bucketizer.getSplitsArray(0).length - 1}, " +
      s"${bucketizer.getSplitsArray(1).length - 1}] buckets for each input column")
    val featuresListOutputReal = addList ++ featuresListOutput
    bucketedData = bucketedData.select(featuresListOutputReal.head, featuresListOutputReal.tail: _*)
    bucketedData = bucketedData.select(featuresListOutputName.split(",").map(name => col(name).cast(IntegerType)): _*)
    bucketedData.write.option("header", "true").mode("overwrite").csv(result_path)
    println("featuresListOutputReal:" + featuresListOutputReal)
    spark.stop()
  }
}
