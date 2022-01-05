package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sun.misc.BASE64Decoder


object EvelGAUC {

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
    val sourceHdfsPath = jsonObj.getString("sourceHdfsPath")
    if (sourceHdfsPath == null || sourceHdfsPath.toString.trim.equals("")) {
      println("sourceHdfsPath is null")
      System.exit(-1)
    }

    var isGAucByDeviceid = jsonObj.getBoolean("isGAucByDeviceid")
    if (isGAucByDeviceid == null) {
      isGAucByDeviceid = true
    }

    var isProb = jsonObj.getBoolean("isProb")
    if (isProb == null) {
      isProb = true
    }

    val model_name = jsonObj.getString("model_name")
    var model_condition = "1=1"
    if (model_name != null && !model_name.toString.trim.equals("")) {
      model_condition = "model_name in (" + model_name + ")"
    }

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    //    val data_path = "D:\\workspace\\py_workspace\\edge_model\\deepfm_edge\\train_data_v0102_result2"
    //    val data_path = "hdfs://AutoRouter/team/cmp/hive_db/tmp/evel_result/esmm_model_video_v0102/2022-01-04_10-08-42_result"
    val data_path = sourceHdfsPath

    var df = spark.read.format("json").load(data_path)
    df.createOrReplaceTempView("tmp1")

    spark.sql("create temporary function AucUDF as 'taichi.udfs.AucUDF'")

    var sql1 = ""
    if (isGAucByDeviceid) {
      //采用deviceid计算auc
      if (isProb) {
        //采用预测的prob计算auc
        sql1 = s"select AucUDF(collect_list(prob) ,collect_list(label)) as device_id_auc from tmp1 where $model_condition group by device_id"
      } else {
        //采用位置posid计算auc
        sql1 = s"select AucUDF(collect_list(1-posid/1000) ,collect_list(label)) as device_id_auc from tmp1 where $model_condition group by device_id"
      }
    } else {
      //采用pvid计算auc
      if (isProb) {
        //采用预测的prob计算auc
        sql1 = s"select AucUDF(collect_list(prob) ,collect_list(label)) as device_id_auc from tmp1 where $model_condition group by pvid"
      } else {
        //采用位置posid计算auc
        sql1 = s"select AucUDF(collect_list(1-posid/1000) ,collect_list(label)) as device_id_auc from tmp1 where $model_condition group by pvid"
      }
    }

    println("sql1:" + sql1)
    df = spark.sql(sql1)
    df.createOrReplaceTempView("tmp2")

    val sql2 = "select sum(split(device_id_auc,',')[0]) as auc_sum,sum(split(device_id_auc,',')[1]) as auc_count_num,sum(split(device_id_auc,',')[0]) / sum(split(device_id_auc,',')[1]) as gauc, sum(split(device_id_auc,',')[2]) as count_sum, count(1) as device_id_count from tmp2"
    spark.sql(sql2).show()

    spark.stop()
  }

}
