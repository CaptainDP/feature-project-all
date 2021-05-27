package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer


object FeatureSampleMerge {

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

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    val tableName = "cmp_tmp_train_user_click_sequence_v1"
    val input_path = "viewfs://AutoLfCluster/team/cmp/hive_db/tmp/" + tableName + "/dt="
    val result_path = "viewfs://AutoLfCluster/team/cmp/hive_db/tmp/" + tableName + "_all"

    val pathList = new ArrayBuffer[String]()

    for (i <- 0 to preCount) {
      val preDate = DateUtil.calcDateByFormat(date, "yyyy-MM-dd(-" + i + "D)")
      val filePath = input_path + preDate
      pathList.append(filePath)
    }

    println("pathList:" + pathList)

    var dataFrame: DataFrame = null
    pathList.foreach(x => {
      val df = spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load(x)

      if (dataFrame == null) {
        dataFrame = df
      } else {
        dataFrame = dataFrame.union(df)
      }
    })

    dataFrame = dataFrame.repartition(100)
    dataFrame.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(result_path)

  }

}
