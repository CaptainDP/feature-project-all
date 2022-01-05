package com.captain.bigdata.taichi.demo.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object EvelGAUC {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    //    val data_path = "D:\\workspace\\py_workspace\\edge_model\\deepfm_edge\\train_data_v0102_result2"
    val data_path = "hdfs://AutoRouter/team/cmp/hive_db/tmp/evel_result/esmm_model_video_v0102/2022-01-05_09-34-55_result"

    var df = spark.read.format("json").load(data_path)
    df.createOrReplaceTempView("tmp1")

    spark.sql("create temporary function AucUDF as 'taichi.udfs.AucUDF'")

    df = spark.sql("select AucUDF(collect_list(prob) ,collect_list(label)) as device_id_auc from tmp1 group by device_id")
    df.createOrReplaceTempView("tmp2")

    spark.sql("select sum(split(device_id_auc,',')[0]) as auc_sum,sum(split(device_id_auc,',')[1]) as count_num,sum(split(device_id_auc,',')[0]) / sum(split(device_id_auc,',')[1]) as gauc from tmp2").show()

    spark.stop()
  }

}
