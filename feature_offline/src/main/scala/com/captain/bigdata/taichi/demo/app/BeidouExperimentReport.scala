package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import taichi.utils.AutoMessage


object BeidouExperimentReport {

  def main(args: Array[String]): Unit = {

    println("args:" + args.mkString(","))

    val options = new Options
    options.addOption("d", true, "date yyyy-MM-dd [default yesterday]")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    //date
    var dt = DateUtil.getDate(new Date(), "yyyy-MM-dd")
    if (cmd.hasOption("d")) {
      dt = cmd.getOptionValue("d")
    }

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val exps = "'视频模型_v7_1_增加特征调整参数','视频模型_v0001_增加特征','video_multi_model_improve_v0101','视频实验_v180扩量','user_perfer_video_score_list','视频精排分分时段调权'"

    var ctr_query_sql =
      """
        |select
        |    title,dt,trial_id,round(click_pv/sight_show_pv*100,2)
        |from
        |    rdm.rdm_app_rcmd_trial_clear_di
        |where
        |    title in (exps)
        |    and trial_id like 'is%'
        |    and dt >= date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),3)
        |order by
        |    title,dt,trial_id
        |""".stripMargin

    ctr_query_sql = ctr_query_sql.replaceAll("exps", exps)
    var df = spark.sql(ctr_query_sql)

    var ctr_msg = "ctr:\n实验\t日期\t对照桶\t实验桶\tctr涨幅\n"
    var i = 0
    var ctrtmp = 0.0
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(3).toString.toDouble - ctrtmp).formatted("%.2f") + "%"
        val line = x.get(0) + "\t" + x.get(1) + "\t" + ctrtmp + "%\t" + x.get(3) + "%\t" + diff + "\n"
        ctr_msg += line
      } else {
        ctrtmp = x.get(3).toString.toDouble
      }
      i += 1
    })
    println("ctr_msg2:" + ctr_msg)


    //--------------------------------------------------------------------------------------------
    var duration_query_sql =
      """
        |select
        |    title,dt,trial_id,round(duration/duration_uv,2),round(finish_read_pv/duration_pv,2)
        |from
        |    rdm.rdm_app_rcmd_trial_nocache_finishread_duration_di
        |where
        |    title in (exps)
        |    and trial_id like 'is%'
        |    and dt >= date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),3)
        |order by
        |    title,dt,trial_id
        |""".stripMargin

    duration_query_sql = duration_query_sql.replaceAll("exps", exps)
    df = spark.sql(duration_query_sql)
    var duration_msg = "时长:\n实验\t日期\t对照桶\t实验桶\t时长涨幅\n"
    var durationtmp = 0.0
    i = 0
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(3).toString.toDouble - durationtmp).formatted("%.2f")
        val line = x.get(0) + "\t" + x.get(1) + "\t" + durationtmp + "\t" + x.get(3) + "\t" + diff + "\n"
        duration_msg += line
      } else {
        durationtmp = x.get(3).toString.toDouble
      }
      i += 1
    })
    println("duration_msg:" + duration_msg)

    val users = "13830,11592,14325,14810,15333"
    val msg = ctr_msg + "\n\n" + duration_msg
    AutoMessage.send("北斗实验ctr和时长效果数据", msg, users, "email")

    spark.stop()
  }

}
