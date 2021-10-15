package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import taichi.utils.AutoMessage

import scala.collection.mutable.ArrayBuffer


object BeidouExperimentReport {

  case class Result(title: String, dt: String, trial_id: String, duizhaoCtr: String, shiyanCtr: String, diffCtr: String, var duizhaoDuration: String, var shiyanDuration: String, var diffDuration: String)

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

    var i = 0
    var ctrtmp = 0.0
    val list = ArrayBuffer[Result]()
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(3).toString.toDouble - ctrtmp).formatted("%.2f")
        val result = Result(x.get(0).toString, x.get(1).toString, x.get(2).toString, ctrtmp + "%", x.get(3) + "%", diff + "%", "", "", "")
        list.append(result)
      } else {
        ctrtmp = x.get(3).toString.toDouble
      }
      i += 1
    })


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
    var durationtmp = 0.0
    i = 0
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(3).toString.toDouble - durationtmp).formatted("%.2f")
        val result = list((i - 1) / 2)
        if (result.title.equals(x.get(0).toString) && result.dt.equals(x.get(1).toString) && result.trial_id.equals(x.get(2))) {
          result.duizhaoDuration = durationtmp + ""
          result.shiyanDuration = x.get(3).toString
          result.diffDuration = diff
        }
      } else {
        durationtmp = x.get(3).toString.toDouble
      }
      i += 1
    })

    var msg = "ctr和时长：\n实验\t日期\t对照桶ctr\t实验桶ctr\tctr涨幅\t对照桶时长\t实验桶时长\t时长涨幅\n"

    list.foreach(x => {
      val line = x.title + "\t\t" + x.dt + "\t\t" + x.duizhaoCtr + "\t\t" + x.shiyanCtr + "\t\t" + x.diffCtr + "\t\t" + x.duizhaoDuration + "\t\t" + x.shiyanDuration + "\t\t" + x.diffDuration + "\n"
      msg += line
    })

    val users = "13830,11592,14325,14810,15333"
    AutoMessage.send("北斗实验ctr和时长效果数据", msg, users, "email")
    AutoMessage.send("北斗实验ctr和时长效果数据", "邮件已发送请查收", users, "ding")

    spark.stop()
  }

}
