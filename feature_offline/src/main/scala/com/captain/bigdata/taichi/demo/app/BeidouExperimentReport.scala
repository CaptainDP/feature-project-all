package com.captain.bigdata.taichi.demo.app

import java.util.Date

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sun.misc.BASE64Decoder
import taichi.utils.{AutoMessage, SendHTMLEmail}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


object BeidouExperimentReport {

  case class Result(title: String,
                    dt: String,
                    trial_id: String,
                    biz_type: String,

                    duizhaoCtr: String,
                    shiyanCtr: String,
                    diffCtr: String,
                    var duizhaoDuration: String,
                    var shiyanDuration: String,
                    var diffDuration: String
                   ) extends java.lang.Comparable[Result] {
    override def compareTo(that: Result): Int = {
      if (this.title == that.title) {
        if (this.dt == that.dt) {
          if (this.trial_id == that.trial_id) {
            if (this.biz_type == that.biz_type) {
              0
            } else {
              this.biz_type.compareToIgnoreCase(that.biz_type)
            }
          } else {
            this.trial_id.compareToIgnoreCase(that.trial_id)
          }
        } else {
          this.dt.compareToIgnoreCase(that.dt)
        }
      } else {
        this.title.compareToIgnoreCase(that.title)
      }
    }
  }

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
    val expsStr = jsonObj.getString("exps")
    //    val exps = "'视频模型_v7_1_增加特征调整参数','视频模型_v0001_增加特征','video_multi_model_improve_v0101','视频实验_v180扩量','user_perfer_video_score_list','视频精排分分时段调权'"
    val exps = expsStr.replaceAll(" ", "").split(",").map(x => "'" + x + "'").mkString(",")

    val debug = jsonObj.getBooleanValue("debug")

    var email = jsonObj.getString("email")
    if (debug) {
      email = "chendapeng@autohome.com.cn"
    }
    val toList = email.split(",").toBuffer
    //    val toList = new ArrayBuffer[String]()
    //    toList.append("chendapeng@autohome.com.cn")
    //    toList.append("zhanglina11592@autohome.com.cn")
    //    toList.append("lianshuailong@autohome.com.cn")
    //    toList.append("liuyuxing@autohome.com.cn")
    //    toList.append("liuyizhuang@autohome.com.cn")
    //
    var users = jsonObj.getString("ding")
    if (debug) {
      users = "13830"
    }
    //    var users = "13830"
    //    users += ",11592,14325,14810,15333"

    var days = jsonObj.getString("days")
    if (days == null || days.equals("")) {
      days = "3"
    }

    val sparkConf = new SparkConf();
    sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    //------------------------------------------实验整体ctr--------------------------------------------------
    var ctr_query_sql =
      """
        |select
        |    title,dt,trial_id,round(click_pv/sight_show_pv*100,2)
        |from
        |    rdm.rdm_app_rcmd_trial_clear_di
        |where
        |    title in (exps)
        |    and trial_id like 'is%'
        |    and dt > date_sub(to_date('curr_dt'),days) and dt <= to_date('curr_dt')
        |order by
        |    title,dt,trial_id
        |""".stripMargin

    ctr_query_sql = ctr_query_sql.replaceAll("exps", exps)
    ctr_query_sql = ctr_query_sql.replaceAll("curr_dt", dt)
    ctr_query_sql = ctr_query_sql.replaceAll("days", days)
    var df = spark.sql(ctr_query_sql)

    var i = 0
    var ctrtmp = 0.0
    var list = ArrayBuffer[Result]()
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(3).toString.toDouble - ctrtmp).formatted("%.2f")
        val result = Result(x.get(0).toString, x.get(1).toString, x.get(2).toString, "all", ctrtmp + "%", x.get(3) + "%", diff + "%", "", "", "")
        list.append(result)
      } else {
        ctrtmp = x.get(3).toString.toDouble
      }
      i += 1
    })


    //--------------------------------------------实验整体时长------------------------------------------------
    var duration_query_sql =
      """
        |select
        |    title,dt,trial_id,round(duration/duration_uv,2),round(finish_read_pv/duration_pv,2)
        |from
        |    rdm.rdm_app_rcmd_trial_nocache_finishread_duration_di
        |where
        |    title in (exps)
        |    and trial_id like 'is%'
        |    and dt > date_sub(to_date('curr_dt'),days) and dt <= to_date('curr_dt')
        |order by
        |    title,dt,trial_id
        |""".stripMargin

    duration_query_sql = duration_query_sql.replaceAll("exps", exps)
    duration_query_sql = duration_query_sql.replaceAll("curr_dt", dt)
    duration_query_sql = duration_query_sql.replaceAll("days", days)

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

    //----------------------------------------业务类型ctr----------------------------------------------------
    var chejiahao_ctr_query_sql =
      """
        |select
        |    title,dt,trial_id,biztype_id,round(click_pv/sight_show_pv*100,2)
        |from
        |    rdm.rdm_app_rcmd_title_biztype_clear_di
        |where
        |    title in (exps)
        |    and trial_id like 'is%' and biztype_id in (3,14,66)
        |    and dt > date_sub(to_date('curr_dt'),days) and dt <= to_date('curr_dt')
        |order by
        |    title,dt,biztype_id,trial_id
        |""".stripMargin

    chejiahao_ctr_query_sql = chejiahao_ctr_query_sql.replaceAll("exps", exps)
    chejiahao_ctr_query_sql = chejiahao_ctr_query_sql.replaceAll("curr_dt", dt)
    chejiahao_ctr_query_sql = chejiahao_ctr_query_sql.replaceAll("days", days)

    df = spark.sql(chejiahao_ctr_query_sql)
    val chejiahaovideo_list = ArrayBuffer[Result]()
    var chejiahaovideo_ctrtmp = 0.0
    i = 0
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(4).toString.toDouble - chejiahaovideo_ctrtmp).formatted("%.2f")
        val result = Result(x.get(0).toString, x.get(1).toString, x.get(2).toString, x.get(3).toString, chejiahaovideo_ctrtmp + "%", x.get(4) + "%", diff + "%", "", "", "")
        chejiahaovideo_list.append(result)
      } else {
        chejiahaovideo_ctrtmp = x.get(4).toString.toDouble
      }
      i += 1
    })

    //----------------------------------------业务类型时长----------------------------------------------------
    var chejiahao_duration_query_sql =
      """
        |select
        |    title,dt,trial_id,biz_type,round(duration/duration_uv,2)
        |from
        |    rdm.rdm_app_rcmd_trial_rtype_nocache_finishread_duration_di
        |where
        |    title in (exps)
        |    and trial_id like 'is%' and biz_type in (3,14,66)
        |    and dt > date_sub(to_date('curr_dt'),days) and dt <= to_date('curr_dt')
        |order by
        |    title,dt,biz_type,trial_id
        |""".stripMargin

    chejiahao_duration_query_sql = chejiahao_duration_query_sql.replaceAll("exps", exps)
    chejiahao_duration_query_sql = chejiahao_duration_query_sql.replaceAll("curr_dt", dt)
    chejiahao_duration_query_sql = chejiahao_duration_query_sql.replaceAll("days", days)

    df = spark.sql(chejiahao_duration_query_sql)
    var chejiahaovideo_durationtmp = 0.0
    i = 0
    df.collect().foreach(x => {
      if (i % 2 == 1) {
        val diff = (x.get(4).toString.toDouble - chejiahaovideo_durationtmp).formatted("%.2f")
        val result = chejiahaovideo_list((i - 1) / 2)
        if (result.title.equals(x.get(0).toString) && result.dt.equals(x.get(1).toString) && result.trial_id.equals(x.get(2)) && result.biz_type.equals(x.get(3))) {
          result.duizhaoDuration = chejiahaovideo_durationtmp + ""
          result.shiyanDuration = x.get(4).toString
          result.diffDuration = diff
        }
      } else {
        chejiahaovideo_durationtmp = x.get(4).toString.toDouble
      }
      i += 1
    })

    list.appendAll(chejiahaovideo_list)
    list = list.sorted

    val dataList = new ArrayBuffer[List[String]]()
    list.foreach(x => {
      val oneList = new ArrayBuffer[String]()
      oneList.append(x.title)
      oneList.append(x.dt)
      oneList.append(x.biz_type)

      //-----------实验大盘--------------
      oneList.append(x.duizhaoCtr)
      oneList.append(x.shiyanCtr)
      oneList.append(x.duizhaoDuration)
      oneList.append(x.shiyanDuration)
      oneList.append(x.diffCtr)
      oneList.append(x.diffDuration)
      if (x.diffCtr.replaceAll("%", "").toDouble <= 0 || x.diffDuration.toDouble <= 0) {
        oneList.append("--")
      } else {
        oneList.append("双正向")
      }

      dataList.append(oneList.toList)
    }
    )

    val host: String = "114.251.201.21"
    val from: String = "chendapeng@autohome.com.cn"


    val title = "北斗实验ctr和时长效果数据"
    val titleList = ArrayBuffer("实验名称", "数据日期", "业务类型", "对照桶ctr", "实验桶ctr", "对照桶时长", "实验桶时长", "ctr涨幅", "时长涨幅", "实验效果")


    val htmlContent = getDemo(titleList.toList, dataList.toList, days)
    val flag = SendHTMLEmail.SendMail(host, from, toList.asJava, title, htmlContent)
    if (!flag) {
      println("send msg error!!!")
      System.exit(1)
    }


    AutoMessage.send("北斗实验ctr和时长效果数据", "邮件已发送请查收", users, "ding")

    spark.stop()
  }

  def getDemo(titleList: List[String], list: List[List[String]], days: String): String = {
    val content = new StringBuilder("<html><head></head><body>")
    content.append("<table border=\"1\" style=\"width:1000px; height:150px;border:solid 1px #E8F2F9;font-size=11px;font-size:11px;\">")
    var titleTmp = "<tr>"
    for (str <- titleList) {
      titleTmp += "<td>" + str + "</td>"
    }
    titleTmp += "</tr>"
    content.append(titleTmp)
    var i = 0

    for (line <- list) {
      var tmp = ""
      var j = 0
      for (str <- line) {
        if (j == 9) {
          if (str.contains("双正向")) {
            tmp += "<td><font color=\"#FF0000\"><span>" + str + "</span></td>"
          } else {
            tmp += "<td><span>" + str + "</span></td>"
          }
        } else {
          if (!line.contains("all")) {
            tmp += "<td><font color=\"#000000\"><span>" + str + "</span></td>"
          } else {
            tmp += "<td><font color=\"#0000FF\"><span>" + str + "</span></td>"
          }
        }
        j += 1
      }
      content.append("<tr>" + tmp + "</tr>")
      i += 1
    }

    content.append("</table>")
    content.append("</body></html>")
    content.toString
  }

}
