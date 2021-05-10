package com.captain.bigdata.taichi.demo.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


/**
 * 计算用户点击序列
 */
object UserClickSequenceApp {

  case class FeatureBean(device_id: String, biz_id: String, recommend_time: String, series_ids: String, biz_type: String, author_id: String, uniq_category_name: String, brand_ids: String, like_cnt_90d: String, reply_cnt_30d: String, device_brand: String, start_time: String, dt: String)

  case class FeatureItemSeqBean(featureBean: FeatureBean, ItemSeqBean: Array[FeatureBean])

  def getString(x: FeatureBean): String = {
    x.dt + "," + x.device_id + "," + x.start_time + "," + x.biz_type + "," + x.biz_id + ","
  }

  def main(args: Array[String]): Unit = {

    val currDate = "2021-05-01"

    var sql =
      """select
        |device_id,
        |object_id as biz_id,
        |get_json_object(translate(item_feature,'\\;',''), '$.recommend_time') as recommend_time,
        |get_json_object(translate(item_feature,'\\;',''), '$.series_ids') as series_ids,
        |object_type as biz_type,
        |get_json_object(translate(item_feature,'\\;',''), '$.author_id') as author_id,
        |get_json_object(translate(item_feature,'\\;',''), '$.uniq_category_name') as uniq_category_name,
        |get_json_object(translate(item_feature,'\\;',''), '$.brand_ids') as brand_ids,
        |get_json_object(translate(item_feature,'\\;',''), '$.like_cnt_90d') as like_cnt_90d,
        |get_json_object(translate(item_feature,'\\;',''), '$.reply_cnt_30d') as reply_cnt_30d,
        |get_json_object(translate(user_feature,'\\;',''), '$.device_brand') as device_brand,
        |start_time as start_time,
        |dt
        |from rdm.rdm_app_rcmd_ai_feature_di
        |where dt='currDate'
        |and is_click = '1'
        |and device_id = '0059C87C5D95374FD17C00DB2EFAA73D39597B3E'
        |""".stripMargin
    sql = sql.replaceAll("currDate", currDate)
    println("sql:" + sql)

    val sparkConf = new SparkConf();
    //sparkConf.setAppName(this.getClass.getSimpleName)
    //    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .enableHiveSupport
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df = spark.sql(sql)
    val rdd = df.as[FeatureBean].rdd.map(x => (x.device_id, x))
    //    rdd.aggregateByKey()
    val groupRdd = rdd.groupByKey()
    val groupSeqRdd = groupRdd.map(x => {
      val lineBuffer = x._2.toBuffer
      val sortByTimeBuffer = lineBuffer.sortBy(_.start_time)
      val itemSeqList = new ArrayBuffer[FeatureItemSeqBean]()
      for (i <- sortByTimeBuffer.indices if sortByTimeBuffer(i).dt.equals(currDate)) {
        val oneSeq = new ArrayBuffer[FeatureBean]()
        val max = Math.min(i, 30)
        val min = i - max
        for (j <- min until max) {
          val itemJ = sortByTimeBuffer(j)
          oneSeq += itemJ
        }
        itemSeqList.append(FeatureItemSeqBean(sortByTimeBuffer(i), oneSeq.toArray))
      }
      itemSeqList
    }).flatMap(x => x)

    groupSeqRdd.collect().foreach(x => {
      val tmp = getString(x.featureBean)
      val tmpList = x.ItemSeqBean.map(x => getString(x)).mkString("#")
      println(tmp + "$" + tmpList)
    })
  }


}
