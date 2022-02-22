package com.captain.bigdata.taichi.demo.app_recall

import java.util.Properties

import com.captain.bigdata.taichi.demo.utils.RecallUtils.{getDiffDatetime, saveHbase}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer


object ItemCFApp {

  //协同过滤
  //https://zhuanlan.zhihu.com/p/394057852
  //https://github.com/xiaogp/recsys_spark

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("itemCF")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    /**
     * window_days: 时间窗口
     * similar_item_num: 商品的候选相似商品数量
     * hot_item_regular: 热门商品惩罚力度
     * profile_decay: 用户偏好时间衰减率
     * recommend_num:  推荐商品数量
     */

    //    val properties = getProPerties(args(0))
    val properties = new Properties()
    val window_days = properties.getProperty("window_days", "3").toInt
    val similar_item_num = properties.getProperty("similar_item_num", "10").toInt
    val hot_item_regular = properties.getProperty("hot_item_regular", "0.5").toDouble
    val profile_decay = properties.getProperty("profile_decay", "1").toDouble
    val recommend_num = properties.getProperty("recommend_num", "10").toInt
    val start_date = getDiffDatetime(window_days)
    val table_date = getDiffDatetime(0)

    println(s"训练时间窗口:${start_date} => ${table_date}")

    val sql = s"select upper(device_id),concat(object_type,'-',object_id),dt from gdm.gdm_04_app_rcmd_di where dt >= '${start_date}' and click_num > 0"
    println("sql:" + sql)
    val df_sales = spark.sql(sql)
      .toDF("user_id", "item_id", "biz_date").cache()

    println(s"数量:${df_sales.count()}")

    // 构建用户购买序列
    val df_sales1 = df_sales.groupBy("user_id").agg(collect_set("item_id").as("item_id_set"))

    // 商品共现矩阵
    val df_sales2 = df_sales1.flatMap { row =>
      val itemlist = row.getAs[scala.collection.mutable.WrappedArray[String]](1).toArray.sorted
      val result = new ArrayBuffer[(String, String, Double)]()
      for (i <- 0 to itemlist.length - 2) {
        for (j <- i + 1 until itemlist.length) {
          result += ((itemlist(i), itemlist(j), 1.0 / math.log(1 + itemlist.length))) // 热门user惩罚
        }
      }
      result
    }.withColumnRenamed("_1", "item_idI").withColumnRenamed("_2", "item_idJ").withColumnRenamed("_3", "score")

    val df_sales3 = df_sales2.groupBy("item_idI", "item_idJ").agg(sum("score").as("sumIJ"))

    // 计算商品的购买次数
    val df_sales0 = df_sales.withColumn("score", lit(1)).groupBy("item_id").agg(sum("score").as("score"))

    // 计算共现相似度,N ∩ M / srqt(N * M), row_number取top top_similar_item_num
    val df_sales4 = df_sales3.join(df_sales0.withColumnRenamed("item_id", "item_idJ").withColumnRenamed("score", "sumJ").select("item_idJ", "sumJ"), "item_idJ")
    val df_sales5 = df_sales4.join(df_sales0.withColumnRenamed("item_id", "item_idI").withColumnRenamed("score", "sumI").select("item_idI", "sumI"), "item_idI")
    val df_sales6 = df_sales5.withColumn("result", bround(col("sumIJ") / sqrt(col("sumI") * col("sumJ")), 5)).withColumn("rank", row_number().over(Window.partitionBy("item_idI").orderBy($"result".desc))).filter(s"rank <= ${similar_item_num}").drop("rank")

    // itme1和item2交换
    val df_sales8 = df_sales6.select("item_idI", "item_idJ", "sumJ", "result").union(df_sales6.select($"item_idJ".as("item_idI"), $"item_idI".as("item_idJ"), $"sumI".as("sumJ"), $"result")).withColumnRenamed("result", "similar").cache()
    val itemcf_similar = df_sales8.map { row =>
      val item_idI = row.getString(0)
      val item_idJ_similar = (row.getString(1).toString, row.getDouble(3))
      (item_idI, item_idJ_similar)
    }.toDF("item_id", "similar_items").groupBy("item_id").agg(collect_list("similar_items").as("similar_items"))

    // 计算用户偏好
    val score = df_sales.withColumn("pref", lit(1) / (datediff(current_date(), $"biz_date") * profile_decay + 1)).groupBy("user_id", "item_id").agg(sum("pref").as("pref"))

    // 连接用户偏好，商品相似度
    val df_user_prefer1 = df_sales8.join(score, $"item_idI" === $"item_id", "inner")

    // 偏好 × 相似度 × 商品热度降权
    val df_user_prefer2 = df_user_prefer1.withColumn("score", col("pref") * col("similar") * (lit(1) / log(col("sumJ") * hot_item_regular + math.E))).select("user_id", "item_idJ", "score")

    // 取推荐top，把已经购买过的去除
    val df_user_prefer3 = df_user_prefer2.groupBy("user_id", "item_idJ").agg(sum("score").as("score")).withColumnRenamed("item_idJ", "item_id")
    val df_user_prefer4 = df_user_prefer3.join(score, Seq("user_id", "item_id"), "left").filter("pref is null")
    val itemcf_recommend = df_user_prefer4.select($"user_id", $"item_id".cast("String"), $"score").withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy($"score".desc))).filter(s"rank <= ${recommend_num}").groupBy("user_id").agg(collect_list("item_id").as("recommend"))


    itemcf_similar.show(false)
    // 保存商品共现相似度数据
    saveHbase(itemcf_similar, "ITEMCF_SIMILAR")

    itemcf_recommend.show(false)
    // 保存用户偏好推荐数据
    saveHbase(itemcf_recommend, "ITEMCF_RECOMMEND")

    spark.close()

  }
}
