package com.captain.bigdata.taichi.demo.rdd


import com.captain.bigdata.taichi.process.transfer.SqlTransfer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType

/**
  * UserAnalyzeRdd
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2019/11/15 10:40
  * @func Analyze user gender and age group
  */
class UserAnalyzeRdd extends SqlTransfer {

  override def extProcess: Unit = {

    val inputMap = context.inputMap
    val rdd = context.df.rdd

    val schema = context.df.schema
      .add("user_sex", StringType)
      .add("user_age_group", StringType)

    val resulRdd = rdd.map(x => {

      val user_id = x.getAs[String]("user_id")
      val user_age = x.getAs[String]("user_age")

      val rspRow = getRow(user_id, user_age)

      val resultRow = Row.merge(x, rspRow)
      resultRow
    })

    val resultDf = context.session.createDataFrame(resulRdd, schema)

    context.df = resultDf

  }

  def isNull(input: String): Boolean = {

    if (null == input || input.equals("") || input.trim.toUpperCase().equals("NULL")) {
      true
    } else {
      false
    }

  }

  def getRow(user_id: String, user_age: String) = {

    var user_sex = "0"
    var user_age_group = "0"

    if (user_id != null && !user_id.isEmpty) {
      user_sex = (user_id.toInt % 2).toString
    }

    if (user_age != null && !user_age.isEmpty) {
      user_age_group = (user_age.toInt / 10).toString
    }

    Row(user_sex, user_age_group)
  }

}

object UserAnalyzeRdd {

  def main(args: Array[String]): Unit = {

    val user_id = "345"
    val user_age = "29"

    val udf = new UserAnalyzeRdd()
    val result = udf.getRow(user_id, user_age)

    println(result)
  }

}
