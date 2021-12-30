package com.captain.bigdata.taichi.demo.udf

import java.util

import com.alibaba.fastjson.JSON
import com.captain.bigdata.taichi.util.DateUtil
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}

import scala.collection.mutable.ArrayBuffer

/**
 * RankScoreUDF
 *
 * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
 * @date 2019/11/19 10:40
 * @func Analyze user gender and age group
 */
class RankScoreUDF extends GenericUDTF {

  var stringOI: PrimitiveObjectInspector = null
  var stringOI2: PrimitiveObjectInspector = null

  val DT = "dt"
  val DEVICE_ID = "device_id"
  val PVID = "pvid"
  val RTYPE = "rtype"
  val BIZ_ID = "biz_id"
  val SCORE = "score"
  val MODEL_NAME = "model_name"
  val PUSH_TIME = "push_time"

  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {

    if (args.length != 2)
      throw new UDFArgumentException("NameParserGenericUDTF() takes exactly tow argument")
    if ((args(0).getCategory ne ObjectInspector.Category.PRIMITIVE) && (args(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory ne PrimitiveObjectInspector.PrimitiveCategory.STRING))
      throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a 1 parameter")
    if ((args(1).getCategory ne ObjectInspector.Category.PRIMITIVE) && (args(1).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory ne PrimitiveObjectInspector.PrimitiveCategory.STRING))
      throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a 2 parameter")

    // input inspectors
    stringOI = args(0).asInstanceOf[PrimitiveObjectInspector]
    stringOI2 = args(1).asInstanceOf[PrimitiveObjectInspector]

    // output inspectors -- an object with fields!
    val fieldNames = new util.ArrayList[String]()
    fieldNames.add(DT)
    fieldNames.add(DEVICE_ID)
    fieldNames.add(PVID)
    fieldNames.add(RTYPE)
    fieldNames.add(BIZ_ID)
    fieldNames.add(SCORE)
    fieldNames.add(MODEL_NAME)
    fieldNames.add(PUSH_TIME)

    val fieldOIs = new util.ArrayList[ObjectInspector]()
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)

  }


  override def process(record: Array[AnyRef]): Unit = {

    var dt = stringOI.getPrimitiveJavaObject(record(0)).toString
    var json = stringOI2.getPrimitiveJavaObject(record(1)).toString

    val results = processInputRecord(dt, json)
    val it = results.iterator
    while ( {
      it.hasNext
    }) {
      val r = it.next
      forward(r)
    }
  }

  override def close(): Unit = {
  }

  def processInputRecord(dt: String, jsonStr: String): util.ArrayList[Array[AnyRef]] = {

    val result = new util.ArrayList[Array[AnyRef]]

    if (jsonStr == null && !jsonStr.isEmpty) {
      return result
    }

    val newDt = DateUtil.getDate(DateUtil.toDate(dt), "yyyy-MM-dd")

    val jsonObj = JSON.parseObject(jsonStr)
    val device_id = jsonObj.getString("device_id")
    val pvid = jsonObj.getString("pvid")
    val push_time = jsonObj.getString("push_time")
    val model_name = jsonObj.getString("model_name")
    val item_list = jsonObj.getJSONArray("item_list")
    val deep_model_score = jsonObj.getJSONArray("deep_model_score")


    var list = ArrayBuffer[String]()
    for (i <- 0 until item_list.size()) {
      val item_id = item_list.getString(i)
      val score = deep_model_score.getBigDecimal(i)
      list += newDt
      list += device_id
      list += pvid
      list += item_id.split("-")(0)
      list += item_id.split("-")(1)
      list += score.toString
      list += model_name
      list += push_time

      //      val newJsonObj = new JSONObject()
      //      newJsonObj.put("dt", newDt)
      //      newJsonObj.put("device_id", device_id)
      //      newJsonObj.put("pvid", pvid)
      //      newJsonObj.put("push_time", push_time)
      //      newJsonObj.put("model_name", model_name)
      //      newJsonObj.put("device_id", device_id)
      //      newJsonObj.put("rtype", item_id.split("-")(0))
      //      newJsonObj.put("biz_id", item_id.split("-")(1))
      //      newJsonObj.put("item_id", item_id)
      //      newJsonObj.put("score", score)
      //      newJsonObj.put("prob", score)

      result.add(list.toArray)
    }

    result
  }

}

object RankScoreUDF {

  def main(args: Array[String]): Unit = {

    val udf = new RankScoreUDF
    val jsonStr =
      """{
        |  "deep_model_score": [
        |    0.006509262602776289,
        |    0.09725915640592575,
        |    0.07107819616794586,
        |    0.01201448030769825,
        |    0.07042230665683746,
        |    0.06944744288921356,
        |    0.035271696746349335,
        |    0.03024158626794815,
        |    0.027580169960856438,
        |    0.01984945870935917,
        |    0.03002103418111801,
        |    0.003922153264284134,
        |    0.010866612195968628,
        |    0.010068323463201523,
        |    0.07171084731817245,
        |    0.008301716297864914,
        |    0.027157070115208626,
        |    0.0038244454190135,
        |    0.0029728137888014317,
        |    0.010296388529241085,
        |    0.0048299795016646385,
        |    0.0034456755965948105,
        |    0.004439510405063629
        |  ],
        |  "device_id": "83C1A8663B60A3F43AA9A4F75F555449B250B016",
        |  "item_list": [
        |    "010250020020004-10090180",
        |    "010250020020004-10049643",
        |    "010250020020004-10065592",
        |    "010250020020004-10083270",
        |    "010250020020004-10090465",
        |    "010250020020004-10091290",
        |    "010250020020004-10080926",
        |    "010250020020004-10068238",
        |    "010250020020004-10065426",
        |    "010250020020004-10087244",
        |    "010250020020004-10088989",
        |    "010250020020004-10044708",
        |    "010250020020004-10085933",
        |    "010250020020004-10078054",
        |    "010250020020004-10092806",
        |    "010250020020004-9986653",
        |    "010250020020004-10073362",
        |    "010250020020004-10057221",
        |    "010250020020004-10049053",
        |    "010250020020004-10053146",
        |    "010250020020004-10056489",
        |    "010250020020004-10060141",
        |    "010250020020004-9934400"
        |  ],
        |  "model_name": "esmm_model_video_v0102",
        |  "push_time": "20211228000423",
        |  "pvid": "1640621063521915092pcI7407UtO6tN",
        |  "simple_model_score": null,
        |  "user_model_embedding": null,
        |  "user_model_feature": null
        |}""".stripMargin
    val dt = "20210801"
    val result = udf.processInputRecord(dt, jsonStr)

    result.get(0).toList.foreach(println(_))

  }

}
