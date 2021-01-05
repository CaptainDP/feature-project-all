package com.captain.bigdata.taichi.demo.udf

import java.util

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}

import scala.collection.mutable.ArrayBuffer

/**
  * UserAnalyzeUDF
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2019/11/19 10:40
  * @func Analyze user gender and age group
  */
class UserAnalyzeUDF extends GenericUDTF {

  var stringOI: PrimitiveObjectInspector = null
  var stringOI2: PrimitiveObjectInspector = null

  //user_sex
  val USER_SEX = "user_sex"
  //user_age_group
  val USER_AGE_GROUP = "user_age_group"

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
    fieldNames.add(USER_SEX)
    fieldNames.add(USER_AGE_GROUP)

    val fieldOIs = new util.ArrayList[ObjectInspector]()
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)

  }


  override def process(record: Array[AnyRef]): Unit = {

    var user_id = stringOI.getPrimitiveJavaObject(record(0)).toString
    var user_age = stringOI2.getPrimitiveJavaObject(record(1)).toString

    val results = processInputRecord(user_id, user_age)
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

  def processInputRecord(user_id: String, user_age: String): util.ArrayList[Array[AnyRef]] = {

    val result = new util.ArrayList[Array[AnyRef]]

    var user_sex = 0
    var user_age_group = 0

    if (user_id != null && !user_id.isEmpty) {
      user_sex = user_id.toInt % 2
    }

    if (user_age != null && !user_age.isEmpty) {
      user_age_group = user_age.toInt / 10
    }

    var list = ArrayBuffer[String]()
    list += user_sex.toString
    list += user_age_group.toString

    result.add(list.toArray)

    result
  }

}

object UserAnalyzeUDF {

  def main(args: Array[String]): Unit = {

    val udf = new UserAnalyzeUDF
    val user_id = "123"
    val user_age = "39"
    val result = udf.processInputRecord(user_id, user_age)

    result.get(0).toList.foreach(println(_))

  }

}
