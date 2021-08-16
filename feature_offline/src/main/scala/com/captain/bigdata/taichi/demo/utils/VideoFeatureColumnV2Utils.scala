package com.captain.bigdata.taichi.demo.utils

import java.text.DecimalFormat

import com.captain.bigdata.taichi.util.DateUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object VideoFeatureColumnV2Utils {


  def getTimeDecay(start: String, end: String): Double = {
    if (start == null || start.trim.equals("")) {
      0
    } else {
      val startTime = DateUtil.toDate(start, "yyyy-MM-dd HH:mm:ss")
      val startTimeStr = DateUtil.getDateTime(startTime, "yyyy-MM-dd HH:mm:ss.SSS")
      val diff = DateUtil.getTimeDiff(end, startTimeStr, "yyyy-MM-dd HH:mm:ss.SSS")
      Math.exp(-0.0005 * diff / 600.0)
    }
  }

  def isNumeric(str: String): Boolean = {
    var flag = false
    try {
      java.lang.Double.parseDouble(str);
      flag = true
    } catch {
      case e: Exception => flag = false
    }
    flag
  }

  def double2String6(value: Double): String = {
    new DecimalFormat("##.######").format(value)
  }

  def double2String5(value: Double): String = {
    new DecimalFormat("##.#####").format(value)
  }

  def double2String3(value: Double): String = {
    new DecimalFormat("##.###").format(value)
  }

  def getMaxMin(list: ArrayBuffer[Float], min: Float, max: Float): ArrayBuffer[Float] = {
    val diff = max - min + 1.0
    list.map(x => {
      val d = (x - min + 1.0) / diff
      d.toFloat
    })
  }

  def getSeriesRate(list: ArrayBuffer[Float], sum: Float): ArrayBuffer[Float] = {
    list.map(x => {
      x / sum
    })
  }


  case class MatchSeries(pvMaxMin: Float, durMaxMin: Float, pvRate: Float, durRate: Float, pv: Float, dur: Float)

  //userFeature.series_list_XX
  //source: 4693-2-17;771-2-4;770-12-114;614-19-34;2313-7-38;5373-1-6;314-36-355;1-2-119;4869-1-3;
  //_ser, _pv, _dur = source.split("-")
  def getSeriesList(column: String): mutable.HashMap[String, MatchSeries] = {
    val map = mutable.HashMap[String, MatchSeries]()
    if (column != null && !column.equals("")) {

      val _series = ArrayBuffer[String]()
      val _pvs = ArrayBuffer[Float]()
      val _durs = ArrayBuffer[Float]()

      //pv
      var pvSum = 0.0f
      var pvMin = 0.0f
      var pvMax = 0.0f

      //dur
      var durSum = 0.0f
      var durMin = 0.0f
      var durMax = 0.0f

      val series_list = column.split(";")
      series_list.foreach(x => {
        val ss = x.split("-")
        val _ser = ss(0)
        val _pv = ss(1).toString.toFloat
        val _dur = ss(2).toString.toFloat

        _series.append(_ser)
        _pvs.append(_pv)
        _durs.append(_dur)

        //pv
        pvSum += _pv.toString.toFloat
        if (_pv < pvMin) {
          pvMin = _pv
        }

        if (_pv > pvMax) {
          pvMax = _pv
        }

        //dur
        durSum += _dur.toString.toFloat
        if (_dur < durMin) {
          durMin = _dur
        }

        if (_dur > durMax) {
          durMax = _dur
        }

      }
      )

      val _pvsRate = getSeriesRate(_pvs, pvSum)
      val _dursRate = getSeriesRate(_pvs, durSum)

      val pvMaxMin = getMaxMin(_pvs, pvMin, pvMax)
      val durMaxMin = getMaxMin(_durs, durMin, durMax)

      for (i <- _series.indices) {
        map(_series(i)) = MatchSeries(pvMaxMin(i), durMaxMin(i), _pvsRate(i), _dursRate(i), _pvs(i), _durs(i))
      }

      map
    } else {
      map
    }
  }

  def get_match_series(uniq_series_ids: String, user_series_list_xx: String): MatchSeries = {

    if (uniq_series_ids != null && !uniq_series_ids.equals("")) {

      val user_series_list_xx_Map = getSeriesList(user_series_list_xx)

      val uniq_series_ids_list = uniq_series_ids.split(";")
      var match_series_pv_maxmin_xx = 0.0f
      var match_series_dur_maxmin_xx = 0.0f
      var match_series_pv_weight_xx = 0.0f
      var match_series_dur_weight_xx = 0.0f
      var match_series_pv_xx = 0.0f
      var match_series_dur_xx = 0.0f

      var count = 0
      for (i <- 0 until uniq_series_ids_list.length) {
        val oneMatchSeries = user_series_list_xx_Map(uniq_series_ids_list(i))
        if (oneMatchSeries != null) {
          match_series_pv_maxmin_xx += oneMatchSeries.pvMaxMin
          match_series_dur_maxmin_xx += oneMatchSeries.durMaxMin
          match_series_pv_weight_xx += oneMatchSeries.pvRate
          match_series_dur_weight_xx += oneMatchSeries.durRate
          match_series_pv_xx += oneMatchSeries.pv
          match_series_dur_xx += oneMatchSeries.dur
          count += 1
        }
      }

      if (count > 0) {
        match_series_pv_maxmin_xx = match_series_pv_maxmin_xx / count
        match_series_dur_maxmin_xx = match_series_dur_maxmin_xx / count
        match_series_pv_weight_xx = match_series_pv_weight_xx / count
        match_series_dur_weight_xx = match_series_dur_weight_xx / count
        match_series_pv_xx = match_series_pv_xx / count
        match_series_dur_xx = match_series_dur_xx / count
      }
      MatchSeries(match_series_pv_maxmin_xx, match_series_dur_maxmin_xx, match_series_pv_weight_xx, match_series_dur_weight_xx, match_series_pv_xx, match_series_dur_xx)

    } else {
      MatchSeries(0, 0, 0, 0, 0, 0)
    }
  }

  //source:pv,uv
  //target:pv/uv
  def callback_ratio(pv: String, uv: String): String = {
    if (pv != null && !pv.equals("") && isNumeric(pv) && uv != null && !uv.equals("") && isNumeric(uv)) {
      val ratio = (pv.toDouble + 1.0) / (uv.toDouble + 1.0)
      double2String3(ratio)
    } else {
      "0.0"
    }
  }

  //https://juejin.cn/post/6844903494135054350
  //source:
  //target:
  def wilson_score(pos: String, total: String, p_z: Double = 5.0): String = {
    if (pos != null && !pos.equals("") && isNumeric(pos) && total != null && !total.equals("") && isNumeric(total)) {
      val posDouble = pos.toDouble
      val totalDouble = total.toDouble + 1
      val pos_rat = posDouble / totalDouble

      val score = ((pos_rat + (p_z * p_z) / (2 * totalDouble)) - ((p_z / (2 * totalDouble)) * Math.sqrt(4 * totalDouble * (1 - pos_rat) * pos_rat + p_z * p_z))) / (1 + p_z * p_z / totalDouble)
      double2String5(score)
    } else {
      "0.0"
    }
  }

  def agee_score(pos: String, pos_z: Double, total: String, total_z: Double): String = {
    if (pos != null && !pos.equals("") && isNumeric(pos) && total != null && !total.equals("") && isNumeric(total)) {
      val posDouble = pos.toDouble
      val totalDouble = total.toDouble + 1
      val score = (posDouble + pos_z) / (totalDouble + total_z)
      double2String5(score)
    } else {
      "0.0"
    }
  }


  //source:
  //target:
  def log(column: String): String = {
    if (column != null && !column.equals("") && isNumeric(column)) {
      double2String6(math.log(column.toDouble + 1))
    } else {
      "0.0"
    }
  }

  //source: WEY:0.181368;坦克300:0.231121;本田CR-V新能源:0.11664;
  //target: WEY;坦克300;本田CR-V新能源;
  def getKeysFromSemicolonList(column: String): String = {
    if (column != null && !column.equals("")) {
      val list1 = column.split(";")
      val list2 = list1.map(x => x.split(":")(0))
      list2.mkString(";")
    } else {
      column
    }
  }

  //替换逗号为分号，并去重
  //source: 5772,5772,2061,5395,5772,4232,2893,5239,166
  //target: 5772;2061;5395;5772;4232;2893;5239;166
  def replaceComma2SemicolonDistinct(column: String): String = {
    if (column != null && !column.equals("")) {
      column.split(",").distinct.mkString(";")
    } else {
      column
    }
  }

  //source: "78,496,2246"
  //target: 78,496,2246
  def replaceQuotes(column: String): String = {
    if (column != null && !column.equals("")) {
      column.replaceAll("\"", "")
    } else {
      column
    }
  }

}
