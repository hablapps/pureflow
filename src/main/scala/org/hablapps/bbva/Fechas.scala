package org.hablapps.bbva

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.commons.lang.StringUtils

import scala.util.matching.Regex


object Fechas {

  def getCalendarFromDate(planDate: Date): Calendar = {
    val cal = Calendar.getInstance
    cal.setTime(planDate)
    cal
  }

  def addDate(planDate: Date, days: Int): Date = {
    val cal = Calendar.getInstance
    cal.setTime(planDate)
    cal.add(Calendar.DAY_OF_MONTH, days)
    cal.getTime
  }

  def dateToHorizondate(date: Date): String = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy/MM/dd")
    formatter.format(date)
  }

  def dateToPartitionString(date: Date): String = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy, MM, dd")
    formatter.format(date)
  }

  def dateToPartitionFields(date: Date): Array[String] = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
    formatter.format(date).split("-")
  }
  def obtenerMes(mes: String): String = {

    if(mes=="Jan") "01"
    else if (mes=="Feb") "02"
    else if (mes=="Mar") "03"
    else if (mes=="Apr") "04"
    else if (mes=="May") "05"
    else if (mes=="Jun") "06"
    else if (mes=="Jul") "07"
    else if (mes=="Aug") "08"
    else if (mes=="Sep") "09"
    else if (mes=="Oct") "10"
    else if (mes=="Nov") "11"
    else if (mes=="Dec") "12"
    else null
  }

  def formatofecha(fecha: String, formato: String): String = {
    if (null != fecha) {
      //Patrón yyyy-MM-dd
      val pattern1 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2}")
      //Patrón dd/MM/yyyy HH:MM para
      val pattern2 = new Regex("[0-9]{2}/[0-9]{2}/[0-9]{4} ([0-9]{1}|[0-9]{2}):[0-9]{2}")
      //Patrón dd/MM/yyyy
      val pattern3 = new Regex("[0-9]{2}/[0-9]{2}/[0-9]{4}")
      //Patrón yyyy-MM-dd HH:mm:ss.S
      val pattern4 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1}")
      //Patrón yyyy-MM-ddTHH:mm:ss.SSS
      val pattern5 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}")
      //Patrón yyyy-MM-ddTHH:mm:ss.SSSSSS
      val pattern6 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}")
      //Patrón yyyy-MM-dd HH:mm:ss.SSSSS
      val pattern7 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{5}")
      //Patrón yyyyMMdd
      val pattern8 = new Regex("[0-9]{8}")
      //Patrón yyyyMMddHHmmss
      val pattern9 = new Regex("[0-9]{14}")
      //Patrón yyyy-MM-ddTHH:mm:ss
      val pattern10 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}")
      //Patrón dd/MM/yyyy HH:mm:ss
      val pattern11 = new Regex("[0-9]{2}/[0-9]{2}/[0-9]{4} ([0-9]{1}|[0-9]{2}):[0-9]{2}:[0-9]{2}")
      //Patrón dd/MM/yy HH.mm.ss.SSS
      val pattern12 = new Regex("[0-9]{2}/[0-9]{2}/[0-9]{2} ([0-9]{1}|[0-9]{2})\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{9}")
      //Patrón dd/MM/yy HH:m:ss.SSS
      val pattern13 = new Regex("[0-9]{2}/[0-9]{2}/[0-9]{2} ([0-9]{1}|[0-9]{2}):[0-9]{2}:[0-9]{2}\\.[0-9]{9}")
      // Patrón dd MMM yy
      val pattern14 = new Regex("[0-9]{2} (...) [0-9]{2}")
      //Patron yyyy-MM-ddTHH:mm:ss.SSSSSSZ
      val pattern15 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}Z")
      //Patrón yyyyMMddHHmmssSSSSSS
      val pattern16 = new Regex("[0-9]{20}")
      val pattern17 = new Regex("[0-9]{1} (...) [0-9]{2}")
      //Patrón dd/MM/yyyy HH:mm:ss.SSS
      val pattern18 = new Regex("[0-9]{2}/[0-9]{2}/[0-9]{4} ([0-9]{1}|[0-9]{2}):[0-9]{2}:[0-9]{2}\\.[0-9]{3}")
      //Patrón yyyyMMddhhmmssSSSSSS
      val pattern19 = new Regex("[0-9]{20}")
      //Patrón yyyy-MM-dd HH:mm:ss
      val pattern20 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2} ([0-9]{1}|[0-9]{2}):[0-9]{2}:[0-9]{2}")
      //Patrón yyyy-MM-dd HH:mm:ss.SSSSSS
      val pattern21 = new Regex("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}")
      val format1 = (pattern1 findAllIn fecha).mkString(",")
      val format2 = (pattern2 findAllIn fecha).mkString(",")
      val format3 = (pattern3 findAllIn fecha).mkString(",")
      val format4 = (pattern4 findAllIn fecha).mkString(",")
      val format5 = (pattern5 findAllIn fecha).mkString(",")
      val format6 = (pattern6 findAllIn fecha).mkString(",")
      val format7 = (pattern7 findAllIn fecha).mkString(",")
      val format8 = (pattern8 findAllIn fecha).mkString(",")
      val format9 = (pattern9 findAllIn fecha).mkString(",")
      val format10 = (pattern10 findAllIn fecha).mkString(",")
      val format11 = (pattern11 findAllIn fecha).mkString(",")
      val format12 = (pattern12 findAllIn fecha).mkString(",")
      val format13 = (pattern13 findAllIn fecha).mkString(",")
      val format14 = (pattern14 findAllIn fecha).mkString(",")
      val format15 = (pattern15 findAllIn fecha).mkString(",")
      val format16 = (pattern16 findAllIn fecha).mkString(",")
      val format17 = (pattern17 findAllIn fecha).mkString(",")
      val format18 = (pattern18 findAllIn fecha).mkString(",")
      val format19 = (pattern19 findAllIn fecha).mkString(",")
      val format20 = (pattern20 findAllIn fecha).mkString(",")
      val format21 = (pattern21 findAllIn fecha).mkString(",")
      if (fecha == format1) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha + "T00:00:000"
          case _ => null
        }
      } else if (fecha == format2) {
        formato match {
          case "yyyyMMdd" => fecha.substring(6, 10) + fecha.substring(3, 5) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(6, 10) + "-" + fecha.substring(3, 5) + "-" + fecha.substring(0, 2) + "T" + fecha.substring(11, 16) + ":00.000"
          case _ => null
        }
      } else if (fecha == format3) {
        formato match {
          case "yyyyMMdd" => fecha.substring(6, 10) + fecha.substring(3, 5) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(6, 10) + "-" + fecha.substring(3, 5) + "-" + fecha.substring(0, 2) + "T00:00:00.000"
          case _ => null
        }
      } else if (fecha == format4) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 10) + "T" + fecha.substring(11, 21) + "00"
          case "yyyy-MM-ddTHH:mm:ss.SSSSSS" => fecha.substring(0, 10) + "T" + fecha.substring(11, 21) + "00000"
          case _ => null
        }
      } else if (fecha == format5) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha
          case "yyyyMMddHHmmssSSSSSS" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10) + fecha.substring(11, 13) + fecha.substring(14, 16) + fecha.substring(17, 19) + fecha.substring(20, 23) + "000"
          case _ => null
        }
      } else if (fecha == format6) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 23)
          case "yyyyMMddHHmmssSSSSSS" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10) + fecha.substring(11, 13) + fecha.substring(14, 16) + fecha.substring(17, 19) + fecha.substring(20, 26)
          case _ => null
        }
      } else if (fecha == format7) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 10) + "T" + fecha.substring(11, 23)
          case "yyyyMMddHHmmssSSSSSS" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10) + fecha.substring(11, 13) + fecha.substring(14, 16) + fecha.substring(17, 19) + fecha.substring(20, 26)
          case _ => null
        }
      } else if (fecha == format8) {
        formato match {
          case "yyyyMMdd" => fecha
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha + "T00:00:00.000"
          case _ => null
        }
      } else if (fecha == format9) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 8)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 4) + "-" + fecha.substring(4, 6) + "-" + fecha.substring(6, 8) + "T" + fecha.substring(8, 10) + ":" + fecha.substring(10, 12) + ":" + fecha.substring(12, 14) + ".000"
          case _ => null
        }
      } else if (fecha == format10) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha + ".000"
          case _ => null
        }
      } else if (fecha == format11) {
        formato match {
          case "yyyyMMdd" => fecha.substring(6, 10) + fecha.substring(3, 5) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(6, 10) + "-" + fecha.substring(3, 5) + "-" + fecha.substring(0, 2) + "T" + fecha.substring(11, 19) + ".000"
          case _ => null
        }
      } else if (fecha == format12) {
        formato match {
          case "yyyyMMdd" => "20" + fecha.substring(6, 8) + fecha.substring(3, 5) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => "20" + fecha.substring(6, 8) + "-" + fecha.substring(3, 5) + "-" + fecha.substring(0, 2) + "T" + fecha.substring(9, 11) + ":" + fecha.substring(12, 14) + ":" + fecha.substring(15, 21)
          case _ => null
        }
      } else if (fecha == format13) {
        formato match {
          case "yyyyMMdd" => "20" + fecha.substring(6, 8) + fecha.substring(3, 5) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => "20" + fecha.substring(6, 8) + "-" + fecha.substring(3, 5) + "-" + fecha.substring(0, 2) + "T" + fecha.substring(9, 21)
          case _ => null
        }
      } else if (fecha == format14) {
        formato match {
          case "yyyyMMdd" => "20" + fecha.substring(7, 9) + obtenerMes(fecha.substring(3, 6)) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => "20" + fecha.substring(7, 9) + obtenerMes(fecha.substring(3, 6)) + fecha.substring(0, 2) + "T00:00:00.000"
          case _ => null
        }
      } else if (fecha == format15) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 23)
          case "yyyyMMddHHmmssSSSSSS" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10) + fecha.substring(11, 13) + fecha.substring(14, 16) + fecha.substring(17, 19) + fecha.substring(20, 26)
          case _ => null
        }
      } else if (fecha == format16) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 8)
          case "yyyy-MM-ddTHH:mm:ss.SSSSSS" => fecha.substring(0, 4) + "-" + fecha.substring(4, 6) + "-" + fecha.substring(6, 8) + "T" + fecha.substring(8, 10) + ":" + fecha.substring(10, 12) + ":" + fecha.substring(12, 14) + "." + fecha.substring(14, 20)
          case "yyyyMMddHHmmssSSSSSS" => fecha
          case _ => null
        }
      } else if (fecha == format17) {
        formato match {
          case "yyyyMMdd" => "20" + fecha.substring(6, 8) + obtenerMes(fecha.substring(2, 5)) + "0" + fecha.substring(0, 1)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => "20" + fecha.substring(6, 8) + obtenerMes(fecha.substring(2, 5)) + "0" + fecha.substring(0, 1) + "T00:00:00.000"
          case _ => null
        }
      } else if (fecha == format18) {
        formato match {
          case "yyyyMMdd" => "20" + fecha.substring(8, 10) + fecha.substring(3, 5) + fecha.substring(0, 2)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => "20" + fecha.substring(8, 10) + "-" + fecha.substring(3, 5) + "-" + fecha.substring(0, 2) + "T" + fecha.substring(11, 13) + ":" + fecha.substring(14, 16) + ":" + fecha.substring(17, 23)
          case _ => null
        }
      } else if (fecha == format19) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 6) + fecha.substring(7, 8)
          case "yyyy-MM-ddTHH:mm:ss.SSSSSS" => fecha.substring(0, 4) + "-" + fecha.substring(5, 6) + "-" + fecha.substring(7, 8) + "T" + fecha.substring(9, 10) + ":" + fecha.substring(11, 12) + ":" + fecha.substring(13, 14) + "." + fecha.substring(15, 20)
          case _ => null
        }
      } else if (fecha == format20) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 6) + fecha.substring(7, 8)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 10) + "T" + fecha.substring(11, 20) + ".000"
          case "yyyy-MM-ddTHH:mm:ss.SSSSSS" => fecha.substring(0, 10) + "T" + fecha.substring(11, 20) + ".000000"
          case _ => null
        }
      } else if (fecha == format21) {
        formato match {
          case "yyyyMMdd" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10)
          case "yyyy-MM-ddTHH:mm:ss.SSS" => fecha.substring(0, 10) + "T" + fecha.substring(11, 23)
          case "yyyyMMddHHmmssSSSSSS" => fecha.substring(0, 4) + fecha.substring(5, 7) + fecha.substring(8, 10) + fecha.substring(11, 13) + fecha.substring(14, 16) + fecha.substring(17, 19) + fecha.substring(20, 26)
          case _ => null
        }
      } else null
    }else null
  }

  val obtenerTuplaFecha: (String, Regex, String) => (String, String, String) = { (source_name: String, pattern:Regex, formato:String) =>
        val sFecha= pattern.findFirstIn(source_name) match {
          case Some(fecha: String) => fecha
          case _ => ""
        }
        val sdfFormato = new SimpleDateFormat(formato)
        if(StringUtils.isNotBlank(sFecha)){
          val dFecha = sdfFormato.parse(sFecha)
          val cal= Calendar.getInstance()
          cal.setTime(dFecha)
          //println(s"fecha: ${cal.get(Calendar.YEAR)} , ${cal.get(Calendar.MONTH)+1}, ${cal.get(Calendar.DAY_OF_MONTH)}")
          (cal.get(Calendar.YEAR).toString
            ,(cal.get(Calendar.MONTH)+1).toString
            ,cal.get(Calendar.DAY_OF_MONTH).toString
          )
    }else (null,null,null)

  }

}
