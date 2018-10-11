package org.hablapps
package bbva
package rdd

import cats.Applicative
import cats.data.Kleisli
import cats.syntax.applicative._
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.hablapps.bbva.ParseoRet._
import org.hablapps.etl._

import scala.reflect.ClassTag
import scala.util.Try
import scala.xml.Elem

case class Transforms[P[_]: Applicative] extends bbva.Transforms[RDD, P, Broadcast[Map[String, String]]]{
  def Enrich360(rawP: RDD[RetRet], bcForeignExchange: Broadcast[Map[String, String]]): P[RDD[Ret360Enriched]] = ???

  def Enrich360[T: ClassTag](rawP: RDD[T], bcForeignExchange: Broadcast[Map[String, String]])(implicit extracted: RetExtracted[T]): P[RDD[Ret360Enriched]] = {
    val rddInicioOTC: RDD[Row] = rawP.mapPartitions { retretList =>
      retretList.collect {
        /** Comienza el parseo del evento Submit */
        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.Submit => parsingSubmitEvents(retret)
//        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.Price => parsingPriceEvents(retret)
//        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.Reject => parsingRejectEvents(retret)
//        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.Confirm => parsingConfirmEvents(retret)
//        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.Close => parsingCloseEvents(retret)
//        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.Accept => parsingAcceptEvents(retret)
//        case retret if extracted.cod_tipoLinea(retret) == MifidParseoRETConstants.LbnPse => parsingLbnPseEvents(retret)
      }
    }

    val rddFinal: RDD[Ret360Enriched] = rddInicioOTC.mapPartitions { rows =>

      val fxMap: Map[String, String] = bcForeignExchange.value

      rows.flatMap {
        case row if row.size == 35 => {
          val source_name: String = getOrNull(row, 0)
          val cod_tipoLinea: String = getOrNull(row, 1)
          val cod_flujo: String = getOrNull(row, 2)
          val cod_status: String = getOrNull(row, 3)
          val created_date: String = getOrNull(row, 4)
          val year: String = getOrNull(row, 5)
          val month: String = getOrNull(row, 6)
          val day: String = getOrNull(row, 7)
          val cod_takername: String = getOrNull(row, 8)
          val cod_sell: String = getOrNull(row, 9)
          val cod_makerversion: String = getOrNull(row, 10)
          val cod_takergroupid: String = getOrNull(row, 11)
          val cod_takergroupfullname: String = getOrNull(row, 12)
          val cod_proxyname: String = getOrNull(row, 13)
          val cod_nettyp: String = getOrNull(row, 14)
          val source_offset: String = getOrNull(row, 15)
          val cod_leg: String = getOrNull(row, 16)
          val cod_gidid: String = getOrNull(row, 17)
          val cod_dealtccy: String = getOrNull(row, 18)
          val cod_ccy1: String = getOrNull(row, 19)
          val cod_ccy2: String = getOrNull(row, 20)
          val cod_valuedate: String =  getOrNull[String](row, 21)
          val cod_takerversion: String = getOrNull(row, 22)
          val cod_servertime: String = getOrNull(row, 23)
          val cod_takeraccount: String = getOrNull(row, 24)
          val cod_dealtamount: String = getOrNull(row, 25)
          val cod_takerid: String = getOrNull(row, 26)
          val cod_allin: String = getOrNull(row, 27)
          val cod_coremarket: String = getOrNull(row, 28)
          val cod_spotdate: String = getOrNull(row, 29)
          val cod_buy: String = getOrNull(row, 30)
          val cod_side: String = getOrNull(row, 31)
          val cod_submitid: String = getOrNull(row, 32)
          val cod_contraamount: String = getOrNull(row, 33)

          /**
            * Se va a realizar una multiplicación de líneas a partir del campo cod_leg (debe ser string)
            */
          val words: Array[String] = cod_leg.split("\\|")
          words.map { word =>

            val dealtCCYUpdated: String = {
              val dealtCcyClean = obtenerCampoPata(cod_dealtccy, word, cod_flujo)
              Option(dealtCcyClean).flatMap(fxMap.get(_)).getOrElse(dealtCcyClean)
            }

            new Ret360Enriched(
              source_name,
              source_offset,
              cod_tipoLinea,
              cod_flujo,
              word,
              cod_gidid,
              cod_submitid,
              dealtCCYUpdated,
              Option(cod_ccy1).flatMap(fxMap.get(_)).getOrElse(cod_ccy1),
              Option(cod_ccy2).flatMap(fxMap.get(_)).getOrElse(cod_ccy2),
              obtenerCampoPata(cod_valuedate, word, cod_flujo),
              cod_takerversion,
              cod_servertime,
              if (cod_tipoLinea != MifidParseoRETConstants.Price) {
                obtenerCampoPata(cod_takeraccount, word, cod_flujo)
              } else cod_takeraccount,
              //Campo cod_dealtamount
              if (cod_tipoLinea != MifidParseoRETConstants.LbnPse) {
                obtenerCampoPata(cod_dealtamount, word, cod_flujo)
              } else cod_dealtamount,
              cod_takerid,
              //Campo cod_allin
              obtenerCampoPata(cod_allin, word, cod_flujo),
              //Campo cod_coremarket
              obtenerCampoPata(cod_coremarket, word, cod_flujo),
              //Campo cod_spotdate
              obtenerCampoPata(cod_spotdate, word, cod_flujo),
              //Campo cod_buy
              obtenerCampoPataLarga(cod_buy, word),
              //Campo cod_takername
              cod_takername,
              //Campo cod_sell
              obtenerCampoPataContenedor(cod_sell, word, cod_flujo),
              //Campo cod_makerversion
              cod_makerversion,
              //Campo cod_status
              cod_status,
              //Campo cod_takergroupid
              cod_takergroupid,
              //Campo cod_takergroupfullname
              cod_takergroupfullname,
              //Campo cod_proxyname
              cod_proxyname,
              //Campo cod_nettype
              cod_nettyp,
              //Campo cod_side
              if (cod_tipoLinea == MifidParseoRETConstants.LbnPse || cod_tipoLinea == MifidParseoRETConstants.Price) {
                obtenerCampoPata(cod_side, word, cod_flujo)
              } else cod_side,
              //Campo cod_contraamount
              if (cod_tipoLinea != MifidParseoRETConstants.LbnPse) {
                obtenerCampoPata(cod_contraamount, word, cod_flujo)
              } else cod_contraamount,
              //Campo created_date
              created_date,
              //Campo year
              Try(year.toInt).getOrElse[Int](-1),
              //Campo month
              Try(month.toInt).getOrElse[Int](-1),
              //Campo day
              Try(day.toInt).getOrElse[Int](-1),
              //Campo last_version
              1
            )
          }
        }
        case _ => Seq.empty[Ret360Enriched]
      }

    }
    rddFinal.filter(_.cod_tipolinea != null).pure[P]

  }

  trait RetExtracted[T] extends Serializable{
    def server_time(sourceType: T): String
    def source_offset(sourceType: T): Long
    def source_name(sourceType: T): String
    def created_date(sourceType: T): String
    def year(sourceType: T): String
    def month(sourceType: T): String
    def day(sourceType: T): String
    def last_version(sourceType: T): Short
    def tipo(sourceType: T): String
    def gidid(sourceType: T): String
    def takername(sourceType: T): String
    def cod_takername(sourceType: T): String
    def proxyname(sourceType: T): String
    def cod_producto(sourceType: T): String
    def cod_tipoLinea(sourceType: T): String
    def cod_leg(sourceType: T): String
    def numLegs(sourceType: T): Int
    def cod_dealtCcy(sourceType: T): String
    def cod_dealtCcy1(sourceType: T): String
    def cod_dealtCcy2(sourceType: T): String
    def cod_dealtCCyForward(sourceType: T): String
    def cod_ccy1(sourceType: T): String
    def cod_ccy2(sourceType: T): String
    def cod_dealtamount(sourceType: T): String
    def cod_dealtamountForward(sourceType: T): String
    def cod_dealtamountSwap(sourceType: T): String
    def cod_allin(sourceType: T): String
    def cod_allinForward(sourceType: T): String
    def cod_allinSwap(sourceType: T): String
    def cod_coremarket(sourceType: T): String
    def cod_coremarketforward(sourceType: T): String
    def cod_coremarketswap(sourceType: T): String
    def valueDate1(sourceType: T): String
    def valueDate2(sourceType: T): String
    def cod_takerversion(sourceType: T): String
    def cod_takeraccount(sourceType: T): String
    def cod_takerid(sourceType: T): String
    def cod_takergroupid(sourceType: T): String
    def cod_takergroupfullname(sourceType: T): String
    def cod_proxyname(sourceType: T): String
    def cod_takergroupname(sourceType: T): String
    def cod_side(sourceType: T): String
  }

  case class XmlRet(
    xml: Elem,
    cod_tipoLinea: String,
    tipo: String,
    gidid: String,
    takername: String,
    proxyname: String,
    source_name: String,
    source_offset: Long,
    created_date: String,
    year: String,
    month: String,
    day: String,
    last_version: Short,
    server_time: String
  )

  object XMLRetExtracted extends RetExtracted[XmlRet]{

    def cod_producto(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ "@type").toString

    def numLegs(sourceType: XmlRet): Int = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg).map { l => l \ "@num" }.length
    def cod_leg(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg).map { l => l \ "@num" }.map(_.text).map(l => if (l == "" || l == null) {
      "null"
    } else l).mkString("|")

    def cod_dealtCCyForward(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtccy).text
    def cod_dealtCcy(sourceType: XmlRet): String =
      (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
        "null"} else l).mkString("|")
    def cod_dealtCcy1(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.Tagrequirement \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
      "null"
    } else l).mkString("|")

    def cod_dealtCcy2(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
      "null"
    } else l).mkString("|")

    def cod_ccy1(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagccy1).text
    def cod_ccy2(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagccy2).text
    def cod_dealtamountForward(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtamount).text
    def cod_dealtamountSwap(sourceType: XmlRet): String = ((sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg filter {
        _ \\ "@type" exists (_.text == MifidParseoRETConstants.TagSWAPFAR)
      }) \ MifidParseoRETConstants.Tagdealtamount).map(_.text).map(l => if (l == "" || l == null) {
        "null"
      } else l).mkString("|")
    def cod_dealtamount(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtamount).map(_.text).map(l => if (l == "" || l == null) {
        "null"
      } else l).mkString("|")


    def cod_allinForward(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquote \ MifidParseoRETConstants.Tagallin).text
    def cod_allinSwap(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg)
      .map { leg =>
        (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquote \ MifidParseoRETConstants.Tagallin).map(_.text).map(l => if (l == "" || l == null) {
          "null"
        } else l).mkString(";")
      }.mkString("|")

    def cod_allin(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg)
      .map { leg =>
        (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquote \ MifidParseoRETConstants.Tagallin).map(_.text).map(l => if (l == "" || l == null) {
          "null"
        } else l).mkString(";")
      }.mkString("|")


    def cod_coremarketforward(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquotebreak \ MifidParseoRETConstants.Tagcoremarket).text
    def cod_coremarketswap(sourceType: XmlRet): String =  (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg)
      .map { leg =>
        (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquotebreak \ MifidParseoRETConstants.Tagcoremarket).map(_.text).map(l => if (l == "" || l == null) {
          "null"
        } else l).mkString(";")
      }.mkString("|")

    def cod_coremarket(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg)
      .map { leg =>
        (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquotebreak \ MifidParseoRETConstants.Tagcoremarket).map(_.text).map(l => if (l == "" || l == null) {
          "null"
        } else l).mkString(";")
      }.mkString("|")


    def cod_sideForward(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
        "null"
      } else l).mkString("|")

    def cod_sideSwap(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg)
      .map { leg =>
        (leg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
          "null"
        } else l).mkString(";")
      }.mkString("|")

    def cod_side(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg)
      .map { leg =>
        (leg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
          "null"
        } else l).mkString(";")
      }.mkString("|")


    def cod_submitid(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.Tagsubmitid).text

    def cod_contraamountForward(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcontraamount).text
    def cod_contraamountSwap(sourceType: XmlRet): String = ((sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg filter {
        _ \\ "@type" exists (_.text == MifidParseoRETConstants.TagSWAPFAR)
      }) \ MifidParseoRETConstants.Tagcontraamount).map(_.text).map(l => if (l == "" || l == null) {
      "null"
    } else l).mkString("|")
    def cod_contraamount(sourceType: XmlRet): String = (sourceType.xml\ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcontraamount).map(_.text).map(l => if (l == "" || l == null) {
      "null"
    } else l).mkString("|")

    override def valueDate1(sourceType: XmlRet): String = if (cod_producto(sourceType) != MifidParseoRETConstants.TagFORWARD) {
      (sourceType.xml \ MifidParseoRETConstants.Tagbreakdown \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
        \ MifidParseoRETConstants.Tagvaluedate).map(_.text).map(l => if (l == "" || l == null) {
        "null"
      } else l).mkString("|")
    } else null

    override def valueDate2(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.Tagvaluedate).map(_.text).map(l => if (l == "" || l == null) {
        "null"
      } else l).mkString("|")

    def cod_tipoLinea(sourceType: XmlRet): String = sourceType.cod_tipoLinea

    override def source_offset(sourceType: XmlRet): Long = sourceType.source_offset

    override def source_name(sourceType: XmlRet): String = sourceType.source_name

    override def created_date(sourceType: XmlRet): String = sourceType.created_date

    override def year(sourceType: XmlRet): String = sourceType.year

    override def month(sourceType: XmlRet): String = sourceType.month

    override def day(sourceType: XmlRet): String = sourceType.day

    override def last_version(sourceType: XmlRet): Short = sourceType.last_version

    override def tipo(sourceType: XmlRet): String = sourceType.tipo

    override def gidid(sourceType: XmlRet): String = sourceType.gidid

    override def takername(sourceType: XmlRet): String = sourceType.takername

    override def proxyname(sourceType: XmlRet): String = sourceType.proxyname

    override def cod_takerversion(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.Tagtakerversion).text

    override def cod_takername(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.TagtakerName).text

    override def cod_takeraccount(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.Tagrequirement \ MifidParseoRETConstants.TagaccountId).map(_.text).map(l => if (l == "" || l == null) {
      "null"
    } else l).mkString("|")

    override def cod_takerid(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.TagtakerId).text
    override def cod_takergroupid(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.TagTakerGroupId).text
    override def cod_takergroupfullname(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.TagTakerGroupFullName).text

    override def cod_proxyname(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.TagProxyName).text
    override def cod_takergroupname(sourceType: XmlRet): String = (sourceType.xml \ MifidParseoRETConstants.TagTakerGroupName).text

    override def server_time(sourceType: XmlRet): String = sourceType.server_time
  }


  private def parsingSubmitEvents[T](retret: T)(implicit extracted: RetExtracted[T]) = {

    val last_version = extracted.last_version(retret)

    val source_name = extracted.source_name(retret)
    val source_offset = extracted.source_offset(retret)
    val created_date = extracted.created_date(retret)

    /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
    val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
    val anio = fecha._1
    val mes = fecha._2
    val dia = fecha._3


    /** Cálculo de las patas encontradas en la línea
      * 1º Se realiza el conteo del número de patas de la línea
      * 2º En la variable leg se concatenan los números de pata de la línea
      * */
    val cod_leg = extracted.cod_leg(retret)
    val cod_producto = extracted.cod_producto(retret)
    val cod_gidid = extracted.gidid(retret)
    val dealt1 = extracted.cod_dealtCcy1(retret)
    val dealt2 = extracted.cod_dealtCcy2(retret)
    val cod_dealtccy = if (StringUtils.isNotBlank(dealt1))dealt1 else dealt2

    val cod_ccy1 = extracted.cod_ccy1(retret)
    val cod_ccy2 = extracted.cod_ccy2(retret)
    val cod_valuedate1 = extracted.valueDate1(retret)
    val cod_valuedate2 = extracted.valueDate2(retret)
    val cod_valuedate = if (StringUtils.isNotBlank(cod_valuedate1)) cod_valuedate1 else cod_valuedate2

    val cod_takerversion = extracted.cod_takerversion(retret)
    val cod_servertime = extracted.server_time(retret)//obtenerServerTime360T(retret.des_log)
    val cod_takername = extracted.cod_takername(retret)
    val cod_takeraccount = extracted.cod_takeraccount(retret)
    val cod_dealtamount = extracted.cod_dealtamount(retret)
    val cod_takerid = extracted.cod_takerid(retret)
    val cod_takergroupid = extracted.cod_takergroupid(retret)
    val cod_takergroupfullname = extracted.cod_takergroupfullname(retret)
    val cod_proxyname = extracted.cod_proxyname(retret)
    val cod_takergroupname = extracted.cod_takergroupname(retret)
    val cod_side = extracted.cod_side(retret)
    val cod_proxygroupid = null
    val cod_proxygroupname = null

    /** Se realiza el filtrado de inclusión/exclusión para el evento submit */
    val cod_flujo = obtenerFiltroSubmit(obtenerFlujo(cod_producto, extracted.numLegs(retret)), cod_takergroupid, cod_takergroupfullname, cod_takergroupname, cod_takername, cod_proxyname, cod_proxygroupid, cod_proxygroupname, cod_producto, "360")

    Row(
      source_name,
      extracted.cod_tipoLinea(retret),
      cod_flujo,
      null,
      created_date,
      anio,
      mes,
      dia,
      cod_takername,
      null,
      null,
      cod_takergroupid,
      cod_takergroupfullname,
      cod_proxyname,
      null,
      source_offset,
      cod_leg,
      cod_gidid,
      cod_dealtccy,
      cod_ccy1,
      cod_ccy2,
      cod_valuedate,
      cod_takerversion,
      cod_servertime,
      cod_takeraccount,
      cod_dealtamount,
      cod_takerid,
      null,
      null,
      null,
      null,
      cod_side,
      null,
      null,
      last_version
    )
  }

  def getOrNull[T](row: Row, pos: Int): T = {
    if (null == row.get(pos)) {
      null.asInstanceOf[T]
    }else row.getAs[T](pos)
  }

}

object Enrich360{

//  def apply[P[_]: Applicative]: TransformB2[RDD, P, RetRet, Broadcast[Map[String, String]], Ret360Enriched] =
//    Kleisli{
//      case (rawP, forExMap) =>
//        Transforms[P].Enrich360(rawP, forExMap)
//    }
}
