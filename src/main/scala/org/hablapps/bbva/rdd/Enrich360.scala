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

import scala.util.Try

case class Transforms[P[_]: Applicative]() extends bbva.Transforms[RDD, P]{

  type SharedVariable[T] = Broadcast[T]

  def Enrich360(rawP: RDD[RetRet], bcForeignExchange: SharedVariable[Map[String, String]]): P[RDD[Ret360Enriched]] = {
    val rddInicioOTC: RDD[Row] = rawP.mapPartitions { retretList =>
      retretList.collect {
        /** Comienza el parseo del evento Submit */
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.Submit => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3

          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3") //.replaceAll("J5cKOATJYT5RYVK<KAhaH9", "J5cKOATJYT5RYVKKAhaH9")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val xmlTagCrossDeal = xml_log \ MifidParseoRETConstants.Tagfxcrossdeal
          val xmlTagLeg = xmlTagCrossDeal \ MifidParseoRETConstants.Tagleg
          /** Cálculo de las patas encontradas en la línea
            * 1º Se realiza el conteo del número de patas de la línea
            * 2º En la variable leg se concatenan los números de pata de la línea
            * */
          val contador_pre = xmlTagLeg.map { l => l \ "@num" }.length
          val cod_leg = xmlTagLeg.map { l => l \ "@num" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_producto = (xmlTagCrossDeal \ "@type").toString
          val cod_gidid = retret.gidid
          val dealt1 = (xmlTagLeg \ MifidParseoRETConstants.Tagrequirement \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val dealt2 = (xmlTagLeg \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_dealtccy = if (StringUtils.isNotBlank(dealt1)) {
            dealt1
          } else {
            dealt2
          }
          val cod_ccy1 = (xmlTagCrossDeal \ MifidParseoRETConstants.Tagccy1).text
          val cod_ccy2 = (xmlTagCrossDeal \ MifidParseoRETConstants.Tagccy2).text
          val cod_valuedate1 = if (cod_producto != MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.Tagbreakdown \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
              \ MifidParseoRETConstants.Tagvaluedate).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          } else null
          val cod_valuedate2 = (xmlTagLeg \ MifidParseoRETConstants.Tagvaluedate).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_valuedate = if (StringUtils.isNotBlank(cod_valuedate1)) {
            cod_valuedate1
          } else {
            cod_valuedate2
          }
          val cod_takerversion = (xml_log \ MifidParseoRETConstants.Tagtakerversion).text
          val cod_servertime = obtenerServerTime360T(retret.des_log)
          val cod_takername = (xml_log \ MifidParseoRETConstants.TagtakerName).text
          val cod_takeraccount = (xmlTagLeg \ MifidParseoRETConstants.Tagrequirement \ MifidParseoRETConstants.TagaccountId).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_dealtamount = (xmlTagLeg \ MifidParseoRETConstants.Tagdealtamount).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_takerid = (xml_log \ MifidParseoRETConstants.TagtakerId).text
          val cod_takergroupid = (xml_log \ MifidParseoRETConstants.TagTakerGroupId).text
          val cod_takergroupfullname = (xml_log \ MifidParseoRETConstants.TagTakerGroupFullName).text
          val cod_proxyname = (xml_log \ MifidParseoRETConstants.TagProxyName).text
          val cod_takergroupname = (xml_log \ MifidParseoRETConstants.TagTakerGroupName).text
          val cod_side = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_proxygroupid = null
          val cod_proxygroupname = null

          /** Se realiza el filtrado de inclusión/exclusión para el evento submit */
          val cod_flujo = obtenerFiltroSubmit(obtenerFlujo(cod_producto, contador_pre), cod_takergroupid, cod_takergroupfullname, cod_takergroupname, cod_takername, cod_proxyname, cod_proxygroupid, cod_proxygroupname, cod_producto, "360")

          Row(
            source_name,
            retret.cod_tipoLinea,
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
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.Price => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3

          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3") //.replaceAll("J5cKOATJYT5RYVK<KAhaH9", "J5cKOATJYT5RYVKKAhaH9")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val xmlTagCrossDeal = xml_log \ MifidParseoRETConstants.Tagfxcrossdeal
          val xmlTagLeg = xmlTagCrossDeal \ MifidParseoRETConstants.Tagleg
          /** Cálculo de las patas encontradas en la línea
            * 1º Se realiza el conteo del número de patas de la línea
            * 2º En la variable leg se concatenan los números de pata de la línea
            * */
          val contador_pre = xmlTagLeg.map { l => l \ "@num" }.length
          val cod_leg = xmlTagLeg.map { l => l \ "@num" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")

          val cod_producto = (xmlTagCrossDeal \ "@type").toString
          val cod_flujo = obtenerFlujo(cod_producto, contador_pre)
          val cod_gidid = retret.gidid
          val dealt1 = (xmlTagLeg \ MifidParseoRETConstants.Tagrequirement \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val dealt2 = (xmlTagLeg \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_dealtccy = if (StringUtils.isNotBlank(dealt1)) {
            dealt1
          } else {
            dealt2
          }
          val cod_ccy1 = (xmlTagCrossDeal \ MifidParseoRETConstants.Tagccy1).text
          val cod_ccy2 = (xmlTagCrossDeal \ MifidParseoRETConstants.Tagccy2).text
          val cod_valuedate = (xmlTagLeg \ MifidParseoRETConstants.Tagvaluedate).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_takerversion = (xml_log \ MifidParseoRETConstants.Tagtakerversion).text
          val cod_servertime = obtenerServerTime360T(des_log)
          val cod_takeraccount = (xml_log \ MifidParseoRETConstants.TagaccountId).text
          val cod_dealtamount = (xmlTagLeg \ MifidParseoRETConstants.Tagdealtamount).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")


          val cod_spotdate = (xmlTagLeg \ MifidParseoRETConstants.Tagvaluedate).map { l => l \ "@spot_date" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_buy = if (cod_producto == MifidParseoRETConstants.TagFORWARD) {
            (xmlTagLeg \ MifidParseoRETConstants.Tagbuy).text
          } else if (cod_flujo == MifidParseoRETConstants.TagSWAP) {
            ((xmlTagLeg filter {
              _ \\ "@type" exists (_.text == MifidParseoRETConstants.TagSWAPFAR)
            }) \ MifidParseoRETConstants.Tagbuy).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          } else {
            (xmlTagLeg \ MifidParseoRETConstants.Tagbuy).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")

          }
          val cod_makerversion = (xml_log \ MifidParseoRETConstants.TagMakerVersion).text

          val cod_nettype = (xmlTagCrossDeal \ MifidParseoRETConstants.TagNetType).text

          val cod_side = xmlTagLeg.map { leg =>
            (leg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString(";")
          }.mkString("|")

          val cod_submitid = (xml_log \ MifidParseoRETConstants.Tagsubmitid).text

          val cod_contraamount = (xml_log \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcontraamount).map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")

          Row(
            source_name,
            retret.cod_tipoLinea,
            cod_flujo,
            null,
            created_date,
            anio,
            mes,
            dia,
            null,
            null,
            cod_makerversion,
            null,
            null,
            null,
            cod_nettype,
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
            null,
            null,
            null,
            cod_spotdate,
            cod_buy,
            cod_side,
            cod_submitid,
            cod_contraamount,
            last_version
          )

          /** Comienza el parseo del evento reject */
        }
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.Reject => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3

          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val cod_flujo = MifidParseoRETConstants.FxCrossDealType.findFirstIn(des_log2)
          match {
            case Some(_) => ""
            case _ => null
          }
          val cod_leg = "0"
          val cod_gidid = retret.gidid
          val cod_servertime = obtenerServerTime360T(des_log)
          val cod_side = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_submitid = null

          Row(
            source_name,
            retret.cod_tipoLinea,
            cod_flujo,
            null,
            created_date,
            anio,
            mes,
            dia,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            source_offset,
            cod_leg,
            cod_gidid,
            null,
            null,
            null,
            null,
            null,
            cod_servertime,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            cod_side,
            cod_submitid,
            null,
            last_version
          )

          /** Comienza el parseo del evento confirm */
        }
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.Confirm => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3
          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val cod_flujo = ""
          val cod_leg = "0"
          val cod_gidid = retret.gidid
          val cod_servertime = obtenerServerTime360T(des_log)
          val cod_status = (xml_log \ "@state").toString

          val cod_side = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_submitid = null

          Row(
            source_name,
            retret.cod_tipoLinea,
            cod_flujo,
            cod_status,
            created_date,
            anio,
            mes,
            dia,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            source_offset,
            cod_leg,
            cod_gidid,
            null,
            null,
            null,
            null,
            null,
            cod_servertime,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            cod_side,
            cod_submitid,
            null,
            last_version
          )

          /** Comienza el parseo del evento close */
        }
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.Close => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3
          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val cod_flujo = ""
          val cod_leg = "0"
          val cod_gidid = retret.gidid
          val cod_servertime = obtenerServerTime360T(des_log)
          val cod_status = (xml_log \ "@state").toString

          val cod_side = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val cod_submitid = null

          Row(
            source_name,
            retret.cod_tipoLinea,
            cod_flujo,
            cod_status,
            created_date,
            anio,
            mes,
            dia,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            source_offset,
            cod_leg,
            cod_gidid,
            null,
            null,
            null,
            null,
            null,
            cod_servertime,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            cod_side,
            cod_submitid,
            null,
            last_version
          )
        }
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.Accept => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3
          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val cod_producto = null
          val cod_flujo = ""
          val cod_leg = "0"
          val cod_gidid = retret.gidid
          val cod_dealtccy = null
          val cod_ccy1 = null
          val cod_ccy2 = null
          val cod_valuedate = null
          val cod_takerversion = null
          val cod_servertime = obtenerServerTime360T(des_log)
          val cod_takeraccount = null
          val cod_dealtamount = null
          val cod_takerid = null
          val cod_allin = null
          val cod_coremarket = null
          val cod_spotdate = null
          val cod_buy = if (des_log.toUpperCase.contains("BUY")) {
            "1"
          } else null
          val cod_takername = null
          val cod_sell = if (des_log.toUpperCase.contains("SELL")) {
            "1"
          } else null
          val cod_makerversion = null
          val cod_status = null
          val cod_takergroupid = null
          val cod_takergroupfullname = null
          val cod_proxyname = null
          val cod_nettype = null
          val cod_side = if (des_log.toUpperCase.contains("BUY")) {
            "BUY"
          } else if (des_log.toUpperCase.contains("SELL")) {
            "SELL"
          } else null
          val cod_submitid = null

          Row(
            source_name,
            retret.cod_tipoLinea,
            cod_flujo,
            cod_status,
            created_date,
            anio,
            mes,
            dia,
            cod_takername,
            cod_sell,
            cod_makerversion,
            cod_takergroupid,
            cod_takergroupfullname,
            cod_proxyname,
            cod_nettype,
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
            cod_allin,
            cod_coremarket,
            cod_spotdate,
            cod_buy,
            cod_side,
            cod_submitid,
            null,
            last_version
          )
        }
        case retret if retret.cod_tipoLinea == MifidParseoRETConstants.LbnPse => {
          val des_log = retret.des_log

          val last_version = retret.last_version

          val source_name = retret.source_name
          val source_offset = retret.source_offset
          val created_date = retret.created_date

          /** Se calcula la fecha que se usara en la particion que sera la contenida en el nombre del fichero */
          val fecha = Fechas.obtenerTuplaFecha(source_name, "\\d{8}".r, "yyyyMMdd")
          val anio = fecha._1
          val mes = fecha._2
          val dia = fecha._3
          /** Se carga el log como un XML */
          val des_log2: String = if (MifidParseoRETConstants.NotFound != des_log.indexOf("<")) {
            des_log.substring(des_log.indexOf("<")).replaceAll("#parameters#", "parameters").replaceAll("""(="[^"]*)(<)([^"]*")""", "$1&lt;$3").replaceAll("""(="[^"]*)(>)([^"]*")""", "$1&gt;$3")
          } else ""
          val xml_log = xml.XML.loadString(des_log2)

          val cod_producto = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ "@type").toString

          /** Cálculo de las patas encontradas en la línea
            * 1º Se realiza el conteo del número de patas de la línea
            * 2º En la variable leg se concatenan los números de pata de la línea
            * */
          val contador = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg).map { l => l \ "@num" }.length
          val cod_leg = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg).map { l => l \ "@num" }.map(_.text).map(l => if (l == "" || l == null) {
            "null"
          } else l).mkString("|")
          val contador1 = contador

          val cod_flujo = obtenerFlujo(cod_producto, contador1)
          val cod_gidid = retret.gidid
          val cod_dealtccy = if (cod_flujo == MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtccy).text
          } else {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtccy).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          }
          val cod_ccy1 = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagccy1).text
          val cod_ccy2 = (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagccy2).text
          val cod_dealtamount = if (cod_flujo == MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtamount).text
          } else if (cod_flujo == MifidParseoRETConstants.TagSWAP) {
            ((xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg filter {
              _ \\ "@type" exists (_.text == MifidParseoRETConstants.TagSWAPFAR)
            }) \ MifidParseoRETConstants.Tagdealtamount).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          } else {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagdealtamount).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          }

          val cod_takerid = null
          val cod_allin = if (cod_flujo == MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquote \ MifidParseoRETConstants.Tagallin).text
          } else if (cod_flujo == MifidParseoRETConstants.TagSWAP) {
            val xmlTagSWAP = xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
            xmlTagSWAP.map { leg =>
              (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquote \ MifidParseoRETConstants.Tagallin).map(_.text).map(l => if (l == "" || l == null) {
                "null"
              } else l).mkString(";")
            }.mkString("|")
          } else {
            val xmlTag = xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
            xmlTag.map { leg =>
              (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquote \ MifidParseoRETConstants.Tagallin).map(_.text).map(l => if (l == "" || l == null) {
                "null"
              } else l).mkString(";")
            }.mkString("|")
          }

          val cod_coremarket = if (cod_flujo == MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquotebreak \ MifidParseoRETConstants.Tagcoremarket).text
          } else if (cod_flujo == MifidParseoRETConstants.TagSWAP) {
            val xmlTagSWAP = xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
            xmlTagSWAP.map { leg =>
              (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquotebreak \ MifidParseoRETConstants.Tagcoremarket).map(_.text).map(l => if (l == "" || l == null) {
                "null"
              } else l).mkString(";")
            }.mkString("|")
          } else {
            val xmlTag = xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
            xmlTag.map { leg =>
              (leg \ MifidParseoRETConstants.Tagcrosscomponent \ MifidParseoRETConstants.Tagquotebreak \ MifidParseoRETConstants.Tagcoremarket).map(_.text).map(l => if (l == "" || l == null) {
                "null"
              } else l).mkString(";")
            }.mkString("|")
          }

          val cod_side = if (cod_flujo == MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          } else if (cod_flujo == MifidParseoRETConstants.TagSWAP) {
            val tagSWAP = xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
            tagSWAP.map { leg =>
              (leg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
                "null"
              } else l).mkString(";")
            }.mkString("|")
          } else {
            val tag = xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg
            tag.map { leg =>
              (leg \ MifidParseoRETConstants.Tagcrosscomponent).map { l => l \ "@side" }.map(_.text).map(l => if (l == "" || l == null) {
                "null"
              } else l).mkString(";")
            }.mkString("|")
          }

          val cod_submitid = null

          val cod_contraamount = if (cod_flujo == MifidParseoRETConstants.TagFORWARD) {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcontraamount).text
          } else if (cod_flujo == MifidParseoRETConstants.TagSWAP) {
            ((xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg filter {
              _ \\ "@type" exists (_.text == MifidParseoRETConstants.TagSWAPFAR)
            }) \ MifidParseoRETConstants.Tagcontraamount).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          } else {
            (xml_log \ MifidParseoRETConstants.TagData \ MifidParseoRETConstants.TagDealNotification \ MifidParseoRETConstants.TagDeal \ MifidParseoRETConstants.Tagfxcrossdeal \ MifidParseoRETConstants.Tagleg \ MifidParseoRETConstants.Tagcontraamount).map(_.text).map(l => if (l == "" || l == null) {
              "null"
            } else l).mkString("|")
          }

          Row(
            source_name,
            retret.cod_tipoLinea,
            cod_flujo,
            null,
            created_date,
            anio,
            mes,
            dia,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            source_offset,
            cod_leg,
            cod_gidid,
            cod_dealtccy,
            cod_ccy1,
            cod_ccy2,
            null,
            null,
            null,
            null,
            cod_dealtamount,
            cod_takerid,
            cod_allin,
            cod_coremarket,
            null,
            null,
            cod_side,
            cod_submitid,
            cod_contraamount,
            last_version
          )
        }
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

  def getOrNull[T](row: Row, pos: Int): T = {
    if (null == row.get(pos)) {
      null.asInstanceOf[T]
    }else row.getAs[T](pos)
  }

}

object EnrichPopulations{

  def apply[P[_]: Applicative]: TransformB2[RDD, P, RetRet, Broadcast[Map[String, String]],Ret360Enriched] =
    Kleisli{
      case (rawP, cityAbbrev) =>
        Transforms[P].Enrich360(rawP, cityAbbrev)
    }
}
