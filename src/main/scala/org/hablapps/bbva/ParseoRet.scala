package org
package hablapps
package bbva

import org.apache.commons.lang.StringUtils

import scala.util.Try

object ParseoRet {

  def obtenerCampoPata(field: String, word: String, cod_flujo: String): String = {
    if (StringUtils.isNotBlank(field) && StringUtils.isNotBlank(word) && field != "null" && StringUtils.isNotBlank(cod_flujo)) {
      val separado = field.split("\\|")
      val intT: Try[Int] = Try(word.toInt)

      val result = for {
        int <- intT.toOption
        s <- separado.lift(int) if s != "null"
      } yield s
      result.orNull
    } else null
  }

  def obtenerCampoPataContenedor(field: String, word: String, cod_flujo: String): String = {
    if (StringUtils.isNotBlank(field) && StringUtils.isNotBlank(word) && StringUtils.isNotBlank(cod_flujo)) {
      val separadofinal = field.split ("\\|")
      if (separadofinal.lift(word.toInt).exists(_ != "null")) {
        separadofinal(word.toInt)
      } else null
    } else null
  }


  def obtenerCampoPataLarga(field: String, word: String): String = {
    if (StringUtils.isNotBlank(field) && StringUtils.isNotBlank(word) && field != "null") {
      val separado = field.toString.split ("\\|")
      if (separado (word.toInt/2) != "null") {
        separado (word.toInt/2)
      } else null
    } else null
  }

  def obtenerServerTime360T(des_log: String): String = {
    if (des_log!=null) {
      des_log
        .replace("/", "")
        .replace(" ", "")
        .substring(0, 4)+"-"+des_log.substring(5, 7)+"-"+des_log.substring(8, 10)+"T"+des_log.substring(11, 23)
    } else null
  }

  def obtenerFlujo(cod_producto: String, contador: Int): String = {
    if (contador == 1 && (cod_producto == MifidParseoRETConstants.TagFORWARD || cod_producto == MifidParseoRETConstants.TagNDF)) {
      MifidParseoRETConstants.TagFORWARD
    } else if (contador <= 2 && cod_producto == MifidParseoRETConstants.TagSWAP) {
      MifidParseoRETConstants.TagSWAP
    } else if ((cod_producto == MifidParseoRETConstants.TagBLOCK) || (contador > 2 && cod_producto == MifidParseoRETConstants.TagSWAP)) {
      MifidParseoRETConstants.TagEstruct
    } else null
  }

  def obtenerTipoLinea(des_log: String): String = {
    if (StringUtils.isNotBlank(des_log)) {
      /** Tags para identificar el evento Submit */
      val valorSubmit1 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent1)
      val valorSubmit2 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent2)
      /** Tags para identificar el evento Price */
      val valorPrice1 = MifidParseoRETConstants.PriceEvent1.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      val valorPrice2 = MifidParseoRETConstants.PriceEvent2.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento Reject */
      val valorReject1 = MifidParseoRETConstants.RejectEvent.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      val valorRejectClose = MifidParseoRETConstants.CloseEvent.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      val valorReject2 =
        if (valorRejectClose != MifidParseoRETConstants.NotFound) {
          val StateCloseReject = des_log.indexOf(MifidParseoRETConstants.StateCloseReject)
          if (StateCloseReject != MifidParseoRETConstants.NotFound){StateCloseReject}else -1
        }else -1
      /** Tags para identificar el evento Confirm */
      val valorConfirm = MifidParseoRETConstants.ConfirmEvent.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento Close */
      val valorClose1 = MifidParseoRETConstants.CloseEvent.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      val valorClose =
        if (valorClose1 != MifidParseoRETConstants.NotFound) {
          val StateClose = des_log.indexOf(MifidParseoRETConstants.StateClose)
          if (StateClose != MifidParseoRETConstants.NotFound){StateClose}else -1
        }else -1
      /** Tags para identificar el evento Accept */
      val valorAccept = MifidParseoRETConstants.AcceptEvent.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento lbnPSE (forma PR)*/
      val segundoTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent1)
      val valorLbnPSE =
        if (segundoTag != MifidParseoRETConstants.NotFound) {
          /*Comprobamos que a continuación se encuentra la cadena tercera Tag en formato XML
              si se encuentra el tipo de linea es LbnPSE
              */
          val tercerTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent2, segundoTag + 1)
          if (tercerTag != MifidParseoRETConstants.NotFound) {

            if(des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent3, segundoTag + 1) != -1){
              des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent3, segundoTag + 1)

            }else if(des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent4, segundoTag + 1) != -1){
              des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent4, segundoTag + 1)

            }else -1
          } else -1
        } else -1

      /** Se asignan los valores para los tipos de línea */
      if (valorSubmit1 != MifidParseoRETConstants.NotFound || valorSubmit2 != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Submit
      } else  if (valorPrice1 != MifidParseoRETConstants.NotFound ){
        MifidParseoRETConstants.Price
      } else  if (valorPrice2 != MifidParseoRETConstants.NotFound ){
        MifidParseoRETConstants.Price
      } else if (valorReject1 != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Reject
      } else if (valorReject2 != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Reject
      } else if (valorConfirm != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Confirm
      } else if (valorClose != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Close
      } else if (valorAccept != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Accept
      } else if (valorLbnPSE != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.LbnPse
      } else null
    } else null
  }


  def obtenerTipoOperativaRET(des_log: String): String = {
    if (StringUtils.isNotBlank(des_log)) {
      /** Tags para identificar la operativa OTC para 360T*/
      val Tag360T1 = des_log.indexOf(MifidParseoRETConstants.TagOperativa360T1)
      val OTC_360T = if (Tag360T1 != MifidParseoRETConstants.NotFound) {
        val Tag360T3 = des_log.indexOf(MifidParseoRETConstants.TagOperativa360T3, Tag360T1 + 1)
        if (Tag360T3 != MifidParseoRETConstants.NotFound) {
          val Tag360T4 = des_log.indexOf(MifidParseoRETConstants.TagOperativa360T4, Tag360T3 + 1)
          if (Tag360T4 != MifidParseoRETConstants.NotFound) {
            Tag360T4
          } else -1
        } else -1
      } else -1
      /** Tags para identificar la operativa OTC para FxALL*/
      val TagFxALL1 = des_log.indexOf(MifidParseoRETConstants.TagOperativaFxALL1)
      val OTC_FxALL = if (TagFxALL1 != MifidParseoRETConstants.NotFound) {
        val TagFxALL2 = des_log.indexOf(MifidParseoRETConstants.TagOperativaFxALL2, TagFxALL1 + 1)
        if (TagFxALL2 != MifidParseoRETConstants.NotFound) {
          val TagFxALL3 = des_log.indexOf(MifidParseoRETConstants.TagOperativaFxALL3, TagFxALL2 + 1)
          if (TagFxALL3 != MifidParseoRETConstants.NotFound) { TagFxALL3
          } else -1
        } else -1
      } else -1
      /** Tags para identificar la operativa OTC para BBG*/
      val TagBBG1 = des_log.indexOf(MifidParseoRETConstants.TagOperativaBBG1)
      val OTC_BBG = if (TagBBG1 != MifidParseoRETConstants.NotFound) {
        val TagBBG2 = des_log.indexOf(MifidParseoRETConstants.TagOperativaBBG2, TagBBG1 + 1)
        if (TagBBG2 != MifidParseoRETConstants.NotFound) {
          val TagBBG3 = des_log.indexOf(MifidParseoRETConstants.TagOperativaBBG3, TagBBG2 + 1)
          if (TagBBG3 != MifidParseoRETConstants.NotFound) { TagBBG3
          } else -1
        } else -1
      } else -1

      /** Se asignan los valores para los tipos de línea */
      if (OTC_360T != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.OperativaOTC
      } else if (OTC_FxALL != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.OperativaOTC
      } else if (OTC_BBG != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.OperativaOTC
      } else null
    } else null
  }


  def obtenerTipoLinea360(des_log: String): String = {
    if (StringUtils.isNotBlank(des_log)) {
      /** Tags para identificar el evento Submit */
      val valorSubmit1 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent1)
      val valorSubmit2 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent2)
      /** Tags para identificar el evento Price */
      val valorPrice = MifidParseoRETConstants.PriceEvent360.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.PriceEvent3602.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Reject */
      val valorReject = MifidParseoRETConstants.RejectEvent360.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.RejectEvent3602.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Confirm */
      val valorConfirm = MifidParseoRETConstants.ConfirmEvent360.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.ConfirmEvent3602.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Close */
      val valorClose = MifidParseoRETConstants.CloseEvent360.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.CloseEvent3602.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Accept */
      val valorAccept = MifidParseoRETConstants.AcceptEvent360.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento lbnPSE (forma PR)*/
      val segundoTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent1)
      //Comprobamos que a continuación se encuentra la cadena de XML
      val valorLbnPSE = if (segundoTag != MifidParseoRETConstants.NotFound) {
        val tercerTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent2, segundoTag + 1)
        if (tercerTag != MifidParseoRETConstants.NotFound) {
          val cuartoTag1 = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent3, tercerTag + 1)
          val cuartoTag2 = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent4, tercerTag + 1)
          if (cuartoTag1 != MifidParseoRETConstants.NotFound) {
            cuartoTag1
          } else if (cuartoTag2 != MifidParseoRETConstants.NotFound) {
            cuartoTag2
          } else -1
        } else -1
      } else -1

      /** Se asignan los valores para los tipos de línea */
      if (valorSubmit1 != MifidParseoRETConstants.NotFound || valorSubmit2 != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Submit
      } else if (valorPrice != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Price
      } else if (valorReject != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Reject
      } else if (valorConfirm != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Confirm
      } else if (valorClose != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Close
      } else if (valorAccept != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Accept
      } else if (valorLbnPSE != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.LbnPse
      } else null
    } else null
  }


  def obtenerTipoLineaFxALL(des_log: String): String = {
    if (StringUtils.isNotBlank(des_log)) {
      /** Tags para identificar el evento Submit */
      val valorSubmit1 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent1)
      val valorSubmit2 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent2)
      /** Tags para identificar el evento Price */
      val valorPrice = MifidParseoRETConstants.PriceEventFX.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.PriceEventFX2.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Reject */
      val valorReject = MifidParseoRETConstants.RejectEventFX.findFirstIn(des_log).getOrElse( MifidParseoRETConstants.RejectEventFX2.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Confirm */
      val valorConfirm = MifidParseoRETConstants.ConfirmEventFX.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.ConfirmEventFX2.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Close */
      val valorClose = MifidParseoRETConstants.CloseEventFX.findFirstIn(des_log).getOrElse( MifidParseoRETConstants.CloseEventFX2.findFirstIn(des_log).getOrElse(MifidParseoRETConstants.NotFound))
      /** Tags para identificar el evento Accept */
      val valorAccept = Try{MifidParseoRETConstants.AcceptEventFX.findFirstIn(des_log).get}
        .getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento lbnPSE (forma PR)*/
      val primerTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent1)
      val valorLbnPSE = if (primerTag != MifidParseoRETConstants.NotFound) {
        val segundoTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent2, primerTag + 1)
        if (segundoTag != MifidParseoRETConstants.NotFound) {
          val cuartoTag1 = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent3, segundoTag + 1)
          val cuartoTag2 = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent4, segundoTag + 1)
          if (cuartoTag1 != MifidParseoRETConstants.NotFound) {
            cuartoTag1
          } else if (cuartoTag2 != MifidParseoRETConstants.NotFound) {
            cuartoTag2
          } else -1
        } else -1
      } else -1

      /** Se asignan los valores para los tipos de línea */
      if (valorSubmit1 != MifidParseoRETConstants.NotFound || valorSubmit2 != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Submit
      } else if (valorPrice != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Price
      } else if (valorReject != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Reject
      } else if (valorConfirm != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Confirm
      } else if (valorClose != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Close
      } else if (valorAccept != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Accept
      } else if (valorLbnPSE != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.LbnPse
      } else null
    } else null
  }

  def obtenerTipoLineaBBG(des_log: String): String = {
    if (StringUtils.isNotBlank(des_log)) {
      /** Tags para identificar el evento Submit */
      val valorSubmit1 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent1)
      val valorSubmit2 = des_log.indexOf(MifidParseoRETConstants.SubmitEvent2)
      /** Tags para identificar el evento Price */
      val valorPrice = Try{MifidParseoRETConstants.PriceEventBBG.findFirstIn(des_log).get}
        .getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento Reject */
      val valorReject = Try{MifidParseoRETConstants.RejectEventBBG.findFirstIn(des_log).get}
        .getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento Confirm */
      val valorConfirm = Try{MifidParseoRETConstants.ConfirmEventBBG.findFirstIn(des_log).get}
        .getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento Close */
      val valorClose = Try{MifidParseoRETConstants.CloseEventBBG.findFirstIn(des_log).get}
        .getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento Accept */
      val valorAccept = Try{MifidParseoRETConstants.AcceptEventBBG.findFirstIn(des_log).get}
        .getOrElse(MifidParseoRETConstants.NotFound)
      /** Tags para identificar el evento lbnPSE (forma PR)*/
      val primerTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent0)
      val valorLbnPSE = if (primerTag != MifidParseoRETConstants.NotFound) {
        //una vez encontontrado el tag server buscamos el segundo tag
        val segundoTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent1, primerTag + 1)
        //Comprobamos que a continuación se encuentra la cadena de XML
        if (segundoTag != MifidParseoRETConstants.NotFound) {
          /*Comprobamos que a continuación se encuentra la cadena tercera Tag en formato XML
              si se encuentra el tipo de linea es LbnPSE
              */
          val tercerTag = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent2, segundoTag + 1)
          if (tercerTag != MifidParseoRETConstants.NotFound) {
            val cuartoTag1 = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent3, tercerTag + 1)
            val cuartoTag2 = des_log.indexOf(MifidParseoRETConstants.TagLbnPseEvent4, tercerTag + 1)
            if (cuartoTag1 != MifidParseoRETConstants.NotFound) {
              cuartoTag1
            } else if (cuartoTag2 != MifidParseoRETConstants.NotFound) {
              cuartoTag2
            } else -1
          } else -1
        } else -1
      } else -1

      /** Se asignan los valores para los tipos de línea */
      if (valorSubmit1 != MifidParseoRETConstants.NotFound || valorSubmit2 != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Submit
      } else if (valorPrice != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Price
      } else if (valorReject != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Reject
      } else if (valorConfirm != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Confirm
      } else if (valorClose != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Close
      } else if (valorAccept != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.Accept
      } else if (valorLbnPSE != MifidParseoRETConstants.NotFound) {
        MifidParseoRETConstants.LbnPse
      } else null
    } else null
  }

  def containsSomeOf(wordsToFind: List[String], filters: List[String]): Boolean = filters match {
    case Nil => false
    case head :: _ if wordsToFind.exists(_.contains(head)) => true
    case _ :: rest => containsSomeOf(wordsToFind, rest)
  }

  def obtenerFiltroSubmit(cod_flujo: String, cod_takergroupid: String, cod_takergroupfullname: String, cod_takergroupname: String, cod_takername: String, cod_proxyname: String, cod_proxygroupid :String ,cod_proxygroupname :String ,cod_producto: String, tipo: String): String = {
    //Branch (incluir)
    val listaBranch = MifidParseoRETConstants.listaTakerGroupId ++ MifidParseoRETConstants.listaTakerGroupFullName
    val cumpleBranch = containsSomeOf(List(cod_takergroupid, cod_takergroupfullname), listaBranch)

    //Cliente (incluir)
    val cumpleCliente = containsSomeOf(List(cod_takername), MifidParseoRETConstants.listaTakerName)

    //Producto (incluir)
    val listaProducto = List(MifidParseoRETConstants.TagFORWARD, MifidParseoRETConstants.TagSWAP,  MifidParseoRETConstants.TagNDF, MifidParseoRETConstants.TagBLOCK)
    val cumpleProducto = containsSomeOf(List(cod_producto), listaProducto)
    //Proxy (excluir)
    if (tipo == "RET") {

      //Plataforma (incluir)
      val listaPlataforma = MifidParseoRETConstants.listaTakerGroupId2 ++ MifidParseoRETConstants.listaTakerGroupName ++ MifidParseoRETConstants.listaProxyGroupId ++ MifidParseoRETConstants.listaProxyGroupNameId
      val cumplePlataforma = containsSomeOf(List(cod_takergroupid, cod_takergroupname, cod_proxygroupid, cod_proxygroupname ), listaPlataforma)

      val cumpleProxyNameRET = containsSomeOf(List(cod_proxyname), MifidParseoRETConstants.listaProxyName)
      if (cumpleBranch && cumpleProducto && cumplePlataforma && !cumpleCliente && !cumpleProxyNameRET) {
        cod_flujo
      } else null
    }
    else if (tipo == "360") {
      val cumpleProxyName360 = containsSomeOf(List(cod_proxyname), MifidParseoRETConstants.listaProxyName360)
      if (cumpleBranch && cumpleProducto && !cumpleCliente && cumpleProxyName360) {
        cod_flujo
      } else null
    }
    else if (tipo == "FxALL") {
      val cumpleProxyNameFxALL = containsSomeOf(List(cod_proxyname), MifidParseoRETConstants.listaProxyNameFxALL)
      if (cumpleBranch && cumpleProducto && !cumpleCliente && cumpleProxyNameFxALL) {
        cod_flujo
      } else null
    }
    else if (tipo == "BBG") {
      val cumpleProxyNameBBG = containsSomeOf(List(cod_proxyname), MifidParseoRETConstants.listaProxyNameBBG)
      if (cumpleBranch && cumpleProducto && !cumpleCliente && cumpleProxyNameBBG) {
        cod_flujo
      } else null
    }
    else null
  }
}
