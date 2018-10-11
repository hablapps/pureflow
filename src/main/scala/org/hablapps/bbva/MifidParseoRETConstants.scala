package org.hablapps.bbva

import org.apache.commons.lang.StringUtils

object MifidParseoRETConstants {
  lazy val TagEstruct = "Estruct"
  lazy val SubmitEvent1 = "FX.CROSS (0, 0) sent <submit instrument=\"FX.CROSS\""
  lazy val SubmitEvent2 = "FX.DIFFUSION (0, 0) sent <submit instrument=\"FX.DIFFUSION\""
  lazy val Submit = "Submit"
  lazy val Tagbreakdown = "breakdown"
  lazy val Tagservertime = "server_time"
  lazy val Tagfxcrossdeal = "fx_cross_deal"
  lazy val Tagrequirement = "requirement"
  lazy val Taggidid = "gid_id"
  lazy val TaggididCompleto = "<gid_id>"
  lazy val TaggididFinalCompleto = "</gid_id>"
  lazy val Taggidid1 = "gid_id=\""
  lazy val Tagsubmitid = "submit_id"
  lazy val Tagdealtccy = "dealt_ccy"
  lazy val TagFORWARD = "FORWARD"
  lazy val TagSWAP = "SWAP"
  lazy val TagNDF = "NDF"
  lazy val TagBLOCK = "BLOCK"
  lazy val Tagccy2 = "ccy2"
  lazy val Tagccy1 = "ccy1"
  lazy val Tagvaluedate = "value_date"
  lazy val Tagtakerversion = "taker_version"
  lazy val Tagdealtamount = "dealt_amount"
  lazy val Tagcontraamount = "contra_amount"
  lazy val TagSell = "sell"
  lazy val TagaccountId = "taker_account"
  lazy val TagtakerId = "taker_id"
  lazy val TagtakerName = "taker_name"
  lazy val PriceEvent1 = "[^\\s]+ \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price instrument=\"FX.CROSS\"".r
  lazy val PriceEvent2 = "[^\\s]+ \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price instrument=\"FX.DIFFUSION\"".r
  lazy val PriceEvent3 = "[^\\s]+ \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price instrument=".r
  lazy val AcceptEvent = "[^\\s]+ \\(\\d+, \\d+\\) sent <accept".r
  lazy val Price = "Price"
  lazy val Tagprice = "price"
  lazy val Tagcrosscomponent = "cross_component"
  lazy val Tagquote = "quote"
  lazy val Tagquotebreak = "quote_breakdown"
  lazy val Tagcoremarket = "core_market_spot_price"
  lazy val Tagbuy = "buy"
  lazy val Tagallin = "all_in"
  lazy val Tagspotdate = "spot_date="
  lazy val RejectEvent = "[^\\s]+ \\(\\d+, \\d+\\) sent <reject gid_id".r
  lazy val Reject = "Reject"
  lazy val Tagrecv = "recv"
  lazy val ConfirmEvent = "[^\\s]+ \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <confirm gid_id".r
  lazy val Confirm = "Confirm"
  lazy val CloseEvent = "[^\\s]+ \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <close".r
  lazy val StateClose = "state=\"Completed\""
  lazy val StateCloseReject = "state=\"Cancelled\""
  lazy val Close = "Close"
  lazy val FxCrossDealType = "<fx_cross_deal type=\"(SWAP|FORWARD|NDF|BLOCK)\"".r
  lazy val NotFound: Int = -1
  lazy val LbnPse = "LbnPSE"
  lazy val TagSWAPFAR = "SWAP-FAR"
  lazy val Tagleg = "leg"
  lazy val LabelFXCross = "FX.CROSS"
  lazy val TagTakerGroupId = "taker_group_id"
  lazy val TagProxyGroupId = "proxy_group_id"
  lazy val TagTakerGroupFullName = "taker_group_full_name"
  lazy val TagTakerGroupName = "taker_group_name"
  lazy val TagProxyGroupName = "proxy_group_name"
  lazy val TagProxyName = "proxy_name"
  lazy val TagMakerVersion = "maker_version"
  lazy val TagNetType = "net_type"
  lazy val TagData = "data"
  lazy val TagDeal = "deal"
  lazy val TagDealNotification = "deal_notification"
  lazy val TagIndicativeQuote = "indicative_quote"
  lazy val listaTakerGroupId = List("DUS", "HK", "LIS", "LON", "MAD", "MIL", "NY", "PAR")
  lazy val listaTakerGroupId2 = List("INT", "PC", "BIL")
  lazy val listaProxyGroupId = List("INT", "PC", "BIL")
  lazy val listaProxyGroupNameId = List("INT", "PC", "BIL")
  lazy val listaTakerGroupFullName = List("Dusse", "Hong Kong", "Lisboa", "Londres", "Madrid", "Milan", "New York", "Paris")
  lazy val listaTakerGroupName = List("INT", "PC", "BIL")
  lazy val listaTakerName = List("AutoClientTrader", "BBVA2Garanti", "BBVACOMPASS", "COMPASS", "compass")
  lazy val listaProxyName = List("360T", "fxall_proxy", "sbp.gl.tradingp", "bbg.gl.tradingp", "fxall.gl.tradin", "fxall_proxy_com", "360ttex.gl.trad", "EyP-Proxy1", "EyP-Proxy2", "Loms")
  lazy val DiarioSlash = "diario/"
  lazy val Guion = "-"
  lazy val Punto = "."
  // filtros de proxy para las diferentes aplicaciones
  lazy val listaProxyNameFxALL = List("fxall.gl.tradin", "fxall_proxy")
  lazy val listaProxyNameBBG = List("bbg.gl.tradingp")
  lazy val LbnPSEEvent360 = "lbnPSE"
  lazy val TagLbnPseEvent0 = "server"
  lazy val TagLbnPseEvent1 = "<gid_message"
  lazy val TagLbnPseEvent2 = "<recipient>lbnPSE"
  lazy val TagLbnPseEvent3 = "<deal instrument=\"FX.CROSS\""
  lazy val TagLbnPseEvent4 = "<deal instrument=\"FX.DIFFUSION\""
  lazy val PriceEvent360 = "[^\\s]+ 360ttex.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price taker_counterparty".r
  lazy val PriceEvent3602 = "[^\\s]+ 360T[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price taker_counterparty".r

  lazy val RejectEvent360 = "[^\\s]+ 360ttex.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) sent [^\\d]{0,}:<reject gid_id".r
  lazy val RejectEvent3602 = "[^\\s]+ 360T[^\\s]{0,} \\(\\d+, \\d+\\) sent [^\\d]{0,}:<reject gid_id".r

  lazy val ConfirmEvent360 = "[^\\s]+ 360ttex.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <confirm gid_id".r
  lazy val ConfirmEvent3602 = "[^\\s]+ 360T[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <confirm gid_id".r

  lazy val CloseEvent360 = "[^\\s]+ 360ttex.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <close".r
  lazy val CloseEvent3602 = "[^\\s]+ 360T[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <close".r

  lazy val AcceptEvent360 = "[^\\s]+ 360ttex.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) sent <accept submit_id=".r

  lazy val PriceEventFX = "[^\\s]+ fxall.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price taker_counterparty".r
  lazy val PriceEventFX2 = "[^\\s]+ fxall_proxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price taker_counterparty".r

  lazy val RejectEventFX = "[^\\s]+ fxall.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) sent [^\\d]{0,}:<reject gid_id".r
  lazy val RejectEventFX2 = "[^\\s]+ fxall_proxy[^\\s]{0,} \\(\\d+, \\d+\\) sent [^\\d]{0,}:<reject gid_id".r

  lazy val ConfirmEventFX = "[^\\s]+ fxall.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <confirm gid_id".r
  lazy val ConfirmEventFX2 = "[^\\s]+ fxall_proxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <confirm gid_id".r

  lazy val CloseEventFX = "[^\\s]+ fxall.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <close".r
  lazy val CloseEventFX2 = "[^\\s]+ fxall_proxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <close".r

  lazy val AcceptEventFX = "[^\\s]+ fxall.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) sent <accept submit_id=".r

  lazy val PriceEventBBG = "[^\\s]+ bbg.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <price taker_counterparty".r
  lazy val RejectEventBBG = "[^\\s]+ bbg.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) sent [^\\d]{0,}:<reject gid_id".r
  lazy val ConfirmEventBBG = "[^\\s]+ bbg.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <confirm gid_id".r
  lazy val CloseEventBBG = "[^\\s]+ bbg.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) recv \\(\\d+\\.\\d+s\\) <close".r
  lazy val AcceptEventBBG = "[^\\s]+ bbg.gl.tradingproxy[^\\s]{0,} \\(\\d+, \\d+\\) sent <accept submit_id=".r

  lazy val Operativa360T = "360T"
  lazy val OperativaFxALL = "FxALL"
  lazy val Operativa3BBG = "BBG"
  lazy val OperativaOTC = "OTC"
  lazy val Accept = "Accept"

  lazy val TagOperativa360T1 = "360ttex.gl.tradingproxy"
  lazy val TagOperativa360T3 = "<log instrument=\"FX."
  lazy val TagOperativa360T4 = "<taker_custom"
  lazy val TagOperativaFxALL1 = "fxall.gl.tradingproxy"
  lazy val TagOperativaFxALL2 = "<log instrument=\"FX."
  lazy val TagOperativaFxALL3 = "<taker_custom"
  lazy val TagOperativaBBG1 = "bbg.gl.tradingproxy"
  lazy val TagOperativaBBG2 = "<log instrument=\"FX."
  lazy val TagOperativaBBG3 = "<taker_custom"
  lazy val listaProxyName360 = List("360ttex.gl.trad", "360T")
  lazy val listaTakerGroupId360 = List("DUS", "HK", "LIS", "LON", "MAD", "MIL", "NY", "PAR")
  lazy val listaTakerGroupFullName360 = List("Dusse", "Hong Kong", "Lisboa", "Londres", "Madrid", "Milan", "New York", "Paris")
  lazy val listaTakerName360 = List("AutoClientTrader", "BBVA2Garanti", "BBVACOMPASS", "COMPASS")
  lazy val TagTakercustom = "taker_custom"

  lazy val Signo = ">"
  lazy val TagComillas = "\""

  lazy val TagTakernameCompleto = "<taker_name>"
  lazy val TagTakernameFinalCompleto = "</taker_name>"
  lazy val TagProxynameCompleto = "<proxy_name>"
  lazy val TagProxynameFinalCompleto = "</proxy_name>"

  val extractGidID: String => String = (des_log: String) => {
    if (StringUtils.isNotBlank(des_log)) {
      val posInicio = des_log.indexOf(MifidParseoRETConstants.TaggididCompleto)
      val posFinal = des_log.indexOf(MifidParseoRETConstants.TaggididFinalCompleto, posInicio)
      if (posInicio != -1) {
        if (posFinal != -1) {
          des_log.substring(posInicio + MifidParseoRETConstants.TaggididCompleto.length(), posFinal)
        } else null
      } else {
        val posInicio = des_log.indexOf(MifidParseoRETConstants.Taggidid1)
        val posFinal = des_log.indexOf(MifidParseoRETConstants.TagComillas, posInicio + MifidParseoRETConstants.Taggidid1.length())
        if (posInicio != -1) {
          if (posFinal != -1) {
            des_log.substring(posInicio + MifidParseoRETConstants.Taggidid1.length(), posFinal)
          } else null
        } else null
      }
    } else null
  }

  val extractTakername: (String, String) => String = (des_log: String, cod_tipolinea: String) => {
    if (StringUtils.isNotBlank(des_log)) {
      if (cod_tipolinea == MifidParseoRETConstants.Submit) {

        val posInicio = des_log.indexOf(MifidParseoRETConstants.TagTakernameCompleto)
        val posFinal = des_log.indexOf(MifidParseoRETConstants.TagTakernameFinalCompleto, posInicio)
        if (posInicio != -1 || posFinal != -1) {
          des_log.substring(posInicio + MifidParseoRETConstants.TagTakernameCompleto.length(), posFinal)
        } else null

      } else if (cod_tipolinea == MifidParseoRETConstants.Price) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.PriceEvent3.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else if (cod_tipolinea == MifidParseoRETConstants.Reject) {
        val reject = "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.RejectEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
        if (StringUtils.isBlank(reject)){
          "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.CloseEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
        }else
          reject
      } else if (cod_tipolinea == MifidParseoRETConstants.Confirm) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.ConfirmEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else if (cod_tipolinea == MifidParseoRETConstants.Close) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.CloseEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else if (cod_tipolinea == MifidParseoRETConstants.Accept) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.AcceptEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else null
    } else null
  }

  val extractProxyname: (String, String) => String = (des_log: String, cod_tipolinea: String) => {
    if (StringUtils.isNotBlank(des_log)) {
      if (cod_tipolinea == MifidParseoRETConstants.Submit) {

        val posInicio = des_log.indexOf(MifidParseoRETConstants.TagProxynameCompleto)
        val posFinal = des_log.indexOf(MifidParseoRETConstants.TagProxynameFinalCompleto, posInicio)
        if (posInicio != -1 || posFinal != -1) {
          des_log.substring(posInicio + MifidParseoRETConstants.TagProxynameCompleto.length(), posFinal)
        } else null

      } else if (cod_tipolinea == MifidParseoRETConstants.Price) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.PriceEvent3.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else if (cod_tipolinea == MifidParseoRETConstants.Reject) {
        val reject = "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.RejectEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
        if (StringUtils.isBlank(reject)){
          "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.CloseEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
        }else
          reject
      } else if (cod_tipolinea == MifidParseoRETConstants.Confirm) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.ConfirmEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else if (cod_tipolinea == MifidParseoRETConstants.Close) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.CloseEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else if (cod_tipolinea == MifidParseoRETConstants.Accept) {
        "[^\\s]+ ".r.findFirstIn(MifidParseoRETConstants.AcceptEvent.findFirstIn(des_log).getOrElse("")).getOrElse("").trim()
      } else null
    } else null
  }

}