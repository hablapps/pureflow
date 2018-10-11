package org.hablapps.bbva

class Ret360Enriched(
  val source_name: String,
  val source_offset: String,
  val cod_tipolinea: String,
  val cod_flujo: String,
  val cod_leg: String,
  val cod_gidid: String,
  val cod_submitid: String,
  val cod_dealtccy: String,
  val cod_ccy1: String,
  val cod_ccy2: String,
  val cod_valuedate: String,
  val cod_takerversion: String,
  val cod_servertime: String,
  val cod_takeraccount: String,
  val cod_dealtamount: String,
  val cod_takerid: String,
  val cod_allin: String,
  val cod_coremarket: String,
  val cod_spotdate: String,
  val cod_buy: String,
  val cod_takername: String,
  val cod_sell: String,
  val cod_makerversion: String,
  val cod_status: String,
  val cod_takergroupid: String,
  val cod_takergroupfullname: String,
  val cod_proxyname: String,
  val cod_nettype: String,
  val cod_side: String,
  val cod_contraamount: String,
  val created_date: String,
  val year: Int,
  val month: Int,
  val day: Int,
  val last_version: Int
) extends Product{

  @throws(classOf[IndexOutOfBoundsException])
  override def productElement(n: Int): Any = n match {
    case 0 => source_name
    case 1 => source_offset
    case 2 => cod_tipolinea
    case 3 => cod_flujo
    case 4 => cod_leg
    case 5 => cod_gidid
    case 6 => cod_submitid
    case 7 => cod_dealtccy
    case 8 => cod_ccy1
    case 9 => cod_ccy2
    case 10  => cod_valuedate
    case 11 => cod_takerversion
    case 12 => cod_servertime
    case 13 => cod_takeraccount
    case 14 => cod_dealtamount
    case 15 => cod_takerid
    case 16 => cod_allin
    case 17 => cod_coremarket
    case 18 => cod_spotdate
    case 19 => cod_buy
    case 20 => cod_takername
    case 21 => cod_sell
    case 22 => cod_makerversion
    case 23 => cod_status
    case 24 => cod_takergroupid
    case 25 => cod_takergroupfullname
    case 26 => cod_proxyname
    case 27 => cod_nettype
    case 28 => cod_side
    case 29 => cod_contraamount
    case 30 => created_date
    case 31 => year
    case 32 => month
    case 33 => day
    case 34 => last_version
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 35

  override def canEqual(that: Any): Boolean = this.equals(that)

  override def equals(other: Any): Boolean = other match {
    case that: Ret360Enriched =>
      (that canEqual this) &&
        source_name == that.source_name &&
        source_offset == that.source_offset &&
        cod_tipolinea == that.cod_tipolinea &&
        cod_flujo == that.cod_flujo &&
        cod_leg == that.cod_leg &&
        cod_gidid == that.cod_gidid &&
        cod_submitid == that.cod_submitid &&
        cod_dealtccy == that.cod_dealtccy &&
        cod_ccy1 == that.cod_ccy1 &&
        cod_ccy2 == that.cod_ccy2 &&
        cod_valuedate == that.cod_valuedate &&
        cod_takerversion == that.cod_takerversion &&
        cod_servertime == that.cod_servertime &&
        cod_takeraccount == that.cod_takeraccount &&
        cod_dealtamount == that.cod_dealtamount &&
        cod_takerid == that.cod_takerid &&
        cod_allin == that.cod_allin &&
        cod_coremarket == that.cod_coremarket &&
        cod_spotdate == that.cod_spotdate &&
        cod_buy == that.cod_buy &&
        cod_takername == that.cod_takername &&
        cod_sell == that.cod_sell &&
        cod_makerversion == that.cod_makerversion &&
        cod_status == that.cod_status &&
        cod_takergroupid == that.cod_takergroupid &&
        cod_takergroupfullname == that.cod_takergroupfullname &&
        cod_proxyname == that.cod_proxyname &&
        cod_nettype == that.cod_nettype &&
        cod_side == that.cod_side &&
        cod_contraamount == that.cod_contraamount &&
        created_date == that.created_date &&
        year == that.year &&
        month == that.month &&
        day == that.day &&
        last_version == that.last_version
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(source_name, source_offset, cod_tipolinea, cod_flujo, cod_leg, cod_gidid, cod_submitid, cod_dealtccy, cod_ccy1, cod_ccy2, cod_valuedate, cod_takerversion, cod_servertime, cod_takeraccount, cod_dealtamount, cod_takerid, cod_allin, cod_coremarket, cod_spotdate, cod_buy, cod_takername, cod_sell, cod_makerversion, cod_status, cod_takergroupid, cod_takergroupfullname, cod_proxyname, cod_nettype, cod_side, cod_contraamount, created_date, year, month, day, last_version)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
