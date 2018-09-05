package org.hablapps
package bbva


case class OperativaOTC(
  cod_gidid: String,
  cod_operativa: String,
  cod_tipolinea: String,
  cod_producto: String,
  cod_flujo: String,
  year: String,
  month: String,
  day: String,
  last_version: String
 )


object OperativaOTC{

  sealed abstract class Error
  case class EmptyGigid() extends Error

}
