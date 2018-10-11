package org.hablapps
package bbva

case class RetRet(
                   source_offset: Long,
                   source_name: String,
                   des_log: String,
                   created_date: String,
                   year: String,
                   month: String,
                   day: String,
                   last_version: Short,
                   tipo: String,
                   cod_tipoLinea: String,
                   gidid: String,
                   takername: String,
                   proxyname: String) {
  override def toString: String = {
    s"""
       |RetRet(
       |   source_offset = $source_offset.toLong,
       |   source_name = ${if (source_name != null) "\"" + source_name + "\"" else null},
       |   des_log = ${if (des_log != null) "\"\"\"" + des_log + "\"\"\"" else null},
       |   created_date = ${if (created_date != null) "\"" + created_date + "\"" else null},
       |   year =  ${if (year != null) "\"" + year + "\"" else null},
       |   month =  ${if (month != null) "\"" + month + "\"" else null},
       |   day =  ${if (day != null) "\"" + day + "\"" else null},
       |   last_version = $last_version.toShort,
       |   tipo =  ${if (tipo != null) "\"" + tipo + "\"" else null},
       |   cod_tipoLinea =  ${if (cod_tipoLinea != null) "\"" + cod_tipoLinea + "\"" else null},
       |   gidid =  ${if (gidid != null) "\"" + gidid + "\"" else null},
       |   takername = ${if (takername != null) "\"" + takername + "\"" else null},
       |   proxyname = ${if (proxyname != null) "\"" + proxyname + "\"" else null}
       |)
    """.stripMargin
  }
}

object RetRet{
  sealed abstract class Error
  case class EmptyField() extends Error
}