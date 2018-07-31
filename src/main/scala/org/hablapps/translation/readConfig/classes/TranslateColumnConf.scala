package org.hablapps
package translation
package readConfig
package classes

import org.apache.spark.sql.Row

case class TranslateColumnConf(
  inputColumn: String,
  lookupSrc: String,
  lookupKeyColumn: String,
  lookupValueColumn: String,
  outputColumn: String)

case class TranslateColumnConf2(
  inputColumn: String,
  lookupSrc: String,
  lookupKeyColumn: String,
  lookupValueColumn: String,
  outputColumn: String,
  ioId: String)

object TranslateColumnConf2 {

  def fromRow(row: Row) =
    fromSCSV(row.mkString(";"))

  def fromSCSV(scsv: String) =
    scsv.split(";").toList match {
      case inputColumn :: lookupSrc :: lookupKeyColumn :: lookupValueColumn :: outputColumn :: ioId :: Nil =>
        TranslateColumnConf2(inputColumn, lookupSrc, lookupKeyColumn, lookupValueColumn, outputColumn, ioId)
      case _ =>
        throw new RuntimeException(s"Couldn't parse $scsv as a TranslateColumnConf2")
    }
}
