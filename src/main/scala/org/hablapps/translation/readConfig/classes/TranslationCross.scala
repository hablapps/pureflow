package org.hablapps
package translation
package readConfig
package classes

import org.apache.spark.sql.Row

case class TranslationCross(
  translationCriteriaId: String,
  inputColumn: String,
  lookupValueColumn: String,
  lookupSrc: String,
  origin: String)

object TranslationCross {

  def fromRow(row: Row) =
    fromSCSV(row.mkString(";"))

  def fromSCSV(scsv: String) =
    scsv.split(";").toList match {
      case translationCriteriaId :: inputColumn :: lookupValueColumn :: lookupSrc :: origin :: Nil =>
        TranslationCross(translationCriteriaId, inputColumn, lookupValueColumn, lookupSrc, origin)
      case _ =>
        throw new RuntimeException(s"Couldn't parse $scsv as a TranslationCross")
    }
}
