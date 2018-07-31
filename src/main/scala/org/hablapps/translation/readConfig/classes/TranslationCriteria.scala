package org.hablapps
package translation
package readConfig
package classes

import org.apache.spark.sql.Row

case class TranslationCriteria(
  processId: String,
  ioId: String,
  translationCriteriaId: String,
  fieldKey: String)

object TranslationCriteria {

  def fromRow(row: Row) =
    fromSCSV(row.mkString(";"))

  def fromSCSV(scsv: String) =
    scsv.split(";").toList match {
      case processId :: ioId :: translationCriteriaId :: fieldKey :: Nil =>
        TranslationCriteria(processId, ioId, translationCriteriaId, fieldKey)
      case _ =>
        throw new RuntimeException(s"Couldn't parse $scsv as a TranslationCriteria")
    }
}
