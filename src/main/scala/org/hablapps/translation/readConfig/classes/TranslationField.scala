package org.hablapps
package translation
package readConfig
package classes

import org.apache.spark.sql.Row

case class TranslationField(
  fieldKey: String,
  field: String,
  order: String)

object TranslationField {

  def fromRow(row: Row) =
    fromSCSV(row.mkString(";"))

  def fromSCSV(scsv: String) =
    scsv.split(";").toList match {
      case fieldKey :: field :: order :: Nil =>
        TranslationField(fieldKey, field, order)
      case _ =>
        throw new RuntimeException(s"Couldn't parse $scsv as a TranslationField")
    }
}
